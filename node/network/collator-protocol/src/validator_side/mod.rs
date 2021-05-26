// Copyright 2020 Parity Technologies (UK) Ltd.
// This file is part of Polkadot.

// Polkadot is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Polkadot is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Polkadot.  If not, see <http://www.gnu.org/licenses/>.

//! The Validator Side of the Collator Protocol implements specific logic around managing collator
//! connections in order to ensure that resources are freed up as the relay chain makes progress.

mod peer_slots;

use std::{collections::{HashMap, HashSet}, sync::Arc, task::Poll};
use std::collections::hash_map::Entry;
use std::time::{Duration, Instant};
use always_assert::never;
use futures::{
	channel::oneshot, future::{BoxFuture, Fuse, FusedFuture}, FutureExt, StreamExt,
	stream::FuturesUnordered, select,
};
use futures_timer::Delay;

use sp_keystore::SyncCryptoStorePtr;

use polkadot_node_network_protocol::{
	request_response as req_res, v1 as protocol_v1,
	peer_set::PeerSet,
	request_response::{
		request::{Recipient, RequestError},
		v1::{CollationFetchingRequest, CollationFetchingResponse},
		OutgoingRequest, Requests,
	},
	OurView, PeerId, UnifiedReputationChange as Rep, View,
};
use polkadot_node_primitives::{SignedFullStatement, Statement, PoV};
use polkadot_node_subsystem_util::{TimeoutExt, metrics::{self, prometheus}};
use polkadot_primitives::v1::{CandidateReceipt, CollatorId, Hash, Id as ParaId};
use polkadot_subsystem::{
	jaeger,
	messages::{
		AllMessages, CollatorProtocolMessage, IfDisconnected,
		NetworkBridgeEvent, NetworkBridgeMessage, CandidateBackingMessage,
	},
	FromOverseer, OverseerSignal, PerLeafSpan, SubsystemContext, SubsystemSender,
};

use crate::error::Fatal;

use peer_slots::{PeerSlots, FitnessEvent, Reservoir, PeerData};
use super::{Result, LOG_TARGET};

// How often to check all peers with activity.
#[cfg(not(test))]
const ACTIVITY_POLL: Duration = Duration::from_secs(1);

#[cfg(test)]
const ACTIVITY_POLL: Duration = Duration::from_millis(10);

// Specific Metrics around the Validator Side of the Collator Protocol
#[derive(Clone, Default)]
pub struct Metrics(Option<MetricsInner>);

impl Metrics {
	fn on_request(&self, succeeded: std::result::Result<(), ()>) {
		if let Some(metrics) = &self.0 {
			match succeeded {
				Ok(()) => metrics.collation_requests.with_label_values(&["succeeded"]).inc(),
				Err(()) => metrics.collation_requests.with_label_values(&["failed"]).inc(),
			}
		}
	}

	/// Provide a timer for `process_msg` which observes on drop.
	fn time_process_msg(&self) -> Option<metrics::prometheus::prometheus::HistogramTimer> {
		self.0.as_ref().map(|metrics| metrics.process_msg.start_timer())
	}

	/// Provide a timer for `handle_collation_request_result` which observes on drop.
	fn time_handle_collation_request_result(
		&self
	) -> Option<metrics::prometheus::prometheus::HistogramTimer> {
		self.0.as_ref().map(|metrics| metrics.handle_collation_request_result.start_timer())
	}

	/// Note the current number of collator peers.
	fn note_collator_peer_count(&self, collator_peers: usize) {
		self.0.as_ref().map(|metrics| metrics.collator_peer_count.set(collator_peers as u64));
	}
}

#[derive(Clone)]
struct MetricsInner {
	collation_requests: prometheus::CounterVec<prometheus::U64>,
	process_msg: prometheus::Histogram,
	handle_collation_request_result: prometheus::Histogram,
	collator_peer_count: prometheus::Gauge<prometheus::U64>,
}

impl metrics::Metrics for Metrics {
	fn try_register(registry: &prometheus::Registry)
		-> std::result::Result<Self, prometheus::PrometheusError>
	{
		let metrics = MetricsInner {
			collation_requests: prometheus::register(
				prometheus::CounterVec::new(
					prometheus::Opts::new(
						"parachain_collation_requests_total",
						"Number of collations requested from Collators.",
					),
					&["success"],
				)?,
				registry,
			)?,
			process_msg: prometheus::register(
				prometheus::Histogram::with_opts(
					prometheus::HistogramOpts::new(
						"parachain_collator_protocol_validator_process_msg",
						"Time spent within `collator_protocol_validator::process_msg`",
					)
				)?,
				registry,
			)?,
			handle_collation_request_result: prometheus::register(
				prometheus::Histogram::with_opts(
					prometheus::HistogramOpts::new(
						"parachain_collator_protocol_validator_handle_collation_request_result",
						"Time spent within `collator_protocol_validator::handle_collation_request_result`",
					)
				)?,
				registry,
			)?,
			collator_peer_count: prometheus::register(
				prometheus::Gauge::new(
					"parachain_collator_peer_count",
					"Amount of collator peers connected",
				)?,
				registry,
			)?,
		};

		Ok(Metrics(Some(metrics)))
	}
}

struct PerRequest {
	/// Responses from collator.
	from_collator: Fuse<BoxFuture<'static, req_res::OutgoingResult<CollationFetchingResponse>>>,
	/// Sender to forward to initial requester.
	to_requester: oneshot::Sender<(CandidateReceipt, PoV)>,
	/// A jaeger span corresponding to the lifetime of the request.
	span: Option<jaeger::Span>,
}

/// All state relevant for the validator side of the protocol lives here.
#[derive(Default)]
struct State {
	/// Our own view.
	view: OurView,

	/// Track all active collators and their data.
	peer_slots: PeerSlots,

	/// The collations we have requested by relay parent and para id.
	///
	/// For each relay parent and para id we may be connected to a number
	/// of collators each of those may have advertised a different collation.
	/// So we group such cases here.
	requested_collations: HashMap<(Hash, ParaId, PeerId), PerRequest>,

	/// Metrics.
	metrics: Metrics,

	/// Span per relay parent.
	span_per_relay_parent: HashMap<Hash, PerLeafSpan>,

	/// Keep track of all fetch collation requests
	collations: FuturesUnordered<BoxFuture<'static, Option<std::result::Result<(CandidateReceipt, PoV), oneshot::Canceled>>>>,
}

// O(n) search for collator ID by iterating through the peers map. This should be fast enough
// unless a large amount of peers is expected.
fn collator_peer_id(
	peer_data: &HashMap<PeerId, PeerData>,
	collator_id: &CollatorId,
) -> Option<PeerId> {
	peer_data.iter()
		.find_map(|(peer, data)|
			data.collator_id().filter(|c| c == &collator_id).map(|_| peer.clone())
		)
}

async fn disconnect_peer(ctx: &mut impl SubsystemContext, peer_id: PeerId) {
	ctx.send_message(
		NetworkBridgeMessage::DisconnectPeer(peer_id, PeerSet::Collation).into()
	).await
}

fn peer_collator_id(
	peer_data: &Reservoir,
	peer_id: &PeerId,
) -> Option<CollatorId> {
	if let Some(peer_data) = peer_data.get(peer_id) {
		if let Some(collator_id) = peer_data.collator_id() {
			return Some(collator_id.clone());
		}
	}

	None
}

/// Another subsystem has requested to fetch collations on a particular leaf for some para.
#[tracing::instrument(level = "trace", skip(ctx, state, tx), fields(subsystem = LOG_TARGET))]
async fn fetch_collation<Context>(
	ctx: &mut Context,
	state: &mut State,
	relay_parent: Hash,
	collator_id: CollatorId,
	para_id: ParaId,
	tx: oneshot::Sender<(CandidateReceipt, PoV)>
)
where
	Context: SubsystemContext<Message = CollatorProtocolMessage>
{
	let peer_id = match collator_peer_id(&state.peer_slots.peer_data, &collator_id) {
		None => return,
		Some(p) => p,
	};
	if state
		.peer_slots
		.peer_data
		.get(&peer_id)
		.map_or(false, |d| d.has_advertised(&relay_parent))
	{
		request_collation(ctx, state, relay_parent, para_id, peer_id, tx).await;
	}
}

/// Notify a collator that its collation got seconded.
#[tracing::instrument(level = "trace", skip(ctx, peer_data), fields(subsystem = LOG_TARGET))]
async fn notify_collation_seconded(
	ctx: &mut impl SubsystemContext<Message = CollatorProtocolMessage>,
	peer_data: &Reservoir,
	id: CollatorId,
	relay_parent: Hash,
	statement: SignedFullStatement,
) {
	if !matches!(statement.payload(), Statement::Seconded(_)) {
		tracing::error!(
			target: LOG_TARGET,
			statement = ?statement,
			"Notify collation seconded called with a wrong statement.",
		);
		return;
	}

	if let Some(peer_id) = collator_peer_id(peer_data, &id) {
		let wire_message = protocol_v1::CollatorProtocolMessage::CollationSeconded(
			relay_parent,
			statement.into()
		);

		ctx.send_message(AllMessages::NetworkBridge(
			NetworkBridgeMessage::SendCollationMessage(
				vec![peer_id],
				protocol_v1::CollationProtocol::CollatorProtocol(wire_message),
			)
		)).await;
	}
}

/// A peer's view has changed. A number of things should be done:
///  - Ongoing collation requests have to be cancelled.
///  - Advertisements by this peer that are no longer relevant have to be removed.
#[tracing::instrument(level = "trace", skip(state), fields(subsystem = LOG_TARGET))]
async fn handle_peer_view_change(
	state: &mut State,
	peer_id: PeerId,
	view: View,
) -> Result<()> {
	let peer_data = state.peer_slots.peer_data.entry(peer_id.clone()).or_insert(PeerData::new(Default::default(), state.peer_slots.peers.len()));

	peer_data.update_view(view);
	state.requested_collations
		.retain(|(rp, _, pid), _| pid != &peer_id || !peer_data.has_advertised(&rp));

	Ok(())
}

/// Request a collation from the network.
/// This function will
///  - Check for duplicate requests.
///  - Check if the requested collation is in our view.
///  - Update PerRequest records with the `result` field if necessary.
/// And as such invocations of this function may rely on that.
#[tracing::instrument(level = "trace", skip(ctx, state, result), fields(subsystem = LOG_TARGET))]
async fn request_collation<Context>(
	ctx: &mut Context,
	state: &mut State,
	relay_parent: Hash,
	para_id: ParaId,
	peer_id: PeerId,
	result: oneshot::Sender<(CandidateReceipt, PoV)>,
)
where
	Context: SubsystemContext<Message = CollatorProtocolMessage>
{
	if !state.view.contains(&relay_parent) {
		tracing::debug!(
			target: LOG_TARGET,
			peer_id = %peer_id,
			para_id = %para_id,
			relay_parent = %relay_parent,
			"collation is no longer in view",
		);
		return;
	}

	if state.requested_collations.contains_key(&(relay_parent, para_id.clone(), peer_id.clone())) {
		tracing::warn!(
			target: LOG_TARGET,
			peer_id = %peer_id,
			%para_id,
			?relay_parent,
			"collation has already been requested",
		);
		return;
	}

	let (full_request, response_recv) =
		OutgoingRequest::new(Recipient::Peer(peer_id), CollationFetchingRequest {
			relay_parent,
			para_id,
		});
	let requests = Requests::CollationFetching(full_request);

	let per_request = PerRequest {
		from_collator: response_recv.boxed().fuse(),
		to_requester: result,
		span: state.span_per_relay_parent.get(&relay_parent).map(|s| {
			s.child("collation-request")
				.with_para_id(para_id)
		}),
	};

	state.requested_collations.insert((relay_parent, para_id.clone(), peer_id.clone()), per_request);

	tracing::debug!(
		target: LOG_TARGET,
		peer_id = %peer_id,
		%para_id,
		?relay_parent,
		"Requesting collation",
	);

	ctx.send_message(AllMessages::NetworkBridge(
		NetworkBridgeMessage::SendRequests(vec![requests], IfDisconnected::ImmediateError))
	).await;
}

/// Networking message has been received.
#[tracing::instrument(level = "trace", skip(ctx, state), fields(subsystem = LOG_TARGET))]
async fn process_incoming_peer_message<Context>(
	ctx: &mut Context,
	state: &mut State,
	origin: PeerId,
	msg: protocol_v1::CollatorProtocolMessage,
)
where
	Context: SubsystemContext<Message = CollatorProtocolMessage>
{
	use protocol_v1::CollatorProtocolMessage::*;
	use sp_runtime::traits::AppVerify;

	match msg {
		Declare(collator_id, para_id, signature) => {
			if collator_peer_id(&state.peer_slots.peer_data, &collator_id).is_some() {
				return peer_slots::insert_event(
					ctx,
					&mut state.peer_slots,
					&origin,
					&collator_id,
					FitnessEvent::Unexpected
				).await;
			}

			let peer_data = match state.peer_slots.peer_data.get_mut(&origin) {
				Some(p) => p,
				None => {
					return peer_slots::insert_event(
						ctx,
						&mut state.peer_slots,
						&origin,
						&collator_id,
						FitnessEvent::Unexpected
					).await;
				}
			};

			if peer_data.is_collating() {
				return peer_slots::insert_event(
					ctx,
					&mut state.peer_slots,
					&origin,
					&collator_id,
					FitnessEvent::Unexpected,
				).await;
			}

			if !signature.verify(&*protocol_v1::declare_signature_payload(&origin), &collator_id) {
				return peer_slots::insert_event(
					ctx,
					&mut state.peer_slots,
					&origin,
					&collator_id,
					FitnessEvent::SigError,
				).await;
			}

			if state.peer_slots.active_paras.is_current_or_next(para_id) {
				tracing::debug!(
					target: LOG_TARGET,
					peer_id = ?origin,
					?collator_id,
					?para_id,
					"Declared as collator for current or next para",
				);

				peer_data.set_collating(&collator_id, &para_id);
			} else {
				tracing::debug!(
					target: LOG_TARGET,
					peer_id = ?origin,
					?collator_id,
					?para_id,
					"Declared as collator for unneeded para",
				);
				peer_slots::insert_event(
					ctx,
					&mut state.peer_slots,
					&origin,
					&collator_id,
					FitnessEvent::Superfluous,
				).await;
				disconnect_peer(ctx, origin).await;
				return;
			}

			state.peer_slots.insert_collator(&collator_id);
			// Sample the peer's connection into the reservoir
			for stale_peer_id in
				peer_slots::sample_connection(&mut state.peer_slots, &origin).into_iter()
			{
				disconnect_peer(ctx, stale_peer_id).await;
			}
		}
		AdvertiseCollation(relay_parent) => {
			let _span = state
				.span_per_relay_parent
				.get(&relay_parent)
				.map(|s| s.child("advertise-collation"));

			if !state.view.contains(&relay_parent) {
				tracing::debug!(
					target: LOG_TARGET,
					peer_id = ?origin,
					?relay_parent,
					"Advertise collation out of view",
				);

				return insert_event_by_peer_id(
					ctx,
					&mut state.peer_slots,
					&origin,
					FitnessEvent::Unexpected
				).await;
			}

			let peer_data = match state.peer_slots.peer_data.get_mut(&origin) {
				None => {
					return insert_event_by_peer_id(
						ctx,
						&mut state.peer_slots,
						&origin,
						FitnessEvent::Unexpected
					).await;
				}
				Some(p) => p,
			};

			match peer_data.insert_advertisement(relay_parent, &state.view) {
				Ok((collator_id, para_id)) => {
					tracing::debug!(
						target: LOG_TARGET,
						peer_id = ?origin,
						%para_id,
						?relay_parent,
						"Received advertise collation",
					);
					let (tx, rx) = oneshot::channel::<(
						CandidateReceipt,
						PoV,
					)>();

					fetch_collation(ctx, state, relay_parent, collator_id, para_id, tx).await;
					let future = async move {
						rx.timeout(Duration::new(2, 200)).await
					};
					state.collations.push(Box::pin(future));
				}
				Err(e) => {
					tracing::debug!(
						target: LOG_TARGET,
						peer_id = ?origin,
						?relay_parent,
						error = ?e,
						"Invalid advertisement",
					);

					return insert_event_by_peer_id(
						ctx,
						&mut state.peer_slots,
						&origin,
						FitnessEvent::Unexpected,
					).await;
				}
			}
		}
		CollationSeconded(_, _) => {
			tracing::warn!(
				target: LOG_TARGET,
				peer_id = ?origin,
				"Unexpected `CollationSeconded` message, decreasing reputation",
			);
			return insert_event_by_peer_id(
				ctx,
				&mut state.peer_slots,
				&origin,
				FitnessEvent::Unexpected,
			).await;
		}
	}
}

/// A leaf has become inactive so we want to
///   - Cancel all ongoing collation requests that are on top of that leaf.
///   - Remove all stored collations relevant to that leaf.
#[tracing::instrument(level = "trace", skip(state), fields(subsystem = LOG_TARGET))]
async fn remove_relay_parent(
	state: &mut State,
	relay_parent: Hash,
) -> Result<()> {
	state.requested_collations.retain(|k, _| {
		k.0 != relay_parent
	});
	Ok(())
}

/// Our view has changed.
#[tracing::instrument(level = "trace", skip(ctx, state, keystore), fields(subsystem = LOG_TARGET))]
async fn handle_our_view_change(
	ctx: &mut impl SubsystemContext,
	state: &mut State,
	keystore: &SyncCryptoStorePtr,
	view: OurView,
) -> Result<()> {
	let old_view = std::mem::replace(&mut state.view, view);

	let added: HashMap<Hash, Arc<jaeger::Span>> = state.view
		.span_per_head()
		.iter()
		.filter(|v| !old_view.contains(&v.0))
		.map(|v| (v.0.clone(), v.1.clone()))
		.collect();

	added.into_iter().for_each(|(h, s)| {
		state.span_per_relay_parent.insert(h, PerLeafSpan::new(s, "validator-side"));
	});

	let added = state.view.difference(&old_view).cloned().collect::<Vec<_>>();
	let removed = old_view
		.difference(&state.view)
		.cloned()
		.collect::<Vec<_>>();

	for removed in removed.iter().cloned() {
		remove_relay_parent(state, removed).await?;
		state.span_per_relay_parent.remove(&removed);
	}

	// Assign incoming and remove outgoing RelayParents.
	peer_slots::cycle_para(
		ctx.sender(),
		&mut state.peer_slots.active_paras,
		keystore,
		added,
		removed
	).await;

	// Prune advertisements and move outdated peers to stale peers.
	for peer_id in peer_slots::handle_view_change(&mut state.peer_slots, &state.view).iter() {
		disconnect_peer(ctx, *peer_id).await;
	}

	// Reset sample to ensure that new incoming connections for viable collators are ensured to be
	// uniformly randomly sampled whenever their parachain becomes assigned.
	state.peer_slots.reset_sample();

	Ok(())
}

/// Bridge event switch.
#[tracing::instrument(level = "trace", skip(ctx, state, keystore), fields(subsystem = LOG_TARGET))]
async fn handle_network_msg<Context>(
	ctx: &mut Context,
	state: &mut State,
	keystore: &SyncCryptoStorePtr,
	bridge_message: NetworkBridgeEvent<protocol_v1::CollatorProtocolMessage>,
) -> Result<()>
where
	Context: SubsystemContext<Message = CollatorProtocolMessage>
{
	use NetworkBridgeEvent::*;

	match bridge_message {
		PeerConnected(peer_id, _role, _) => {
			peer_slots::handle_connection(&mut state.peer_slots, peer_id);
			state.metrics.note_collator_peer_count(state.peer_slots.peer_data.len());
		},
		PeerDisconnected(peer_id) => {
			state.peer_slots.peer_data.remove(&peer_id);
			state.metrics.note_collator_peer_count(state.peer_slots.peer_data.len());
		},
		PeerViewChange(peer_id, view) => {
			handle_peer_view_change(state, peer_id, view).await?;
		},
		OurViewChange(view) => {
			handle_our_view_change(ctx, state, keystore, view).await?;
		},
		PeerMessage(remote, msg) => {
			process_incoming_peer_message(ctx, state, remote, msg).await;
		}
	}

	Ok(())
}

/// The main message receiver switch.
#[tracing::instrument(level = "trace", skip(ctx, keystore, state), fields(subsystem = LOG_TARGET))]
async fn process_msg<Context>(
	ctx: &mut Context,
	keystore: &SyncCryptoStorePtr,
	msg: CollatorProtocolMessage,
	state: &mut State,
)
where
	Context: SubsystemContext<Message = CollatorProtocolMessage>
{
	use CollatorProtocolMessage::*;

	let _timer = state.metrics.time_process_msg();

	match msg {
		CollateOn(id) => {
			tracing::warn!(
				target: LOG_TARGET,
				para_id = %id,
				"CollateOn message is not expected on the validator side of the protocol",
			);
		}
		DistributeCollation(_, _, _) => {
			tracing::warn!(
				target: LOG_TARGET,
				"DistributeCollation message is not expected on the validator side of the protocol",
			);
		}
		FetchCollation(relay_parent, collator_id, para_id, tx) => {
			let _span = state
				.span_per_relay_parent
				.get(&relay_parent)
				.map(|s| s.child("fetch-collation"));
			fetch_collation(ctx, state, relay_parent, collator_id, para_id, tx).await;
		}
		ReportCollator(collator_id) => {
			if let Some(origin) = collator_peer_id(&state.peer_slots.peer_data, &collator_id) {
				peer_slots::insert_event(
					ctx,
					&mut state.peer_slots,
					&origin,
					&collator_id,
					FitnessEvent::ReportBad
				).await;
			}
		}
		NoteGoodCollation(collator_id) => {
			if let Some(origin) = collator_peer_id(&state.peer_slots.peer_data, &collator_id) {
				peer_slots::insert_event(
					ctx,
					&mut state.peer_slots,
					&origin,
					&collator_id,
					FitnessEvent::NotifyGood
				).await;
			}
		}
		NotifyCollationSeconded(collator_id, relay_parent, statement) => {
			/// Notify a collator that its collation got seconded.
			notify_collation_seconded(
				ctx,
				&state.peer_slots.peer_data,
				collator_id,
				relay_parent,
				statement
			).await;
		}
		NetworkBridgeUpdateV1(event) => {
			if let Err(e) = handle_network_msg(
				ctx,
				state,
				keystore,
				event,
			).await {
				tracing::warn!(
					target: LOG_TARGET,
					err = ?e,
					"Failed to handle incoming network message",
				);
			}
		}
		CollationFetchingRequest(_) => {
			tracing::warn!(
				target: LOG_TARGET,
				"CollationFetchingRequest message is not expected on the validator side of the protocol",
			);
		}
		// TODO Ladi
		_ => {},
	}
}

// wait until next inactivity check. returns the instant for the following check.
async fn wait_until_next_check(last_poll: Instant) -> Instant {
	let now = Instant::now();
	let next_poll = last_poll + ACTIVITY_POLL;

	if next_poll > now {
		Delay::new(next_poll - now).await
	}

	Instant::now()
}

/// The main run loop.
#[tracing::instrument(skip(ctx, keystore, metrics), fields(subsystem = LOG_TARGET))]
pub(crate) async fn run<Context>(
	mut ctx: Context,
	keystore: SyncCryptoStorePtr,
	eviction_policy: crate::CollatorEvictionPolicy,
	metrics: Metrics,
) -> Result<()>
	where Context: SubsystemContext<Message = CollatorProtocolMessage>
{
	use FromOverseer::*;
	use OverseerSignal::*;

	let mut state = State {
		metrics,

		..Default::default()
	};

	let next_inactivity_stream = futures::stream::unfold(
		Instant::now() + ACTIVITY_POLL,
		|next_check| async move { Some(((), wait_until_next_check(next_check).await)) }
	).fuse();

	futures::pin_mut!(next_inactivity_stream);

	loop {
		select! {
			res = ctx.recv().fuse() => {
				match res {
					Ok(Communication { msg }) => {
						tracing::trace!(target: LOG_TARGET, msg = ?msg, "received a message");
						process_msg(
							&mut ctx,
							&keystore,
							msg,
							&mut state,
						).await;
					}
					Ok(Signal(Conclude)) => break,
					_ => panic!{},
				}
			}
			_ = next_inactivity_stream.next() => {
				disconnect_inactive_peers(&mut ctx, &eviction_policy, &state.peer_slots.peer_data).await;
			}
			res = state.collations.next() => {
				if let Some(Some(Ok((candidate_receipt, pov)))) = res {
					ctx.send_message(
						CandidateBackingMessage::Second(
							candidate_receipt.descriptor.relay_parent,
							candidate_receipt,
							pov,
						).into()
					).await;
				}
			}
			
		}

		let mut retained_requested = HashSet::new();
		let mut peer_fitness = Vec::new();
		for ((hash, para_id, peer_id), per_req) in state.requested_collations.iter_mut() {
			// Despite the await, this won't block on the response itself.
			if let Some(event) = poll_collation_response(
				&state.metrics, &state.span_per_relay_parent,
				hash, para_id, peer_id, per_req
			).await {
				peer_fitness.push((peer_id.clone(), event));
			} else {
				retained_requested.insert((*hash, *para_id, *peer_id));
			}
		}

		for (peer_id, event) in peer_fitness.into_iter() {
			insert_event_by_peer_id(&mut ctx, &mut state.peer_slots, &peer_id, event).await;
		}

		state.requested_collations.retain(|k, _| retained_requested.contains(k));
	}
	Ok(())
}

// This issues `NetworkBridge` notifications to disconnect from all inactive peers at the
// earliest possible point. This does not yet clean up any metadata, as that will be done upon
// receipt of the `PeerDisconnected` event.
async fn disconnect_inactive_peers(
	ctx: &mut impl SubsystemContext,
	eviction_policy: &crate::CollatorEvictionPolicy,
	peers: &HashMap<PeerId, PeerData>,
) {
	let now = Instant::now();
	for (peer, peer_data) in peers {
		if peer_data.is_inactive(now, &eviction_policy) {
			disconnect_peer(ctx, peer.clone()).await;
		}
	}
}

/// Poll collation response, return immediately if there is none.
///
/// Ready responses are handled, by logging and decreasing peer's reputation on error and by
/// forwarding proper responses to the requester.
///
/// Returns: `true` if `from_collator` future was ready.
async fn poll_collation_response(
	metrics: &Metrics,
	spans: &HashMap<Hash, PerLeafSpan>,
	hash: &Hash,
	para_id: &ParaId,
	peer_id: &PeerId,
	per_req: &mut PerRequest
) -> Option<FitnessEvent> {
	if never!(per_req.from_collator.is_terminated()) {
		tracing::error!(
			target: LOG_TARGET,
			"We remove pending responses once received, this should not happen."
		);
		return None;
	}

	let mut out = None;

	if let Poll::Ready(response) = futures::poll!(&mut per_req.from_collator) {
		let _span = spans.get(&hash)
				.map(|s| s.child("received-collation"));
		let _timer = metrics.time_handle_collation_request_result();

		let mut metrics_result = Err(());
		let mut success = "false";

		match response {
			Err(RequestError::InvalidResponse(err)) => {
				tracing::warn!(
					target: LOG_TARGET,
					hash = ?hash,
					para_id = ?para_id,
					peer_id = ?peer_id,
					err = ?err,
					"Collator provided response that could not be decoded"
				);
				out = Some(FitnessEvent::Corrupted);
			}
			Err(RequestError::NetworkError(err)) => {
				tracing::warn!(
					target: LOG_TARGET,
					hash = ?hash,
					para_id = ?para_id,
					peer_id = ?peer_id,
					err = ?err,
					"Fetching collation failed due to network error"
				);
				// A minor decrease in reputation for any network failure seems
				// sensbile. In theory this could be exploited, by DoSing this node,
				// which would result in reduced reputation for proper nodes, but the
				// same can happen for penalities on timeouts, which we also have.
				out = Some(FitnessEvent::NetworkError);
			}
			Err(RequestError::Canceled(_)) => {
				tracing::warn!(
					target: LOG_TARGET,
					hash = ?hash,
					para_id = ?para_id,
					peer_id = ?peer_id,
					"Request timed out"
				);
				// A minor decrease in reputation for any network failure seems
				// sensbile. In theory this could be exploited, by DoSing this node,
				// which would result in reduced reputation for proper nodes, but the
				// same can happen for penalities on timeouts, which we also have.
				out = Some(FitnessEvent::Timeout);
			}
			Ok(CollationFetchingResponse::Collation(receipt, _))
				if receipt.descriptor().para_id != *para_id =>
			{
				tracing::debug!(
					target: LOG_TARGET,
					expected_para_id = ?para_id,
					got_para_id = ?receipt.descriptor().para_id,
					peer_id = ?peer_id,
					"Got wrong para ID for requested collation."
				);
				out = Some(FitnessEvent::ParaError);
			}
			Ok(CollationFetchingResponse::Collation(receipt, pov)) => {
				tracing::debug!(
					target: LOG_TARGET,
					para_id = %para_id,
					hash = ?hash,
					candidate_hash = ?receipt.hash(),
					"Received collation",
				);
				// Actual sending:
				let _span = jaeger::Span::new(&pov, "received-collation");
				let (mut tx, _) = oneshot::channel();
				std::mem::swap(&mut tx, &mut (per_req.to_requester));
				let result = tx.send((receipt, pov));

				if let Err(_) = result  {
					tracing::warn!(
						target: LOG_TARGET,
						hash = ?hash,
						para_id = ?para_id,
						peer_id = ?peer_id,
						"Sending response back to requester failed (receiving side closed)"
					);
				} else {
					metrics_result = Ok(());
					success = "true";
				}
			}
		};
		metrics.on_request(metrics_result);
		per_req.span.as_mut().map(|s| s.add_string_tag("success", success));
	}

	out
}

async fn insert_event_by_peer_id<Context>(
	ctx: &mut Context,
	peer_slots: &mut PeerSlots,
	peer_id: &PeerId,
	event: FitnessEvent,
) where
	Context: SubsystemContext<Message = CollatorProtocolMessage>,
{
	if let Some(collator_id) = peer_collator_id(&peer_slots.peer_data, peer_id) {
		peer_slots::insert_event(ctx, peer_slots, peer_id, &collator_id, event).await;
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::{iter, time::Duration};
	use std::sync::Arc;
	use futures::{executor, future, Future};
	use sp_core::{crypto::Pair, Encode};
	use sp_keystore::SyncCryptoStore;
	use sp_keystore::testing::KeyStore as TestKeyStore;
	use sp_keyring::Sr25519Keyring;
	use assert_matches::assert_matches;

	use polkadot_primitives::v1::{
		CollatorPair, ValidatorId, ValidatorIndex, CoreState, CandidateDescriptor,
		GroupRotationInfo, ScheduledCore, OccupiedCore, GroupIndex,
	};
	use polkadot_node_primitives::BlockData;
	use polkadot_node_subsystem_util::TimeoutExt;
	use polkadot_subsystem_testhelpers as test_helpers;
	use polkadot_subsystem::messages::{RuntimeApiMessage, RuntimeApiRequest};
	use polkadot_node_network_protocol::{our_view, ObservedRole,
		request_response::Requests
	};
	use super::peer_slots::{
		BENEFIT_NOTIFY_GOOD, COST_INVALID_SIGNATURE, COST_REPORT_BAD, COST_UNNEEDED_COLLATOR,
		COST_REQUEST_TIMED_OUT,
	};
	const ACTIVITY_TIMEOUT: Duration = Duration::from_millis(50);
	const DECLARE_TIMEOUT: Duration = Duration::from_millis(25);

	#[derive(Clone)]
	struct TestState {
		chain_ids: Vec<ParaId>,
		relay_parent: Hash,
		collators: Vec<CollatorPair>,
		validators: Vec<Sr25519Keyring>,
		validator_public: Vec<ValidatorId>,
		validator_groups: Vec<Vec<ValidatorIndex>>,
		group_rotation_info: GroupRotationInfo,
		cores: Vec<CoreState>,
	}

	impl Default for TestState {
		fn default() -> Self {
			let chain_a = ParaId::from(1);
			let chain_b = ParaId::from(2);

			let chain_ids = vec![chain_a, chain_b];
			let relay_parent = Hash::repeat_byte(0x05);
			let collators = iter::repeat(())
				.map(|_| CollatorPair::generate().0)
				.take(4)
				.collect();

			let validators = vec![
				Sr25519Keyring::Alice,
				Sr25519Keyring::Bob,
				Sr25519Keyring::Charlie,
				Sr25519Keyring::Dave,
				Sr25519Keyring::Eve,
			];

			let validator_public = validators.iter().map(|k| k.public().into()).collect();
			let validator_groups = vec![
				vec![ValidatorIndex(0), ValidatorIndex(1)],
				vec![ValidatorIndex(2), ValidatorIndex(3)],
				vec![ValidatorIndex(4)],
			];

			let group_rotation_info = GroupRotationInfo {
				session_start_block: 0,
				group_rotation_frequency: 1,
				now: 0,
			};

			let cores = vec![
				CoreState::Scheduled(ScheduledCore {
					para_id: chain_ids[0],
					collator: None,
				}),
				CoreState::Free,
				CoreState::Occupied(OccupiedCore {
					next_up_on_available: None,
					occupied_since: 0,
					time_out_at: 1,
					next_up_on_time_out: None,
					availability: Default::default(),
					group_responsible: GroupIndex(0),
					candidate_hash: Default::default(),
					candidate_descriptor: {
						let mut d = CandidateDescriptor::default();
						d.para_id = chain_ids[1];

						d
					},
				}),
			];

			Self {
				chain_ids,
				relay_parent,
				collators,
				validators,
				validator_public,
				validator_groups,
				group_rotation_info,
				cores,
			}
		}
	}

	type VirtualOverseer = test_helpers::TestSubsystemContextHandle<CollatorProtocolMessage>;

	struct TestHarness {
		virtual_overseer: VirtualOverseer,
	}

	fn test_harness<T: Future<Output = VirtualOverseer>>(test: impl FnOnce(TestHarness) -> T) {
		let _ = env_logger::builder()
			.is_test(true)
			.filter(
				Some("polkadot_collator_protocol"),
				log::LevelFilter::Trace,
			)
			.filter(
				Some(LOG_TARGET),
				log::LevelFilter::Trace,
			)
			.try_init();

		let pool = sp_core::testing::TaskExecutor::new();

		let (context, virtual_overseer) = test_helpers::make_subsystem_context(pool.clone());

		let keystore = TestKeyStore::new();
		keystore.sr25519_generate_new(
			polkadot_primitives::v1::PARACHAIN_KEY_TYPE_ID,
			Some(&Sr25519Keyring::Alice.to_seed()),
		).unwrap();

		let subsystem = run(
			context,
			Arc::new(keystore),
			crate::CollatorEvictionPolicy {
				inactive_collator: ACTIVITY_TIMEOUT,
				undeclared: DECLARE_TIMEOUT,
			},
			Metrics::default(),
		);

		let test_fut = test(TestHarness { virtual_overseer });

		futures::pin_mut!(test_fut);
		futures::pin_mut!(subsystem);

		executor::block_on(future::join(async move {
			let mut overseer = test_fut.await;
			overseer_signal(&mut overseer, OverseerSignal::Conclude).await;
		}, subsystem)).1.unwrap();
	}

	const TIMEOUT: Duration = Duration::from_millis(200);

	async fn overseer_send(
		overseer: &mut VirtualOverseer,
		msg: CollatorProtocolMessage,
	) {
		tracing::trace!("Sending message:\n{:?}", &msg);
		overseer
			.send(FromOverseer::Communication { msg })
			.timeout(TIMEOUT)
			.await
			.expect(&format!("{:?} is enough for sending messages.", TIMEOUT));
	}

	async fn overseer_recv(
		overseer: &mut VirtualOverseer,
	) -> AllMessages {
		let msg = overseer_recv_with_timeout(overseer, TIMEOUT)
			.await
			.expect(&format!("{:?} is enough to receive messages.", TIMEOUT));

		tracing::trace!("Received message:\n{:?}", &msg);

		msg
	}

	async fn overseer_recv_with_timeout(
		overseer: &mut VirtualOverseer,
		timeout: Duration,
	) -> Option<AllMessages> {
		tracing::trace!("Waiting for message...");
		overseer
			.recv()
			.timeout(timeout)
			.await
	}

	async fn overseer_signal(
		overseer: &mut VirtualOverseer,
		signal: OverseerSignal,
	) {
		overseer
			.send(FromOverseer::Signal(signal))
			.timeout(TIMEOUT)
			.await
			.expect(&format!("{:?} is more than enough for sending signals.", TIMEOUT));
	}

	async fn respond_to_core_info_queries(
		virtual_overseer: &mut VirtualOverseer,
		test_state: &TestState,
	) {
		assert_matches!(
			overseer_recv(virtual_overseer).await,
			AllMessages::RuntimeApi(RuntimeApiMessage::Request(
				_,
				RuntimeApiRequest::Validators(tx),
			)) => {
				let _ = tx.send(Ok(test_state.validator_public.clone()));
			}
		);

		assert_matches!(
			overseer_recv(virtual_overseer).await,
			AllMessages::RuntimeApi(RuntimeApiMessage::Request(
				_,
				RuntimeApiRequest::ValidatorGroups(tx),
			)) => {
				let _ = tx.send(Ok((
					test_state.validator_groups.clone(),
					test_state.group_rotation_info.clone(),
				)));
			}
		);

		assert_matches!(
			overseer_recv(virtual_overseer).await,
			AllMessages::RuntimeApi(RuntimeApiMessage::Request(
				_,
				RuntimeApiRequest::AvailabilityCores(tx),
			)) => {
				let _ = tx.send(Ok(test_state.cores.clone()));
			}
		);
	}

	// As we receive a relevant advertisement act on it and issue a collation request.
	#[test]
	fn act_on_advertisement() {
		let test_state = TestState::default();

		test_harness(|test_harness| async move {
			let TestHarness {
				mut virtual_overseer,
			} = test_harness;

			let pair = CollatorPair::generate().0;
			tracing::trace!("activating");

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::OurViewChange(our_view![test_state.relay_parent])
				)
			).await;

			respond_to_core_info_queries(&mut virtual_overseer, &test_state).await;

			let peer_b = PeerId::random();

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerConnected(
						peer_b,
						ObservedRole::Full,
						None,
					),
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_b.clone(),
						protocol_v1::CollatorProtocolMessage::Declare(
							pair.public(),
							test_state.chain_ids[0],
							pair.sign(&protocol_v1::declare_signature_payload(&peer_b)),
						)
					)
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_b.clone(),
						protocol_v1::CollatorProtocolMessage::AdvertiseCollation(
							test_state.relay_parent,
						)
					)
				)
			).await;

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::SendRequests(reqs, IfDisconnected::ImmediateError)
			) => {
				let req = reqs.into_iter().next()
					.expect("There should be exactly one request");
				match req {
					Requests::CollationFetching(req) => {
						let payload = req.payload;
						assert_eq!(payload.relay_parent, test_state.relay_parent);
						assert_eq!(payload.para_id, test_state.chain_ids[0]);
					}
					_ => panic!("Unexpected request"),
				}
			});

			virtual_overseer
		});
	}

	// Test that other subsystems may modify collators' reputations.
	#[test]
	fn collator_reporting_works() {
		let test_state = TestState::default();

		test_harness(|test_harness| async move {
			let TestHarness {
				mut virtual_overseer,
			} = test_harness;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::OurViewChange(our_view![test_state.relay_parent])
				)
			).await;

			respond_to_core_info_queries(&mut virtual_overseer, &test_state).await;

			let peer_b = PeerId::random();
			let peer_c = PeerId::random();

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerConnected(
						peer_b,
						ObservedRole::Full,
						None,
					),
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerConnected(
						peer_c,
						ObservedRole::Full,
						None,
					),
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_b.clone(),
						protocol_v1::CollatorProtocolMessage::Declare(
							test_state.collators[0].public(),
							test_state.chain_ids[0],
							test_state.collators[0].sign(&protocol_v1::declare_signature_payload(&peer_b)),
						),
					)
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_c.clone(),
						protocol_v1::CollatorProtocolMessage::Declare(
							test_state.collators[1].public(),
							test_state.chain_ids[0],
							test_state.collators[1].sign(&protocol_v1::declare_signature_payload(&peer_c)),
						),
					)
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::ReportCollator(test_state.collators[0].public()),
			).await;

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(
					NetworkBridgeMessage::ReportPeer(peer, rep),
				) => {
					assert_eq!(peer, peer_b);
					assert_eq!(rep, COST_REPORT_BAD);
				}
			);

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NoteGoodCollation(test_state.collators[1].public()),
			).await;

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(
					NetworkBridgeMessage::ReportPeer(peer, rep),
				) => {
					assert_eq!(peer, peer_c);
					assert_eq!(rep, BENEFIT_NOTIFY_GOOD);
				}
			);
			virtual_overseer
		});
	}

	// Test that we verify the signatures on `Declare` and `AdvertiseCollation` messages.
	#[test]
	fn collator_authentication_verification_works() {
		let test_state = TestState::default();

		test_harness(|test_harness| async move {
			let TestHarness {
				mut virtual_overseer,
			} = test_harness;

			let peer_b = PeerId::random();

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerConnected(
						peer_b,
						ObservedRole::Full,
						None,
					),
				)
			).await;

			// the peer sends a declare message but sign the wrong payload
			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(NetworkBridgeEvent::PeerMessage(
					peer_b.clone(),
					protocol_v1::CollatorProtocolMessage::Declare(
						test_state.collators[0].public(),
						test_state.chain_ids[0],
						test_state.collators[0].sign(&[42]),
					),
				)),
			)
			.await;

			// it should be reported for sending a message with an invalid signature
			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(
					NetworkBridgeMessage::ReportPeer(peer, rep),
				) => {
					assert_eq!(peer, peer_b);
					assert_eq!(rep, COST_INVALID_SIGNATURE);
				}
			);
			virtual_overseer
		});
	}

	// A test scenario that takes the following steps
	//  - Two collators connect, declare themselves and advertise a collation relevant to
	//	our view.
	//  - This results subsystem acting upon these advertisements and issuing two messages to
	//	the CandidateBacking subsystem.
	//  - CandidateBacking requests both of the collations.
	//  - Collation protocol requests these collations.
	//  - The collations are sent to it.
	//  - Collations are fetched correctly.
	#[test]
	fn fetch_collations_works() {
		let test_state = TestState::default();

		test_harness(|test_harness| async move {
			let TestHarness {
				mut virtual_overseer,
			} = test_harness;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::OurViewChange(our_view![test_state.relay_parent])
				),
			).await;

			respond_to_core_info_queries(&mut virtual_overseer, &test_state).await;

			let peer_b = PeerId::random();
			let peer_c = PeerId::random();

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerConnected(
						peer_b,
						ObservedRole::Full,
						None,
					),
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerConnected(
						peer_c,
						ObservedRole::Full,
						None,
					),
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_b.clone(),
						protocol_v1::CollatorProtocolMessage::Declare(
							test_state.collators[0].public(),
							test_state.chain_ids[0],
							test_state.collators[0].sign(&protocol_v1::declare_signature_payload(&peer_b)),
						)
					)
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_c.clone(),
						protocol_v1::CollatorProtocolMessage::Declare(
							test_state.collators[1].public(),
							test_state.chain_ids[0],
							test_state.collators[1].sign(&protocol_v1::declare_signature_payload(&peer_c)),
						)
					)
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_b.clone(),
						protocol_v1::CollatorProtocolMessage::AdvertiseCollation(
							test_state.relay_parent,
						)
					)
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_c.clone(),
						protocol_v1::CollatorProtocolMessage::AdvertiseCollation(
							test_state.relay_parent,
						)
					)
				)
			).await;

			let response_channel = assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::SendRequests(reqs, IfDisconnected::ImmediateError)
			) => {
				let req = reqs.into_iter().next()
					.expect("There should be exactly one request");
				match req {
					Requests::CollationFetching(req) => {
						let payload = req.payload;
						assert_eq!(payload.relay_parent, test_state.relay_parent);
						assert_eq!(payload.para_id, test_state.chain_ids[0]);
						req.pending_response
					}
					_ => panic!("Unexpected request"),
				}
			});

			let mut candidate_a = CandidateReceipt::default();
			candidate_a.descriptor.para_id = test_state.chain_ids[0];
			candidate_a.descriptor.relay_parent = test_state.relay_parent;
			response_channel.send(Ok(
				CollationFetchingResponse::Collation(
					candidate_a.clone(),
					PoV {
						block_data: BlockData(vec![]),
					},
				).encode()
			)).expect("Sending response should succeed");

			let response_channel = assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::SendRequests(reqs, IfDisconnected::ImmediateError)
			) => {
				let req = reqs.into_iter().next()
					.expect("There should be exactly one request");
				match req {
					Requests::CollationFetching(req) => {
						let payload = req.payload;
						assert_eq!(payload.relay_parent, test_state.relay_parent);
						assert_eq!(payload.para_id, test_state.chain_ids[0]);
						req.pending_response
					}
					_ => panic!("Unexpected request"),
				}
			});

			let mut candidate_b = CandidateReceipt::default();
			candidate_b.descriptor.para_id = test_state.chain_ids[0];
			candidate_b.descriptor.relay_parent = test_state.relay_parent;

			response_channel.send(Ok(
				CollationFetchingResponse::Collation(
					candidate_b.clone(),
					PoV {
						block_data: BlockData(vec![1, 2, 3]),
					},
				).encode()
			)).expect("Sending response should succeed");

			virtual_overseer
		});
	}

	#[test]
	fn inactive_disconnected() {
		let test_state = TestState::default();

		test_harness(|test_harness| async move {
			let TestHarness {
				mut virtual_overseer,
			} = test_harness;

			let pair = CollatorPair::generate().0;

			let hash_a = test_state.relay_parent;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::OurViewChange(our_view![hash_a])
				)
			).await;

			respond_to_core_info_queries(&mut virtual_overseer, &test_state).await;

			let peer_b = PeerId::random();

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerConnected(
						peer_b.clone(),
						ObservedRole::Full,
						None,
					)
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_b.clone(),
						protocol_v1::CollatorProtocolMessage::Declare(
							pair.public(),
							test_state.chain_ids[0],
							pair.sign(&protocol_v1::declare_signature_payload(&peer_b)),
						)
					)
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_b.clone(),
						protocol_v1::CollatorProtocolMessage::AdvertiseCollation(
							test_state.relay_parent,
						)
					)
				)
			).await;

			let _ = assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::SendRequests(reqs, IfDisconnected::ImmediateError)
			) => {
				let req = reqs.into_iter().next()
					.expect("There should be exactly one request");
				match req {
					Requests::CollationFetching(req) => {
						let payload = req.payload;
						assert_eq!(payload.relay_parent, test_state.relay_parent);
						assert_eq!(payload.para_id, test_state.chain_ids[0]);
						req.pending_response
					}
					_ => panic!("Unexpected request"),
				}
			});

			Delay::new(ACTIVITY_TIMEOUT * 3).await;

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::ReportPeer(
					peer,
					rep,
				)) => {
					assert_eq!(peer, peer_b);
					assert_eq!(rep, COST_REQUEST_TIMED_OUT);
				}
			);

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::DisconnectPeer(
					peer,
					peer_set,
				)) => {
					assert_eq!(peer, peer_b);
					assert_eq!(peer_set, PeerSet::Collation);
				}
			);
			virtual_overseer
		});
	}

	#[test]
	fn activity_extends_life() {
		let test_state = TestState::default();

		test_harness(|test_harness| async move {
			let TestHarness {
				mut virtual_overseer,
			} = test_harness;

			let pair = CollatorPair::generate().0;

			let hash_a = test_state.relay_parent;
			let hash_b = Hash::repeat_byte(1);
			let hash_c = Hash::repeat_byte(2);

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::OurViewChange(our_view![hash_a, hash_b, hash_c])
				)
			).await;

			// 3 heads, 3 times.
			respond_to_core_info_queries(&mut virtual_overseer, &test_state).await;
			respond_to_core_info_queries(&mut virtual_overseer, &test_state).await;
			respond_to_core_info_queries(&mut virtual_overseer, &test_state).await;

			let peer_b = PeerId::random();

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerConnected(
						peer_b.clone(),
						ObservedRole::Full,
						None,
					)
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_b.clone(),
						protocol_v1::CollatorProtocolMessage::Declare(
							pair.public(),
							test_state.chain_ids[0],
							pair.sign(&protocol_v1::declare_signature_payload(&peer_b)),
						)
					)
				)
			).await;

			Delay::new(ACTIVITY_TIMEOUT * 2 / 3).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_b.clone(),
						protocol_v1::CollatorProtocolMessage::AdvertiseCollation(
							hash_a,
						)
					)
				)
			).await;

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::SendRequests(reqs, IfDisconnected::ImmediateError)
			) => {
				let req = reqs.into_iter().next()
					.expect("There should be exactly one request");
				match req {
					Requests::CollationFetching(req) => {
						let payload = req.payload;
						assert_eq!(payload.relay_parent, hash_a);
						assert_eq!(payload.para_id, test_state.chain_ids[0]);
					}
					_ => panic!("Unexpected request"),
				}
			});

			Delay::new(ACTIVITY_TIMEOUT * 2 / 3).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_b.clone(),
						protocol_v1::CollatorProtocolMessage::AdvertiseCollation(
							hash_b
						)
					)
				)
			).await;

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::ReportPeer(
					peer,
					rep,
				)) => {
					assert_eq!(peer, peer_b);
					assert_eq!(rep, COST_REQUEST_TIMED_OUT);
				}
			);

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::SendRequests(reqs, IfDisconnected::ImmediateError)
			) => {
				let req = reqs.into_iter().next()
					.expect("There should be exactly one request");
				match req {
					Requests::CollationFetching(req) => {
						let payload = req.payload;
						assert_eq!(payload.relay_parent, hash_b);
						assert_eq!(payload.para_id, test_state.chain_ids[0]);
					}
					_ => panic!("Unexpected request"),
				}
			});

			Delay::new(ACTIVITY_TIMEOUT * 2 / 3).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_b.clone(),
						protocol_v1::CollatorProtocolMessage::AdvertiseCollation(
							hash_c,
						)
					)
				)
			).await;

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::ReportPeer(
					peer,
					rep,
				)) => {
					assert_eq!(peer, peer_b);
					assert_eq!(rep, COST_REQUEST_TIMED_OUT);
				}
			);

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::SendRequests(reqs, IfDisconnected::ImmediateError)
			) => {
				let req = reqs.into_iter().next()
					.expect("There should be exactly one request");
				match req {
					Requests::CollationFetching(req) => {
						let payload = req.payload;
						assert_eq!(payload.relay_parent, hash_c);
						assert_eq!(payload.para_id, test_state.chain_ids[0]);
					}
					_ => panic!("Unexpected request"),
				}
			});

			Delay::new(ACTIVITY_TIMEOUT * 3 / 2).await;

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::ReportPeer(
					peer,
					rep,
				)) => {
					assert_eq!(peer, peer_b);
					assert_eq!(rep, COST_REQUEST_TIMED_OUT);
				}
			);

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::DisconnectPeer(
					peer,
					peer_set,
				)) => {
					assert_eq!(peer, peer_b);
					assert_eq!(peer_set, PeerSet::Collation);
				}
			);
			virtual_overseer
		});
	}

	#[test]
	fn disconnect_if_no_declare() {
		let test_state = TestState::default();

		test_harness(|test_harness| async move {
			let TestHarness {
				mut virtual_overseer,
			} = test_harness;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::OurViewChange(our_view![test_state.relay_parent])
				)
			).await;

			respond_to_core_info_queries(&mut virtual_overseer, &test_state).await;

			let peer_b = PeerId::random();

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerConnected(
						peer_b.clone(),
						ObservedRole::Full,
						None,
					)
				)
			).await;

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::DisconnectPeer(
					peer,
					peer_set,
				)) => {
					assert_eq!(peer, peer_b);
					assert_eq!(peer_set, PeerSet::Collation);
				}
			);
			virtual_overseer
		})
	}

	#[test]
	fn disconnect_if_wrong_declare() {
		let test_state = TestState::default();

		test_harness(|test_harness| async move {
			let TestHarness {
				mut virtual_overseer,
			} = test_harness;

			let pair = CollatorPair::generate().0;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::OurViewChange(our_view![test_state.relay_parent])
				)
			).await;

			respond_to_core_info_queries(&mut virtual_overseer, &test_state).await;

			let peer_b = PeerId::random();

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerConnected(
						peer_b.clone(),
						ObservedRole::Full,
						None,
					)
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_b.clone(),
						protocol_v1::CollatorProtocolMessage::Declare(
							pair.public(),
							ParaId::from(69),
							pair.sign(&protocol_v1::declare_signature_payload(&peer_b)),
						)
					)
				)
			).await;

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::ReportPeer(
					peer,
					rep,
				)) => {
					assert_eq!(peer, peer_b);
					assert_eq!(rep, COST_UNNEEDED_COLLATOR);
				}
			);

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::DisconnectPeer(
					peer,
					peer_set,
				)) => {
					assert_eq!(peer, peer_b);
					assert_eq!(peer_set, PeerSet::Collation);
				}
			);
			virtual_overseer
		})
	}

	#[test]
	fn view_change_clears_old_collators() {
		let mut test_state = TestState::default();

		test_harness(|test_harness| async move {
			let TestHarness {
				mut virtual_overseer,
			} = test_harness;

			let pair = CollatorPair::generate().0;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::OurViewChange(our_view![test_state.relay_parent])
				)
			).await;

			respond_to_core_info_queries(&mut virtual_overseer, &test_state).await;

			let peer_b = PeerId::random();

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerConnected(
						peer_b.clone(),
						ObservedRole::Full,
						None,
					)
				)
			).await;

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::PeerMessage(
						peer_b.clone(),
						protocol_v1::CollatorProtocolMessage::Declare(
							pair.public(),
							test_state.chain_ids[0],
							pair.sign(&protocol_v1::declare_signature_payload(&peer_b)),
						)
					)
				)
			).await;

			let hash_b = Hash::repeat_byte(69);

			overseer_send(
				&mut virtual_overseer,
				CollatorProtocolMessage::NetworkBridgeUpdateV1(
					NetworkBridgeEvent::OurViewChange(our_view![hash_b])
				)
			).await;

			test_state.group_rotation_info = test_state.group_rotation_info.bump_rotation();
			respond_to_core_info_queries(&mut virtual_overseer, &test_state).await;

			assert_matches!(
				overseer_recv(&mut virtual_overseer).await,
				AllMessages::NetworkBridge(NetworkBridgeMessage::DisconnectPeer(
					peer,
					peer_set,
				)) => {
					assert_eq!(peer, peer_b);
					assert_eq!(peer_set, PeerSet::Collation);
				}
			);
			virtual_overseer
		})
	}
}
