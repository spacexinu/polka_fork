// Copyright 2021 Parity Technologies (UK) Ltd.
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

use std::{
	collections::HashMap,
	sync::{
		atomic::{AtomicU64, Ordering as AtomicOrdering},
		Arc,
	},
	time::Duration,
};

use assert_matches::assert_matches;
use futures::{
	channel::oneshot,
	future::{self, BoxFuture},
};

use kvdb::KeyValueDB;
use parity_scale_codec::Encode;

use polkadot_node_primitives::SignedDisputeStatement;
use polkadot_node_subsystem::{
	messages::{DisputeCoordinatorMessage, DisputeDistributionMessage, ImportStatementsResult},
	overseer::FromOverseer,
	OverseerSignal,
};
use polkadot_node_subsystem_util::TimeoutExt;
use sc_keystore::LocalKeystore;
use sp_core::testing::TaskExecutor;
use sp_keyring::Sr25519Keyring;
use sp_keystore::{SyncCryptoStore, SyncCryptoStorePtr};

use polkadot_node_subsystem::{
	jaeger,
	messages::{AllMessages, BlockDescription, RuntimeApiMessage, RuntimeApiRequest},
	ActivatedLeaf, ActiveLeavesUpdate, LeafStatus,
};
use polkadot_node_subsystem_test_helpers::{make_subsystem_context, TestSubsystemContextHandle};
use polkadot_primitives::v1::{
	BlakeTwo256, BlockNumber, CandidateHash, CandidateReceipt, Hash, HashT, Header,
	ScrapedOnChainVotes, SessionIndex, SessionInfo, ValidatorId, ValidatorIndex,
};

use crate::{
	metrics::Metrics,
	real::{
		backend::Backend, participation::participation_full_happy_path,
		status::ACTIVE_DURATION_SECS,
	},
	Config, DisputeCoordinatorSubsystem,
};

use super::{
	db::v1::DbBackend,
	status::{Clock, Timestamp},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

// sets up a keystore with the given keyring accounts.
fn make_keystore(accounts: &[Sr25519Keyring]) -> LocalKeystore {
	let store = LocalKeystore::in_memory();

	for s in accounts.iter().copied().map(|k| k.to_seed()) {
		store
			.sr25519_generate_new(polkadot_primitives::v1::PARACHAIN_KEY_TYPE_ID, Some(s.as_str()))
			.unwrap();
	}

	store
}

fn session_to_hash(session: SessionIndex, extra: impl Encode) -> Hash {
	BlakeTwo256::hash_of(&(session, extra))
}

type VirtualOverseer = TestSubsystemContextHandle<DisputeCoordinatorMessage>;

#[derive(Clone)]
struct MockClock {
	time: Arc<AtomicU64>,
}

impl Default for MockClock {
	fn default() -> Self {
		MockClock { time: Arc::new(AtomicU64::default()) }
	}
}

impl Clock for MockClock {
	fn now(&self) -> Timestamp {
		self.time.load(AtomicOrdering::SeqCst)
	}
}

impl MockClock {
	fn set(&self, to: Timestamp) {
		self.time.store(to, AtomicOrdering::SeqCst)
	}
}

struct TestState {
	validators: Vec<Sr25519Keyring>,
	validator_public: Vec<ValidatorId>,
	validator_groups: Vec<Vec<ValidatorIndex>>,
	master_keystore: Arc<sc_keystore::LocalKeystore>,
	subsystem_keystore: Arc<sc_keystore::LocalKeystore>,
	db: Arc<dyn KeyValueDB>,
	config: Config,
	clock: MockClock,
	headers: HashMap<Hash, Header>,
}

impl Default for TestState {
	fn default() -> TestState {
		let validators = vec![
			Sr25519Keyring::Alice,
			Sr25519Keyring::Bob,
			Sr25519Keyring::Charlie,
			Sr25519Keyring::Dave,
			Sr25519Keyring::Eve,
			Sr25519Keyring::One,
			Sr25519Keyring::Ferdie,
		];

		let validator_public = validators.iter().map(|k| ValidatorId::from(k.public())).collect();

		let validator_groups = vec![
			vec![ValidatorIndex(0), ValidatorIndex(1)],
			vec![ValidatorIndex(2), ValidatorIndex(3)],
			vec![ValidatorIndex(4), ValidatorIndex(5), ValidatorIndex(6)],
		];

		let master_keystore = make_keystore(&validators).into();
		let subsystem_keystore = make_keystore(&[Sr25519Keyring::Alice]).into();

		let db = Arc::new(kvdb_memorydb::create(1));
		let config = Config { col_data: 0 };

		TestState {
			validators,
			validator_public,
			validator_groups,
			master_keystore,
			subsystem_keystore,
			db,
			config,
			clock: MockClock::default(),
			headers: HashMap::new(),
		}
	}
}

impl TestState {
	async fn activate_leaf_at_session(
		&mut self,
		virtual_overseer: &mut VirtualOverseer,
		session: SessionIndex,
		block_number: BlockNumber,
	) {
		assert!(block_number > 0);

		let parent_hash = session_to_hash(session, b"parent");
		let block_header = Header {
			parent_hash,
			number: block_number,
			digest: Default::default(),
			state_root: Default::default(),
			extrinsics_root: Default::default(),
		};
		let block_hash = block_header.hash();

		let _ = self.headers.insert(block_hash, block_header.clone());

		virtual_overseer
			.send(FromOverseer::Signal(OverseerSignal::ActiveLeaves(
				ActiveLeavesUpdate::start_work(ActivatedLeaf {
					hash: block_hash,
					span: Arc::new(jaeger::Span::Disabled),
					number: block_number,
					status: LeafStatus::Fresh,
				}),
			)))
			.await;

		self.handle_sync_queries(virtual_overseer, block_hash, session).await;
	}

	async fn handle_sync_queries(
		&self,
		virtual_overseer: &mut VirtualOverseer,
		block_hash: Hash,
		session: SessionIndex,
	) {
		assert_matches!(
			virtual_overseer.recv().await,
			AllMessages::RuntimeApi(RuntimeApiMessage::Request(
				h,
				RuntimeApiRequest::SessionIndexForChild(tx),
			)) => {
				assert_eq!(h, block_hash);
				let _ = tx.send(Ok(session));
			}
		);

		loop {
			// answer session info queries until the current session is reached.
			assert_matches!(
				virtual_overseer.recv().await,
				AllMessages::RuntimeApi(RuntimeApiMessage::Request(
					h,
					RuntimeApiRequest::SessionInfo(session_index, tx),
				)) => {
					assert_eq!(h, block_hash);

					let _ = tx.send(Ok(Some(self.session_info())));
					if session_index == session { break }
				}
			)
		}

		assert_matches!(
			virtual_overseer.recv().await,
			AllMessages::RuntimeApi(RuntimeApiMessage::Request(
				_new_leaf,
				RuntimeApiRequest::CandidateEvents(tx),
			)) => {
				tx.send(Ok(Vec::new())).unwrap();
			}
		);

		assert_matches!(
			virtual_overseer.recv().await,
			AllMessages::RuntimeApi(RuntimeApiMessage::Request(
				_new_leaf,
				RuntimeApiRequest::FetchOnChainVotes(tx),
			)) => {
				//add some `BackedCandidates` or resolved disputes here as needed
				tx.send(Ok(Some(ScrapedOnChainVotes::default()))).unwrap();
			}
		)
	}

	async fn handle_resume_sync(
		&self,
		virtual_overseer: &mut VirtualOverseer,
		session: SessionIndex,
	) {
		let leaves: Vec<Hash> = self.headers.keys().cloned().collect();
		for (n, leaf) in leaves.iter().enumerate() {
			virtual_overseer
				.send(FromOverseer::Signal(OverseerSignal::ActiveLeaves(
					ActiveLeavesUpdate::start_work(ActivatedLeaf {
						hash: *leaf,
						number: n as u32,
						span: Arc::new(jaeger::Span::Disabled),
						status: LeafStatus::Fresh,
					}),
				)))
				.await;

			self.handle_sync_queries(virtual_overseer, *leaf, session).await;
		}
	}

	fn session_info(&self) -> SessionInfo {
		let discovery_keys = self.validators.iter().map(|k| <_>::from(k.public())).collect();

		let assignment_keys = self.validators.iter().map(|k| <_>::from(k.public())).collect();

		SessionInfo {
			validators: self.validator_public.clone(),
			discovery_keys,
			assignment_keys,
			validator_groups: self.validator_groups.clone(),
			n_cores: self.validator_groups.len() as _,
			zeroth_delay_tranche_width: 0,
			relay_vrf_modulo_samples: 1,
			n_delay_tranches: 100,
			no_show_slots: 1,
			needed_approvals: 10,
		}
	}

	async fn issue_statement_with_index(
		&self,
		index: usize,
		candidate_hash: CandidateHash,
		session: SessionIndex,
		valid: bool,
	) -> SignedDisputeStatement {
		let public = self.validator_public[index].clone();

		let keystore = self.master_keystore.clone() as SyncCryptoStorePtr;

		SignedDisputeStatement::sign_explicit(&keystore, valid, candidate_hash, session, public)
			.await
			.unwrap()
			.unwrap()
	}

	fn resume<F>(self, test: F) -> Self
	where
		F: FnOnce(TestState, VirtualOverseer) -> BoxFuture<'static, TestState>,
	{
		let (ctx, ctx_handle) = make_subsystem_context(TaskExecutor::new());
		let subsystem = DisputeCoordinatorSubsystem::new(
			self.db.clone(),
			self.config.clone(),
			self.subsystem_keystore.clone(),
			Metrics::default(),
		);
		let backend = DbBackend::new(self.db.clone(), self.config.column_config());
		let subsystem_task = subsystem.run(ctx, backend, Box::new(self.clock.clone()));
		let test_task = test(self, ctx_handle);

		let (_, state) = futures::executor::block_on(future::join(subsystem_task, test_task));
		state
	}
}

fn test_harness<F>(test: F) -> TestState
where
	F: FnOnce(TestState, VirtualOverseer) -> BoxFuture<'static, TestState>,
{
	TestState::default().resume(test)
}

#[test]
fn conflicting_votes_lead_to_dispute_participation() {
	test_harness(|mut test_state, mut virtual_overseer| {
		Box::pin(async move {
			let session = 1;

			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			let candidate_receipt = CandidateReceipt::default();
			let candidate_hash = candidate_receipt.hash();

			test_state.activate_leaf_at_session(&mut virtual_overseer, session, 1).await;

			let valid_vote =
				test_state.issue_statement_with_index(3, candidate_hash, session, true).await;

			let invalid_vote =
				test_state.issue_statement_with_index(1, candidate_hash, session, false).await;

			let invalid_vote_2 =
				test_state.issue_statement_with_index(2, candidate_hash, session, false).await;

			let (pending_confirmation, _confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements: vec![
							(valid_vote, ValidatorIndex(3)),
							(invalid_vote, ValidatorIndex(1)),
						],
						pending_confirmation,
					},
				})
				.await;

			participation_full_happy_path(&mut virtual_overseer).await;

			{
				let (tx, rx) = oneshot::channel();
				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::ActiveDisputes(tx),
					})
					.await;

				assert_eq!(rx.await.unwrap(), vec![(session, candidate_hash)]);

				let (tx, rx) = oneshot::channel();
				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::QueryCandidateVotes(
							vec![(session, candidate_hash)],
							tx,
						),
					})
					.await;

				let (_, _, votes) = rx.await.unwrap().get(0).unwrap().clone();
				assert_eq!(votes.valid.len(), 1);
				assert_eq!(votes.invalid.len(), 1);
			}

			let (pending_confirmation, _confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements: vec![(invalid_vote_2, ValidatorIndex(2))],
						pending_confirmation,
					},
				})
				.await;

			{
				let (tx, rx) = oneshot::channel();
				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::QueryCandidateVotes(
							vec![(session, candidate_hash)],
							tx,
						),
					})
					.await;

				let (_, _, votes) = rx.await.unwrap().get(0).unwrap().clone();
				assert_eq!(votes.valid.len(), 1);
				assert_eq!(votes.invalid.len(), 2);
			}

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;

			// This confirms that the second vote doesn't lead to participation again.
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	});
}

#[test]
fn positive_votes_dont_trigger_participation() {
	test_harness(|mut test_state, mut virtual_overseer| {
		Box::pin(async move {
			let session = 1;

			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			let candidate_receipt = CandidateReceipt::default();
			let candidate_hash = candidate_receipt.hash();

			test_state.activate_leaf_at_session(&mut virtual_overseer, session, 1).await;

			let valid_vote =
				test_state.issue_statement_with_index(2, candidate_hash, session, true).await;

			let valid_vote_2 =
				test_state.issue_statement_with_index(1, candidate_hash, session, true).await;

			let (pending_confirmation, _confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements: vec![(valid_vote, ValidatorIndex(2))],
						pending_confirmation,
					},
				})
				.await;

			{
				let (tx, rx) = oneshot::channel();
				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::ActiveDisputes(tx),
					})
					.await;

				assert!(rx.await.unwrap().is_empty());

				let (tx, rx) = oneshot::channel();
				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::QueryCandidateVotes(
							vec![(session, candidate_hash)],
							tx,
						),
					})
					.await;

				let (_, _, votes) = rx.await.unwrap().get(0).unwrap().clone();
				assert_eq!(votes.valid.len(), 1);
				assert!(votes.invalid.is_empty());
			}

			let (pending_confirmation, _confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements: vec![(valid_vote_2, ValidatorIndex(1))],
						pending_confirmation,
					},
				})
				.await;

			{
				let (tx, rx) = oneshot::channel();
				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::ActiveDisputes(tx),
					})
					.await;

				assert!(rx.await.unwrap().is_empty());

				let (tx, rx) = oneshot::channel();
				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::QueryCandidateVotes(
							vec![(session, candidate_hash)],
							tx,
						),
					})
					.await;

				let (_, _, votes) = rx.await.unwrap().get(0).unwrap().clone();
				assert_eq!(votes.valid.len(), 2);
				assert!(votes.invalid.is_empty());
			}

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;

			// This confirms that no participation request is made.
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	});
}

#[test]
fn wrong_validator_index_is_ignored() {
	test_harness(|mut test_state, mut virtual_overseer| {
		Box::pin(async move {
			let session = 1;

			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			let candidate_receipt = CandidateReceipt::default();
			let candidate_hash = candidate_receipt.hash();

			test_state.activate_leaf_at_session(&mut virtual_overseer, session, 1).await;

			let valid_vote =
				test_state.issue_statement_with_index(2, candidate_hash, session, true).await;

			let invalid_vote =
				test_state.issue_statement_with_index(1, candidate_hash, session, false).await;

			let (pending_confirmation, _confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements: vec![
							(valid_vote, ValidatorIndex(1)),
							(invalid_vote, ValidatorIndex(2)),
						],
						pending_confirmation,
					},
				})
				.await;

			{
				let (tx, rx) = oneshot::channel();
				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::ActiveDisputes(tx),
					})
					.await;

				assert!(rx.await.unwrap().is_empty());

				let (tx, rx) = oneshot::channel();
				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::QueryCandidateVotes(
							vec![(session, candidate_hash)],
							tx,
						),
					})
					.await;

				let (_, _, votes) = rx.await.unwrap().get(0).unwrap().clone();
				assert!(votes.valid.is_empty());
				assert!(votes.invalid.is_empty());
			}

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;

			// This confirms that no participation request is made.
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	});
}

#[test]
fn finality_votes_ignore_disputed_candidates() {
	test_harness(|mut test_state, mut virtual_overseer| {
		Box::pin(async move {
			let session = 1;

			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			let candidate_receipt = CandidateReceipt::default();
			let candidate_hash = candidate_receipt.hash();

			test_state.activate_leaf_at_session(&mut virtual_overseer, session, 1).await;

			let valid_vote =
				test_state.issue_statement_with_index(2, candidate_hash, session, true).await;

			let invalid_vote =
				test_state.issue_statement_with_index(1, candidate_hash, session, false).await;

			let (pending_confirmation, _confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements: vec![
							(valid_vote, ValidatorIndex(2)),
							(invalid_vote, ValidatorIndex(1)),
						],
						pending_confirmation,
					},
				})
				.await;

			participation_full_happy_path(&mut virtual_overseer).await;

			{
				let (tx, rx) = oneshot::channel();

				let base_block = Hash::repeat_byte(0x0f);
				let block_hash_a = Hash::repeat_byte(0x0a);
				let block_hash_b = Hash::repeat_byte(0x0b);

				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::DetermineUndisputedChain {
							base: (10, base_block),
							block_descriptions: vec![BlockDescription {
								block_hash: block_hash_a,
								session,
								candidates: vec![candidate_hash],
							}],
							tx,
						},
					})
					.await;

				assert_eq!(rx.await.unwrap(), (10, base_block));

				let (tx, rx) = oneshot::channel();
				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::DetermineUndisputedChain {
							base: (10, base_block),
							block_descriptions: vec![
								BlockDescription {
									block_hash: block_hash_a,
									session,
									candidates: vec![],
								},
								BlockDescription {
									block_hash: block_hash_b,
									session,
									candidates: vec![candidate_hash],
								},
							],
							tx,
						},
					})
					.await;

				assert_eq!(rx.await.unwrap(), (11, block_hash_a));
			}

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	});
}

#[test]
fn supermajority_valid_dispute_may_be_finalized() {
	test_harness(|mut test_state, mut virtual_overseer| {
		Box::pin(async move {
			let session = 1;

			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			let candidate_receipt = CandidateReceipt::default();
			let candidate_hash = candidate_receipt.hash();

			test_state.activate_leaf_at_session(&mut virtual_overseer, session, 1).await;

			let supermajority_threshold =
				polkadot_primitives::v1::supermajority_threshold(test_state.validators.len());

			let valid_vote =
				test_state.issue_statement_with_index(2, candidate_hash, session, true).await;

			let invalid_vote =
				test_state.issue_statement_with_index(1, candidate_hash, session, false).await;

			let (pending_confirmation, _confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements: vec![
							(valid_vote, ValidatorIndex(2)),
							(invalid_vote, ValidatorIndex(1)),
						],
						pending_confirmation,
					},
				})
				.await;

			participation_full_happy_path(&mut virtual_overseer).await;

			let mut statements = Vec::new();
			for i in (0..supermajority_threshold - 1).map(|i| i + 3) {
				let vote =
					test_state.issue_statement_with_index(i, candidate_hash, session, true).await;

				statements.push((vote, ValidatorIndex(i as _)));
			}

			let (pending_confirmation, _confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements,
						pending_confirmation,
					},
				})
				.await;

			{
				let (tx, rx) = oneshot::channel();

				let base_hash = Hash::repeat_byte(0x0f);
				let block_hash_a = Hash::repeat_byte(0x0a);
				let block_hash_b = Hash::repeat_byte(0x0b);

				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::DetermineUndisputedChain {
							base: (10, base_hash),
							block_descriptions: vec![BlockDescription {
								block_hash: block_hash_a,
								session,
								candidates: vec![candidate_hash],
							}],
							tx,
						},
					})
					.await;

				assert_eq!(rx.await.unwrap(), (11, block_hash_a));

				let (tx, rx) = oneshot::channel();
				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::DetermineUndisputedChain {
							base: (10, base_hash),
							block_descriptions: vec![
								BlockDescription {
									block_hash: block_hash_a,
									session,
									candidates: vec![],
								},
								BlockDescription {
									block_hash: block_hash_b,
									session,
									candidates: vec![candidate_hash],
								},
							],
							tx,
						},
					})
					.await;

				assert_eq!(rx.await.unwrap(), (12, block_hash_b));
			}

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	});
}

#[test]
fn concluded_supermajority_for_non_active_after_time() {
	test_harness(|mut test_state, mut virtual_overseer| {
		Box::pin(async move {
			let session = 1;

			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			let candidate_receipt = CandidateReceipt::default();
			let candidate_hash = candidate_receipt.hash();

			test_state.activate_leaf_at_session(&mut virtual_overseer, session, 1).await;

			let supermajority_threshold =
				polkadot_primitives::v1::supermajority_threshold(test_state.validators.len());

			let valid_vote =
				test_state.issue_statement_with_index(2, candidate_hash, session, true).await;

			let invalid_vote =
				test_state.issue_statement_with_index(1, candidate_hash, session, false).await;

			let (pending_confirmation, _confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements: vec![
							(valid_vote, ValidatorIndex(2)),
							(invalid_vote, ValidatorIndex(1)),
						],
						pending_confirmation,
					},
				})
				.await;

			participation_full_happy_path(&mut virtual_overseer).await;

			let mut statements = Vec::new();
			for i in (0..supermajority_threshold - 1).map(|i| i + 3) {
				let vote =
					test_state.issue_statement_with_index(i, candidate_hash, session, true).await;

				statements.push((vote, ValidatorIndex(i as _)));
			}

			let (pending_confirmation, _confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements,
						pending_confirmation,
					},
				})
				.await;

			test_state.clock.set(ACTIVE_DURATION_SECS + 1);

			{
				let (tx, rx) = oneshot::channel();

				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::ActiveDisputes(tx),
					})
					.await;

				assert!(rx.await.unwrap().is_empty());

				let (tx, rx) = oneshot::channel();

				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::RecentDisputes(tx),
					})
					.await;

				assert_eq!(rx.await.unwrap().len(), 1);
			}

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	});
}

#[test]
fn concluded_supermajority_against_non_active_after_time() {
	test_harness(|mut test_state, mut virtual_overseer| {
		Box::pin(async move {
			let session = 1;

			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			let candidate_receipt = CandidateReceipt::default();
			let candidate_hash = candidate_receipt.hash();

			test_state.activate_leaf_at_session(&mut virtual_overseer, session, 1).await;

			let supermajority_threshold =
				polkadot_primitives::v1::supermajority_threshold(test_state.validators.len());

			let valid_vote =
				test_state.issue_statement_with_index(2, candidate_hash, session, true).await;

			let invalid_vote =
				test_state.issue_statement_with_index(1, candidate_hash, session, false).await;

			let (pending_confirmation, _confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements: vec![
							(valid_vote, ValidatorIndex(2)),
							(invalid_vote, ValidatorIndex(1)),
						],
						pending_confirmation,
					},
				})
				.await;

			participation_full_happy_path(&mut virtual_overseer).await;

			let mut statements = Vec::new();
			for i in (0..supermajority_threshold - 1).map(|i| i + 3) {
				let vote =
					test_state.issue_statement_with_index(i, candidate_hash, session, false).await;

				statements.push((vote, ValidatorIndex(i as _)));
			}

			let (pending_confirmation, _confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements,
						pending_confirmation,
					},
				})
				.await;

			test_state.clock.set(ACTIVE_DURATION_SECS + 1);

			{
				let (tx, rx) = oneshot::channel();

				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::ActiveDisputes(tx),
					})
					.await;

				assert!(rx.await.unwrap().is_empty());

				let (tx, rx) = oneshot::channel();

				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::RecentDisputes(tx),
					})
					.await;

				assert_eq!(rx.await.unwrap().len(), 1);
			}

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	});
}

#[test]
fn resume_dispute_without_local_statement() {
	let session = 1;

	test_harness(|mut test_state, mut virtual_overseer| {
		Box::pin(async move {
			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			let candidate_receipt = CandidateReceipt::default();
			let candidate_hash = candidate_receipt.hash();

			test_state.activate_leaf_at_session(&mut virtual_overseer, session, 1).await;

			let valid_vote =
				test_state.issue_statement_with_index(1, candidate_hash, session, true).await;

			let invalid_vote =
				test_state.issue_statement_with_index(2, candidate_hash, session, false).await;

			let (pending_confirmation, confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements: vec![
							(valid_vote, ValidatorIndex(1)),
							(invalid_vote, ValidatorIndex(2)),
						],
						pending_confirmation,
					},
				})
				.await;

			participation_full_happy_path(&mut virtual_overseer).await;

			assert_eq!(confirmation_rx.await, Ok(ImportStatementsResult::ValidImport));

			{
				let (tx, rx) = oneshot::channel();

				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::ActiveDisputes(tx),
					})
					.await;

				assert_eq!(rx.await.unwrap().len(), 1);
			}

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	})
	// Alice should send a DisputeParticiationMessage::Participate on restart since she has no
	// local statement for the active dispute.
	.resume(|test_state, mut virtual_overseer| {
		Box::pin(async move {
			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			let candidate_receipt = CandidateReceipt::default();
			let candidate_hash = candidate_receipt.hash();

			participation_full_happy_path(&mut virtual_overseer).await;

			let valid_vote0 =
				test_state.issue_statement_with_index(0, candidate_hash, session, true).await;
			let valid_vote3 =
				test_state.issue_statement_with_index(3, candidate_hash, session, true).await;
			let valid_vote4 =
				test_state.issue_statement_with_index(4, candidate_hash, session, true).await;
			let valid_vote5 =
				test_state.issue_statement_with_index(5, candidate_hash, session, true).await;

			let (pending_confirmation, _confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements: vec![
							(valid_vote0, ValidatorIndex(0)),
							(valid_vote3, ValidatorIndex(3)),
							(valid_vote4, ValidatorIndex(4)),
							(valid_vote5, ValidatorIndex(5)),
						],
						pending_confirmation,
					},
				})
				.await;

			// Advance the clock far enough so that the concluded dispute will be omitted from an
			// ActiveDisputes query.
			test_state.clock.set(test_state.clock.now() + ACTIVE_DURATION_SECS + 1);

			{
				let (tx, rx) = oneshot::channel();

				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::ActiveDisputes(tx),
					})
					.await;

				assert!(rx.await.unwrap().is_empty());
			}

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	});
}

#[test]
fn resume_dispute_with_local_statement() {
	let session = 1;

	test_harness(|mut test_state, mut virtual_overseer| {
		Box::pin(async move {
			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			let candidate_receipt = CandidateReceipt::default();
			let candidate_hash = candidate_receipt.hash();

			test_state.activate_leaf_at_session(&mut virtual_overseer, session, 1).await;

			let local_valid_vote =
				test_state.issue_statement_with_index(0, candidate_hash, session, true).await;

			let valid_vote =
				test_state.issue_statement_with_index(1, candidate_hash, session, true).await;

			let invalid_vote =
				test_state.issue_statement_with_index(2, candidate_hash, session, false).await;

			let (pending_confirmation, confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements: vec![
							(local_valid_vote, ValidatorIndex(0)),
							(valid_vote, ValidatorIndex(1)),
							(invalid_vote, ValidatorIndex(2)),
						],
						pending_confirmation,
					},
				})
				.await;

			assert_eq!(confirmation_rx.await, Ok(ImportStatementsResult::ValidImport));

			{
				let (tx, rx) = oneshot::channel();

				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::ActiveDisputes(tx),
					})
					.await;

				assert_eq!(rx.await.unwrap().len(), 1);
			}

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	})
	// Alice should not send a DisputeParticiationMessage::Participate on restart since she has a
	// local statement for the active dispute.
	.resume(|test_state, mut virtual_overseer| {
		Box::pin(async move {
			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			// Assert that subsystem is not sending Participation messages because we issued a local statement
			assert!(virtual_overseer.recv().timeout(TEST_TIMEOUT).await.is_none());

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	});
}

#[test]
fn resume_dispute_without_local_statement_or_local_key() {
	let session = 1;
	let mut test_state = TestState::default();
	test_state.subsystem_keystore = make_keystore(&[Sr25519Keyring::Two]).into();
	test_state
		.resume(|mut test_state, mut virtual_overseer| {
			Box::pin(async move {
				test_state.handle_resume_sync(&mut virtual_overseer, session).await;

				let candidate_receipt = CandidateReceipt::default();
				let candidate_hash = candidate_receipt.hash();

				test_state.activate_leaf_at_session(&mut virtual_overseer, session, 1).await;

				let valid_vote =
					test_state.issue_statement_with_index(1, candidate_hash, session, true).await;

				let invalid_vote =
					test_state.issue_statement_with_index(2, candidate_hash, session, false).await;

				let (pending_confirmation, confirmation_rx) = oneshot::channel();
				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::ImportStatements {
							candidate_hash,
							candidate_receipt: candidate_receipt.clone(),
							session,
							statements: vec![
								(valid_vote, ValidatorIndex(1)),
								(invalid_vote, ValidatorIndex(2)),
							],
							pending_confirmation,
						},
					})
					.await;

				assert_eq!(confirmation_rx.await, Ok(ImportStatementsResult::ValidImport));

				{
					let (tx, rx) = oneshot::channel();

					virtual_overseer
						.send(FromOverseer::Communication {
							msg: DisputeCoordinatorMessage::ActiveDisputes(tx),
						})
						.await;

					assert_eq!(rx.await.unwrap().len(), 1);
				}

				virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;
				assert!(virtual_overseer.try_recv().await.is_none());

				test_state
			})
		})
		// Two should not send a DisputeParticiationMessage::Participate on restart since she is no
		// validator in that dispute.
		.resume(|test_state, mut virtual_overseer| {
			Box::pin(async move {
				test_state.handle_resume_sync(&mut virtual_overseer, session).await;

				// Assert that subsystem is not sending Participation messages because we issued a local statement
				assert!(virtual_overseer.recv().timeout(TEST_TIMEOUT).await.is_none());

				virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;
				assert!(virtual_overseer.try_recv().await.is_none());

				test_state
			})
		});
}

#[test]
fn resume_dispute_with_local_statement_without_local_key() {
	let session = 1;

	let test_state = TestState::default();
	let mut test_state = test_state.resume(|mut test_state, mut virtual_overseer| {
		Box::pin(async move {
			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			let candidate_receipt = CandidateReceipt::default();
			let candidate_hash = candidate_receipt.hash();

			test_state.activate_leaf_at_session(&mut virtual_overseer, session, 1).await;

			let local_valid_vote =
				test_state.issue_statement_with_index(0, candidate_hash, session, true).await;

			let valid_vote =
				test_state.issue_statement_with_index(1, candidate_hash, session, true).await;

			let invalid_vote =
				test_state.issue_statement_with_index(2, candidate_hash, session, false).await;

			let (pending_confirmation, confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements: vec![
							(local_valid_vote, ValidatorIndex(0)),
							(valid_vote, ValidatorIndex(1)),
							(invalid_vote, ValidatorIndex(2)),
						],
						pending_confirmation,
					},
				})
				.await;

			assert_eq!(confirmation_rx.await, Ok(ImportStatementsResult::ValidImport));

			{
				let (tx, rx) = oneshot::channel();

				virtual_overseer
					.send(FromOverseer::Communication {
						msg: DisputeCoordinatorMessage::ActiveDisputes(tx),
					})
					.await;

				assert_eq!(rx.await.unwrap().len(), 1);
			}

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	});
	// No keys:
	test_state.subsystem_keystore = make_keystore(&[Sr25519Keyring::Two]).into();
	// Two should not send a DisputeParticiationMessage::Participate on restart since we gave
	// her a non existing key.
	test_state.resume(|test_state, mut virtual_overseer| {
		Box::pin(async move {
			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			// Assert that subsystem is not sending Participation messages because we don't
			// have a key.
			assert!(virtual_overseer.recv().timeout(TEST_TIMEOUT).await.is_none());

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	});
}

#[test]
fn issue_valid_local_statement_does_cause_distribution_but_not_duplicate_participation() {
	issue_local_statement_does_cause_distribution_but_not_duplicate_participation(true);
}

#[test]
fn issue_invalid_local_statement_does_cause_distribution_but_not_duplicate_participation() {
	issue_local_statement_does_cause_distribution_but_not_duplicate_participation(false);
}

fn issue_local_statement_does_cause_distribution_but_not_duplicate_participation(validity: bool) {
	test_harness(|mut test_state, mut virtual_overseer| {
		Box::pin(async move {
			let session = 1;

			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			let candidate_receipt = CandidateReceipt::default();
			let candidate_hash = candidate_receipt.hash();

			test_state.activate_leaf_at_session(&mut virtual_overseer, session, 1).await;

			let other_vote = test_state
				.issue_statement_with_index(1, candidate_hash, session, !validity)
				.await;

			let (pending_confirmation, confirmation_rx) = oneshot::channel();
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::ImportStatements {
						candidate_hash,
						candidate_receipt: candidate_receipt.clone(),
						session,
						statements: vec![(other_vote, ValidatorIndex(1))],
						pending_confirmation,
					},
				})
				.await;

			assert_eq!(confirmation_rx.await, Ok(ImportStatementsResult::ValidImport));

			// Initiate dispute locally:
			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::IssueLocalStatement(
						session,
						candidate_hash,
						candidate_receipt.clone(),
						validity,
					),
				})
				.await;

			// Dispute distribution should get notified now:
			assert_matches!(
				virtual_overseer.recv().await,
				AllMessages::DisputeDistribution(
					DisputeDistributionMessage::SendDispute(msg)
				) => {
					assert_eq!(msg.session_index(), session);
					assert_eq!(msg.candidate_receipt(), &candidate_receipt);
				}
			);

			// Make sure we won't participate:
			assert!(virtual_overseer.recv().timeout(TEST_TIMEOUT).await.is_none());

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	});
}

#[test]
fn negative_issue_local_statement_only_triggers_import() {
	test_harness(|mut test_state, mut virtual_overseer| {
		Box::pin(async move {
			let session = 1;

			test_state.handle_resume_sync(&mut virtual_overseer, session).await;

			let candidate_receipt = CandidateReceipt::default();
			let candidate_hash = candidate_receipt.hash();

			test_state.activate_leaf_at_session(&mut virtual_overseer, session, 1).await;

			virtual_overseer
				.send(FromOverseer::Communication {
					msg: DisputeCoordinatorMessage::IssueLocalStatement(
						session,
						candidate_hash,
						candidate_receipt.clone(),
						false,
					),
				})
				.await;

			let backend = DbBackend::new(test_state.db.clone(), test_state.config.column_config());

			let votes = backend.load_candidate_votes(session, &candidate_hash).unwrap().unwrap();
			assert_eq!(votes.invalid.len(), 1);
			assert_eq!(votes.valid.len(), 0);

			let disputes = backend.load_recent_disputes().unwrap();
			assert_eq!(disputes, None);

			// Assert that subsystem is not participating.
			assert!(virtual_overseer.recv().timeout(TEST_TIMEOUT).await.is_none());

			virtual_overseer.send(FromOverseer::Signal(OverseerSignal::Conclude)).await;
			assert!(virtual_overseer.try_recv().await.is_none());

			test_state
		})
	});
}
