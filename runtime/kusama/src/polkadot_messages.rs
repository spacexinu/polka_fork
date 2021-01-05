// Copyright 2019-2020 Parity Technologies (UK) Ltd.
// This file is part of Parity Bridges Common.

// Parity Bridges Common is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Parity Bridges Common is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Parity Bridges Common.  If not, see <http://www.gnu.org/licenses/>.

//! Everything required to serve Kusama <-> Polkadot message lanes.

use crate::Runtime;

use bp_message_lane::{
	source_chain::TargetHeaderChain,
	target_chain::{ProvedMessages, SourceHeaderChain},
	InboundLaneData, LaneId, Message, MessageNonce,
};
use bp_runtime::{InstanceId, POLKADOT_BRIDGE_INSTANCE};
use bridge_runtime_common::messages::{self, ChainWithMessageLanes, MessageBridge};
use frame_support::{
	weights::{Weight, WeightToFeePolynomial, DispatchClass},
	RuntimeDebug,
};
use runtime_common::{BlockWeights, BlockLength};
use sp_core::storage::StorageKey;
use sp_std::{convert::TryFrom, ops::RangeInclusive};

/// Storage key of the Kusama -> Polkadot message in the runtime storage.
pub fn message_key(lane: &LaneId, nonce: MessageNonce) -> StorageKey {
	pallet_message_lane::storage_keys::message_key::<Runtime, <Kusama as ChainWithMessageLanes>::MessageLaneInstance>(
		lane, nonce,
	)
}

/// Storage key of the Kusama -> Polkadot message lane state in the runtime storage.
pub fn outbound_lane_data_key(lane: &LaneId) -> StorageKey {
	pallet_message_lane::storage_keys::outbound_lane_data_key::<<Kusama as ChainWithMessageLanes>::MessageLaneInstance>(
		lane,
	)
}

/// Storage key of the Polkadot -> Kusama message lane state in the runtime storage.
pub fn inbound_lane_data_key(lane: &LaneId) -> StorageKey {
	pallet_message_lane::storage_keys::inbound_lane_data_key::<
		Runtime,
		<Kusama as ChainWithMessageLanes>::MessageLaneInstance,
	>(lane)
}

/// Message payload for Kusama -> Polkadot messages.
pub type ToPolkadotMessagePayload = messages::source::FromThisChainMessagePayload<WithPolkadotMessageBridge>;

/// Message verifier for Kusama -> Polkadot messages.
pub type ToPolkadotMessageVerifier = messages::source::FromThisChainMessageVerifier<WithPolkadotMessageBridge>;

/// Message payload for Polkadot -> Kusama messages.
pub type FromPolkadotMessagePayload = messages::target::FromBridgedChainMessagePayload<WithPolkadotMessageBridge>;

/// Messages proof for Polkadot -> Kusama messages.
type FromPolkadotMessagesProof = messages::target::FromBridgedChainMessagesProof<WithPolkadotMessageBridge>;

/// Messages delivery proof for Kusama -> Polkadot messages.
type ToPolkadotMessagesDeliveryProof = messages::source::FromBridgedChainMessagesDeliveryProof<WithPolkadotMessageBridge>;

/// Call-dispatch based message dispatch for Polkadot -> Kusama messages.
pub type FromPolkadotMessageDispatch = messages::target::FromBridgedChainMessageDispatch<
	WithPolkadotMessageBridge,
	crate::Runtime,
	crate::PolkadotCallDispatchInstance,
>;

/// Kusama <-> Polkadot message bridge.
#[derive(RuntimeDebug, Clone, Copy)]
pub struct WithPolkadotMessageBridge;

impl MessageBridge for WithPolkadotMessageBridge {
	const INSTANCE: InstanceId = POLKADOT_BRIDGE_INSTANCE;

	const RELAYER_FEE_PERCENT: u32 = 10;

	type ThisChain = Kusama;
	type BridgedChain = Polkadot;

	fn maximal_extrinsic_size_on_target_chain() -> u32 {
		*BlockLength::get().max.get(DispatchClass::Normal)
	}

	fn weight_limits_of_message_on_bridged_chain(message_payload: &[u8]) -> RangeInclusive<Weight> {
		// we don't want to relay too large messages + keep reserve for future upgrades
		let max_extrinsic_weight = BlockWeights::get()
			.get(DispatchClass::Normal)
			.max_extrinsic
			.unwrap_or(Weight::MAX);
		let upper_limit = max_extrinsic_weight / 2;

		// given Millau chain parameters (`TransactionByteFee`, `WeightToFee`, `FeeMultiplierUpdate`),
		// the minimal weight of the message may be computed as message.length()
		let lower_limit = Weight::try_from(message_payload.len()).unwrap_or(Weight::MAX);

		lower_limit..=upper_limit
	}

	fn weight_of_delivery_transaction() -> Weight {
		0 // TODO: https://github.com/paritytech/parity-bridges-common/issues/391
	}

	fn weight_of_delivery_confirmation_transaction_on_this_chain() -> Weight {
		0 // TODO: https://github.com/paritytech/parity-bridges-common/issues/391
	}

	fn weight_of_reward_confirmation_transaction_on_target_chain() -> Weight {
		0 // TODO: https://github.com/paritytech/parity-bridges-common/issues/391
	}

	fn this_weight_to_this_balance(weight: Weight) -> bp_kusama::Balance {
		<crate::Runtime as pallet_transaction_payment::Config>::WeightToFee::calc(&weight)
	}

	fn bridged_weight_to_bridged_balance(weight: Weight) -> bp_polkadot::Balance {
		// we're using the same weights in both chains now
		<crate::Runtime as pallet_transaction_payment::Config>::WeightToFee::calc(&weight) as _
	}

	fn bridged_balance_to_this_balance(bridged_balance: bp_polkadot::Balance) -> bp_kusama::Balance {
		// 1:1 conversion that will probably change in the future
		bridged_balance as _
	}
}

/// Kusama chain from message lane point of view.
#[derive(RuntimeDebug, Clone, Copy)]
pub struct Kusama;

impl messages::ChainWithMessageLanes for Kusama {
	type Hash = crate::Hash;
	type AccountId = crate::AccountId;
	type Signer = crate::AccountPublic;
	type Signature = crate::Signature;
	type Call = crate::Call;
	type Weight = Weight;
	type Balance = crate::Balance;

	type MessageLaneInstance = crate::PolkadotMessageLaneInstance;
}

/// Polkadot chain from message lane point of view.
#[derive(RuntimeDebug, Clone, Copy)]
pub struct Polkadot;

impl messages::ChainWithMessageLanes for Polkadot {
	type Hash = bp_polkadot::Hash;
	type AccountId = bp_polkadot::AccountId;
	type Signer = bp_polkadot::AccountPublic;
	type Signature = bp_polkadot::Signature;
	type Call = (); // unknown to us
	type Weight = Weight;
	type Balance = bp_polkadot::Balance;

	// this is also Instance1, but since it is instance in the other runtime, let's not use alias
	type MessageLaneInstance = pallet_message_lane::Instance1;
}

impl TargetHeaderChain<ToPolkadotMessagePayload, bp_polkadot::AccountId> for Polkadot {
	type Error = &'static str;
	// The proof is:
	// - hash of the header this proof has been created with;
	// - the storage proof of one or several keys;
	// - id of the lane we prove state of.
	type MessagesDeliveryProof = ToPolkadotMessagesDeliveryProof;

	fn verify_message(payload: &ToPolkadotMessagePayload) -> Result<(), Self::Error> {
		messages::source::verify_chain_message::<WithPolkadotMessageBridge>(payload)
	}

	fn verify_messages_delivery_proof(
		proof: Self::MessagesDeliveryProof,
	) -> Result<(LaneId, InboundLaneData<crate::AccountId>), Self::Error> {
		messages::source::verify_messages_delivery_proof::<WithPolkadotMessageBridge, Runtime>(proof)
	}
}

impl SourceHeaderChain<bp_polkadot::Balance> for Polkadot {
	type Error = &'static str;
	type MessagesProof = FromPolkadotMessagesProof;

	fn verify_messages_proof(
		proof: Self::MessagesProof,
		max_messages: MessageNonce,
	) -> Result<ProvedMessages<Message<bp_polkadot::Balance>>, Self::Error> {
		messages::target::verify_messages_proof::<WithPolkadotMessageBridge, Runtime>(proof, max_messages)
	}
}
