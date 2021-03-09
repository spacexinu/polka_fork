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

//! Everything required to serve Polkadot <-> Kusama message lanes.
//!
//! (this file should be a part of Polkadot runtime).

use crate::Runtime;

use bp_message_lane::{
	source_chain::TargetHeaderChain,
	target_chain::{ProvedMessages, SourceHeaderChain},
	InboundLaneData, LaneId, Message, MessageNonce, Parameter as MessageLaneParameter,
};
use bp_runtime::{InstanceId, KUSAMA_BRIDGE_INSTANCE};
use bridge_runtime_common::messages::{self, ChainWithMessageLanes, MessageBridge, MessageLaneTransaction};
use codec::{Decode, Encode};
use frame_support::{
	parameter_types,
	weights::{DispatchClass, Weight},
	RuntimeDebug,
};
use sp_core::storage::StorageKey;
use sp_runtime::{FixedPointNumber, FixedU128};
use sp_std::{convert::TryFrom, ops::RangeInclusive};

parameter_types! {
	/// Kusama to Polkadot conversion rate.
	///
	/// At the moment when this code was written (09.03.2021), the rates were:
	///
	/// - Kusama: 245.64$ (https://coinmarketcap.com/currencies/kusama/);
	/// - Polkadot: 34.43$ (https://coinmarketcap.com/currencies/polkadot-new/).
	///
	/// This value is used in the following expression: `polkadot_balance = rate * kusama_balance`.
	/// The `polkadot_balance` is then used to charge message submitter for the message sending.
	/// So the rate should be `3443 / 24564 ~ 0.14`. But since we don't want to update this rate
	/// whenever it is actually changed + overcharging is better than undercharging in our case,
	/// let's make some reserve. Let it be `rate = max_over_7d(Polkadot) / min_over_7d(Kusama)`:
	///
	/// - min_over_7d(Kusama): 212.47$ (https://coinmarketcap.com/currencies/kusama/);
	/// - max_over_7d(Polkadot): 38.49$ (https://coinmarketcap.com/currencies/polkadot-new/).
	///
	/// So our conversion rate with reserve would be `3848 / 21247 ~ 0.18`.
	///
	/// If actual value of the rate will be larger than ours, it'll mean that we're overcharging
	/// message senders. If it'll be lesser, it'll mean that message senders are undercharged
	/// and non-altruistic relayers may choose not to deliver messages.
	storage KusamaToPolkadotConversionRate: FixedU128 = FixedU128::saturating_from_rational(3848, 21247);

	/// Expected value of `NextFeeMultiplier` at Kusama.
	///
	/// At the moment when this code was written (09.03.2021), the value on both chains was
	/// `1_000_000_000`, which is equal to `MinimumMultiplier`. But there's an
	/// https://github.com/paritytech/polkadot/pull/2481, where minimal value is going to be decreased
	/// even further. So given that (1) both chains are not congested right now (2) overcharging
	/// message sender is better than undercharging (3) maximal possible daily increase of `NextFeeMultiplier`
	/// on Polkadot is approximately 40% (https://gist.github.com/svyatonik/66f6393a92e47d276652f8abeb24bc1c) 
	/// => let's set initial value of this parameter to the `MinimumMultiplier * 140%`.
	///
	/// If actual value of the multiplier will be lessaer than ours, it'll mean that we're overcharging
	/// message senders. If it'll be lesser, it'll mean that message senders are undercharged
	/// and non-altruistic relayers may choose not to deliver messages.
	storage KusamaNextFeeMultiplier: FixedU128 = bp_kusama::MINIMUM_NEXT_FEE_MULTIPLIER
		.saturated_mul(FixedU128::saturating_from_rational(140, 100));
}

/// Storage key of the Polkadot -> Kusama message in the runtime storage.
pub fn message_key(lane: &LaneId, nonce: MessageNonce) -> StorageKey {
	pallet_message_lane::storage_keys::message_key::<Runtime, <Polkadot as ChainWithMessageLanes>::MessageLaneInstance>(
		lane, nonce,
	)
}

/// Storage key of the Polkadot -> Kusama message lane state in the runtime storage.
pub fn outbound_lane_data_key(lane: &LaneId) -> StorageKey {
	pallet_message_lane::storage_keys::outbound_lane_data_key::<<Polkadot as ChainWithMessageLanes>::MessageLaneInstance>(
		lane,
	)
}

/// Storage key of the Kusama -> Polkadot message lane state in the runtime storage.
pub fn inbound_lane_data_key(lane: &LaneId) -> StorageKey {
	pallet_message_lane::storage_keys::inbound_lane_data_key::<
		Runtime,
		<Polkadot as ChainWithMessageLanes>::MessageLaneInstance,
	>(lane)
}

/// Message payload for Polkadot -> Kusama messages.
pub type ToKusamaMessagePayload = messages::source::FromThisChainMessagePayload<WithKusamaMessageBridge>;

/// Message verifier for Polkadot -> Kusama messages.
pub type ToKusamaMessageVerifier = messages::source::FromThisChainMessageVerifier<WithKusamaMessageBridge>;

/// Message payload for Kusama -> Polkadot messages.
pub type FromKusamaMessagePayload = messages::target::FromBridgedChainMessagePayload<WithKusamaMessageBridge>;

/// Encoded Polkadot Call as it comes from Kusama.
pub type FromKusamaEncodedCall = messages::target::FromBridgedChainEncodedMessageCall<WithKusamaMessageBridge>;

/// Call-dispatch based message dispatch for Kusama -> Polkadot messages.
pub type FromKusamaMessageDispatch = messages::target::FromBridgedChainMessageDispatch<
	WithKusamaMessageBridge,
	crate::Runtime,
	pallet_bridge_call_dispatch::DefaultInstance,
>;

/// Messages proof for Kusama -> Polkadot messages.
pub type FromKusamaMessagesProof = messages::target::FromBridgedChainMessagesProof<bp_kusama::Hash>;

/// Messages delivery proof for Polkadot -> Kusama messages.
pub type ToKusamaMessagesDeliveryProof = messages::source::FromBridgedChainMessagesDeliveryProof<bp_kusama::Hash>;

/// Kusama <-> Polkadot message bridge.
#[derive(RuntimeDebug, Clone, Copy)]
pub struct WithKusamaMessageBridge;

impl MessageBridge for WithKusamaMessageBridge {
	const INSTANCE: InstanceId = KUSAMA_BRIDGE_INSTANCE;

	const RELAYER_FEE_PERCENT: u32 = 10;

	type ThisChain = Polkadot;
	type BridgedChain = Kusama;

	fn bridged_balance_to_this_balance(bridged_balance: bp_kusama::Balance) -> bp_polkadot::Balance {
		bp_polkadot::Balance::try_from(KusamaToPolkadotConversionRate::get().saturating_mul_int(bridged_balance))
			.unwrap_or(bp_polkadot::Balance::MAX)
	}
}

/// Polkadot chain from message lane point of view.
#[derive(RuntimeDebug, Clone, Copy)]
pub struct Polkadot;

impl messages::ChainWithMessageLanes for Polkadot {
	type Hash = bp_polkadot::Hash;
	type AccountId = bp_polkadot::AccountId;
	type Signer = bp_polkadot::AccountSigner;
	type Signature = bp_polkadot::Signature;
	type Weight = Weight;
	type Balance = bp_polkadot::Balance;

	type MessageLaneInstance = crate::WithKusamaMessageLaneInstance;
}

impl messages::ThisChainWithMessageLanes for Polkadot {
	type Call = crate::Call;

	fn is_outbound_lane_enabled(lane: &LaneId) -> bool {
		*lane == LaneId::default()
	}

	fn maximal_pending_messages_at_outbound_lane() -> MessageNonce {
		// we don't want too many pending messages in the Polkadot -> Kusama lanes
		// => the maximal theoretical number of messages that can be delivered in the single delivery transaction
		bp_kusama::MAX_UNCONFIRMED_MESSAGES_AT_INBOUND_LANE
	}

	fn estimate_delivery_confirmation_transaction() -> MessageLaneTransaction<Weight> {
		let inbound_data_size = InboundLaneData::<bp_polkadot::AccountId>::encoded_size_hint(
			bp_polkadot::MAXIMAL_ENCODED_ACCOUNT_ID_SIZE,
			1,
		).unwrap_or(u32::MAX);

		MessageLaneTransaction {
			dispatch_weight: bp_polkadot::MAX_SINGLE_MESSAGE_DELIVERY_CONFIRMATION_TX_WEIGHT,
			size: inbound_data_size
				.saturating_add(bp_kusama::EXTRA_STORAGE_PROOF_SIZE)
				.saturating_add(bp_polkadot::TX_EXTRA_BYTES),
		}
	}

	fn transaction_payment(transaction: MessageLaneTransaction<Weight>) -> bp_rialto::Balance {
		// see `KusamaNextFeeMultiplier` for details - we're increasing multiplier for
		// this chain a bit, because it may be increased before delivery confirmation transaction
		// will be submitted
		const THIS_CHAIN_NEXT_FEE_MULTIPLIER_INCREASE_COEFF: FixedU128::saturating_from_rational(105, 100);

		messages::transaction_payment(
			runtime_common::BlockWeights::get().get(DispatchClass::Normal).base_extrinsic,
			crate::TransactionByteFee::get(),
			|weight| crate::WeightToFee::calc(weight),
			<Runtime as pallet_transaction_payment::Config>::next_fee_multiplier()
				.saturating_mul(THIS_CHAIN_NEXT_FEE_MULTIPLIER_INCREASE_COEFF),
			transaction,
		)
	}
}

/// Kusama chain from message lane point of view.
#[derive(RuntimeDebug, Clone, Copy)]
pub struct Kusama;

impl messages::ChainWithMessageLanes for Kusama {
	type Hash = bp_kusama::Hash;
	type AccountId = bp_kusama::AccountId;
	type Signer = bp_kusama::AccountSigner;
	type Signature = bp_kusama::Signature;
	type Weight = Weight;
	type Balance = bp_kusama::Balance;

	type MessageLaneInstance = pallet_message_lane::DefaultInstance;
}

impl messages::BridgedChainWithMessageLanes for Kusama {
	fn maximal_extrinsic_size() -> u32 {
		bp_kusama::max_extrinsic_size()
	}

	fn message_weight_limits(_message_payload: &[u8]) -> RangeInclusive<Weight> {
		// we don't want to relay too large messages + keep reserve for future upgrades
		let upper_limit = messages::target::maximal_incoming_message_dispatch_weight(bp_kusama::max_extrinsic_weight());

		// we're charging for payload bytes in `WithKusamaMessageBridge::transaction_payment` function
		//
		// this bridge may be used to deliver all kind of messages, so we're not making any assumptions about
		// minimal dispatch weight here

		0..=upper_limit
	}

	fn estimate_delivery_transaction(
		message_payload: &[u8],
		message_dispatch_weight: Weight,
	) -> MessageLaneTransaction<Weight> {
		let message_payload_len = u32::try_from(message_payload.len()).unwrap_or(u32::MAX);
		let extra_bytes_in_payload = Weight::from(message_payload_len)
			.saturating_sub(pallet_message_lane::EXPECTED_DEFAULT_MESSAGE_LENGTH.into());

		MessageLaneTransaction {
			dispatch_weight: extra_bytes_in_payload
				.saturating_mul(bp_kusama::ADDITIONAL_MESSAGE_BYTE_DELIVERY_WEIGHT)
				.saturating_add(bp_kusama::DEFAULT_MESSAGE_DELIVERY_TX_WEIGHT)
				.saturating_add(message_dispatch_weight),
			size: message_payload_len
				.saturating_add(bp_polkadot::EXTRA_STORAGE_PROOF_SIZE)
				.saturating_add(bp_kusama::TX_EXTRA_BYTES),
		}
	}

	fn transaction_payment(transaction: MessageLaneTransaction<Weight>) -> bp_kusama::Balance {
		messages::transaction_payment(
			bp_kusama::BlockWeights::get().get(DispatchClass::Normal).base_extrinsic,
			bp_kusama::TRANSACTION_BYTE_FEE,
			|weight| bp_kusama::WeightToFee::calc(weight),
			KusamaNextFeeMultiplier::get(),
			transaction,
		)
	}
}

impl TargetHeaderChain<ToKusamaMessagePayload, bp_kusama::AccountId> for Kusama {
	type Error = &'static str;
	// The proof is:
	// - hash of the header this proof has been created with;
	// - the storage proof of one or several keys;
	// - id of the lane we prove state of.
	type MessagesDeliveryProof = ToKusamaMessagesDeliveryProof;

	fn verify_message(payload: &ToKusamaMessagePayload) -> Result<(), Self::Error> {
		messages::source::verify_chain_message::<WithKusamaMessageBridge>(payload)
	}

	fn verify_messages_delivery_proof(
		proof: Self::MessagesDeliveryProof,
	) -> Result<(LaneId, InboundLaneData<bp_polkadot::AccountId>), Self::Error> {
		messages::source::verify_messages_delivery_proof::<WithKusamaMessageBridge, Runtime>(proof)
	}
}

impl SourceHeaderChain<bp_kusama::Balance> for Kusama {
	type Error = &'static str;
	// The proof is:
	// - hash of the header this proof has been created with;
	// - the storage proof of one or several keys;
	// - id of the lane we prove messages for;
	// - inclusive range of messages nonces that are proved.
	type MessagesProof = FromKusamaMessagesProof;

	fn verify_messages_proof(
		proof: Self::MessagesProof,
		messages_count: u32,
	) -> Result<ProvedMessages<Message<bp_kusama::Balance>>, Self::Error> {
		messages::target::verify_messages_proof::<WithKusamaMessageBridge, Runtime>(proof, messages_count)
	}
}

/// Polkadot <-> Kusama message lane pallet parameters.
#[derive(RuntimeDebug, Clone, Encode, Decode, PartialEq, Eq)]
pub enum WithKusamaMessageLaneParameter {
	/// Kusama to Polkadot conversion rate. Is used to update `KusamaToPolkadotConversionRate` value.
	KusamaToPolkadotConversionRate(FixedU128),
	/// `NextFeeMultiplier` value on Kusama chain. Is used to update `KusamaNextFeeMultiplier` value.
	KusamaNextFeeMultiplier(FixedU128),
}

impl MessageLaneParameter for WithKusamaMessageLaneParameter {
	fn save(&self) {
		match *self {
			WithKusamaMessageLaneParameter::KusamaToPolkadotConversionRate(ref conversion_rate) => {
				KusamaToPolkadotConversionRate::set(conversion_rate),
			},
			WithKusamaMessageLaneParameter::KusamaNextFeeMultiplier(ref multiplier) => {
				KusamaNextFeeMultiplier::set(multiplier),
			},
		}
	}
}
