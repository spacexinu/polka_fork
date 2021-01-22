// Copyright 2017-2020 Parity Technologies (UK) Ltd.
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

//! Module to handle parathread/parachain registration and related fund management.
//! In essence this is a simple wrapper around `paras`.

use crate::WASM_MAGIC;
use sp_std::{prelude::*, result};
use frame_support::{
	decl_storage, decl_module, decl_error, decl_event, ensure,
	dispatch::DispatchResult,
	traits::{Get, Currency, ReservableCurrency},
};
use frame_system::{self, ensure_root, ensure_signed};
use primitives::v1::{
	Id as ParaId, ValidationCode, HeadData,
};
use runtime_parachains::{
	paras::{
		self,
		ParaGenesisArgs,
	},
	dmp, ump, hrmp,
	ensure_parachain,
	Origin,
};
use crate::slots;

use parity_scale_codec::{Encode, Decode};

type BalanceOf<T> =
	<<T as Config>::Currency as Currency<<T as frame_system::Config>::AccountId>>::Balance;

pub trait Config: frame_system::Config + paras::Config {
	/// The overarching event type.
	type Event: From<Event<Self>> + Into<<Self as frame_system::Config>::Event>;

	/// The aggregated origin type must support the `parachains` origin. We require that we can
	/// infallibly convert between this origin and the system origin, but in reality, they're the
	/// same type, we just can't express that to the Rust type system without writing a `where`
	/// clause everywhere.
	type Origin: From<<Self as frame_system::Config>::Origin>
		+ Into<result::Result<Origin, <Self as Config>::Origin>>;

	/// The system's currency for parathread payment.
	type Currency: ReservableCurrency<Self::AccountId>;

	/// The deposit to be paid to register a Para. Should include the cost of registering
	/// validation code and genesis head data.
	type ParaDeposit: Get<BalanceOf<Self>>;

	/// The maximum size for the validation code.
	type MaxCodeSize: Get<u32>;

	/// The maximum size for the head code.
	type MaxHeadSize: Get<u32>;
}

#[derive(Encode, Decode, Clone, PartialEq, Eq)]
pub enum ParaState {
	Parachain,
	Parathread,
}

#[derive(Encode, Decode, Clone, PartialEq, Eq, Default)]
pub struct ParaData {
	genesis_head: HeadData,
	validation_code: ValidationCode,
}

#[derive(Encode, Decode, Clone, PartialEq, Eq, Default)]
pub struct ParaInfo<Account, Balance> {
	manager: Account,
	deposit: Balance,
	state: Option<ParaState>,
}

decl_storage! {
	trait Store for Module<T: Config> as Registrar {
		/// Amount held on deposit for each para and the original depositor.
		///
		/// The given account ID is responsible for registering the code and initial head data, but may only do
		/// so if it isn't yet registered. (After that, it's up to governance to do so.)
		pub Paras: map hasher(twox_64_concat) ParaId => Option<ParaInfo<T::AccountId, BalanceOf<T>>>;

		/// Onboarding data for a para.
		pub Onboarding: map hasher(twox_64_concat) ParaId => Option<ParaData>;
	}
}

decl_event! {
	pub enum Event<T> where
		AccountId = <T as frame_system::Config>::AccountId,
		ParaId = ParaId,
	{
		Claimed(ParaId, AccountId),
	}
}

decl_error! {
	pub enum Error for Module<T: Config> {
		/// The Id is not registered.
		NotRegistered,
		/// The Id given is already in use.
		InUse,
		/// The caller is not the owner of this Id.
		NotOwner,
		/// Invalid para code size.
		CodeTooLarge,
		/// Invalid para head data size.
		HeadDataTooLarge,
		/// The validation code provided doesn't start with the Wasm file magic string.
		DefinitelyNotWasm,
		/// No para data has been uploaded.
		DataNotUploaded,
		/// Parachain already exists.
		ParaAlreadyExists,
	}
}

decl_module! {
	pub struct Module<T: Config> for enum Call where origin: <T as frame_system::Config>::Origin {
		type Error = Error<T>;

		fn deposit_event() = default;

		#[weight = 0]
		fn claim(origin, id: ParaId) -> DispatchResult {
			let who = ensure_signed(origin)?;
			ensure!(!Paras::<T>::contains_key(id), Error::<T>::InUse);
			T::Currency::reserve(&who, T::ParaDeposit::get())?;
			let info = ParaInfo {
				manager: who.clone(),
				deposit: T::ParaDeposit::get(),
				state: None,
			};
			Paras::<T>::insert(id, info);
			Self::deposit_event(RawEvent::Claimed(id, who));
			Ok(())
		}

		#[weight = 0]
		fn unclaim(origin, id: ParaId) -> DispatchResult {
			// TODO...
			//   (Should only be possible when called from the parachain itself or Root.
			let id = ensure_parachain(<T as Config>::Origin::from(origin))?;
			//ensure_root(origin)?;
			if let Some(info) = Paras::<T>::take(&id) {
				T::Currency::unreserve(&info.manager, info.deposit);
			}
			Onboarding::remove(&id);
			Ok(())
		}

		#[weight = 0]
		fn add_onboarding_data(origin, id: ParaId, genesis_head: HeadData, validation_code: ValidationCode) {
			let who = ensure_signed(origin)?;
			let info = Paras::<T>::get(&id).ok_or(Error::<T>::NotRegistered)?;
			ensure!(who == info.manager, Error::<T>::NotOwner);
			ensure!(validation_code.0.len() <= T::MaxCodeSize::get() as usize, Error::<T>::CodeTooLarge);
			ensure!(genesis_head.0.len() <= T::MaxHeadSize::get() as usize, Error::<T>::HeadDataTooLarge);
			ensure!(validation_code.0.starts_with(WASM_MAGIC), Error::<T>::DefinitelyNotWasm);
			let para_data = ParaData { genesis_head, validation_code };
			Onboarding::insert(id, para_data);
		}
	}
}

impl<T: Config> slots::Registrar<T::AccountId> for Module<T> {
	fn max_head_size() -> u32 {
		T::MaxHeadSize::get()
	}

	fn max_code_size() -> u32 {
		T::MaxHeadSize::get()
	}

	// Make a registered para into a parachain.
	//
	// Can fail if the para id is not registered or onboarding data is not uploaded.
	fn make_parachain(id: ParaId) -> DispatchResult {
		Self::make_para(id, true)
	}

	fn make_parathread(id: ParaId) -> DispatchResult {
		//ensure!(ParathreadsRegistrationEnabled::get(), Error::<T>::ParathreadsRegistrationDisabled);
		Self::make_para(id, false)
	}

}

impl<T: Config> Module<T> {
	fn make_para(id: ParaId, parachain: bool) -> DispatchResult {
		let mut info = Paras::<T>::get(&id).ok_or(Error::<T>::NotRegistered)?;

		let outgoing = <paras::Module<T>>::outgoing_paras();
		ensure!(outgoing.binary_search(&id).is_err(), Error::<T>::ParaAlreadyExists);

		let data = Onboarding::take(id).ok_or(Error::<T>::DataNotUploaded)?;

		let genesis = ParaGenesisArgs {
			genesis_head: data.genesis_head,
			validation_code: data.validation_code,
			parachain,
		};

		runtime_parachains::schedule_para_initialize::<T>(id, genesis);

		if parachain {
			info.state = Some(ParaState::Parachain);
		} else {
			info.state = Some(ParaState::Parathread);
		}

		Paras::<T>::insert(&id, info);
		Ok(())
	}
}
