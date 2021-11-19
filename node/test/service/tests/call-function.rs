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

use polkadot_test_service::*;
use sp_keyring::Sr25519Keyring::{Alice, Bob};

#[substrate_test_utils::test]
async fn call_function_actually_works() {
	let alice =
		run_validator_node(tokio::runtime::Handle::current(), Alice, || {}, Vec::new(), None);

	let transfer_call = polkadot_test_runtime::Call::Balances(pallet_balances::Call::transfer {
		dest: Default::default(),
		value: 1,
	});
	let result = alice.send_extrinsic(transfer_call, Bob).await.expect("return value expected");
	assert!(result.starts_with("0x"), "result starts with 0x");
	alice.task_manager.clean_shutdown().await;
}
