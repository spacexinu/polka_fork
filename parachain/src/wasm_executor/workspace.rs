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

use crate::primitives::{ValidationParams, ValidationResult};
use parity_scale_codec::{Decode, Encode};
use raw_sync::{
	events::{Event, EventImpl, EventInit, EventState},
	Timeout,
};
use shared_memory::{Shmem, ShmemConf};
use std::{
	error::Error,
	fmt,
	io::{Cursor, Write},
	slice,
	time::Duration,
};

// maximum memory in bytes
const MAX_RUNTIME_MEM: usize = 1024 * 1024 * 1024; // 1 GiB
const MAX_CODE_MEM: usize = 16 * 1024 * 1024; // 16 MiB
const MAX_VALIDATION_RESULT_HEADER_MEM: usize = MAX_CODE_MEM + 1024; // 16.001 MiB

/// Params header in shared memory. All offsets should be aligned to WASM page size.
#[derive(Encode, Decode, Debug)]
struct ValidationHeader {
	code_size: u64,
	params_size: u64,
}

#[derive(Encode, Decode, Debug)]
pub enum WorkerValidationError {
	InternalError(String),
	ValidationError(String),
}

#[derive(Encode, Decode, Debug)]
pub enum ValidationResultHeader {
	Ok(ValidationResult),
	Error(WorkerValidationError),
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
enum Mode {
	Initialize,
	Attach,
}

fn stringify_err(err: Box<dyn Error>) -> String {
	format!("{:?}", err)
}

struct Inner {
	shmem: Shmem,
	candidate_ready_ev: Box<dyn EventImpl>,
	result_ready_ev: Box<dyn EventImpl>,
	worker_ready_ev: Box<dyn EventImpl>,

	/// The number of bytes reserved by the auxilary stuff like events from the beginning of the
	/// shared memory area.
	consumed: usize,
}

impl Inner {
	fn layout(shmem: Shmem, mode: Mode) -> Self {
		unsafe {
			let base_ptr = shmem.as_ptr();
			let mut consumed = 0;

			let candidate_ready_ev = add_event(base_ptr, &mut consumed, mode);
			let result_ready_ev = add_event(base_ptr, &mut consumed, mode);
			let worker_ready_ev = add_event(base_ptr, &mut consumed, mode);

			let consumed = align_up_to(consumed, 16);

			Self {
				shmem,
				consumed,
				candidate_ready_ev,
				result_ready_ev,
				worker_ready_ev,
			}
		}
	}

	fn as_slice(&self) -> &[u8] {
		unsafe {
			let base_ptr = self.shmem.as_ptr().add(self.consumed);
			let residue = self.shmem.len() - self.consumed;
			slice::from_raw_parts(base_ptr, residue)
		}
	}

	fn as_slice_mut(&mut self) -> &mut [u8] {
		unsafe {
			let base_ptr = self.shmem.as_ptr().add(self.consumed);
			let residue = self.shmem.len() - self.consumed;
			slice::from_raw_parts_mut(base_ptr, residue)
		}
	}
}

fn align_up_to(v: usize, alignment: usize) -> usize {
	(v + alignment - 1) & !(alignment - 1)
}

unsafe fn add_event(base_ptr: *mut u8, consumed: &mut usize, mode: Mode) -> Box<dyn EventImpl> {
	// SAFETY: there is no safety proof since the documentation doesn't specify the particular constraints
	//         besides requiring the pointer to be valid. AFAICT, the pointer is valid.
	use std::convert::TryFrom as _;
	let offset =
		isize::try_from(*consumed).expect("we only use small offsets within this module; qed");
	let ptr = base_ptr.offset(offset);
	let (ev, used_bytes) = match mode {
		Mode::Initialize => Event::new(ptr, true).unwrap(),
		Mode::Attach => Event::from_existing(ptr).unwrap(),
	};
	*consumed += used_bytes;
	ev
}

pub struct WorkItem<'handle> {
	pub params: &'handle [u8],
	pub code: &'handle [u8],
}

pub enum WaitForWorkErr {
	Wait(String),
	FailedToDecode(String),
}

#[derive(Debug)]
pub enum ReportResultErr {
	Signal(String),
}

pub struct WorkerHandle {
	inner: Inner,
}

impl WorkerHandle {
	pub fn signal_ready(&self) -> Result<(), String> {
		self.inner
			.worker_ready_ev
			.set(EventState::Signaled)
			.map_err(stringify_err)?;
		Ok(())
	}

	pub fn wait_for_work(&mut self, timeout_secs: u64) -> Result<WorkItem, WaitForWorkErr> {
		self.inner
			.candidate_ready_ev
			.wait(Timeout::Val(Duration::from_secs(timeout_secs)))
			.map_err(stringify_err)
			.map_err(WaitForWorkErr::Wait)?;

		let mut cur = self.inner.as_slice();
		let header = ValidationHeader::decode(&mut cur)
			.map_err(|e| format!("{:?}", e))
			.map_err(WaitForWorkErr::FailedToDecode)?;

		let (params, cur) = cur.split_at(header.params_size as usize);
		let (code, _) = cur.split_at(header.code_size as usize);

		Ok(WorkItem { params, code })
	}

	pub fn report_result(&mut self, result: ValidationResultHeader) -> Result<(), ReportResultErr> {
		let mut cur = self.inner.as_slice_mut();
		result.encode_to(&mut cur);
		self.inner
			.result_ready_ev
			.set(EventState::Signaled)
			.map_err(stringify_err)
			.map_err(ReportResultErr::Signal)?;

		Ok(())
	}
}

#[derive(Debug)]
pub enum WaitUntilReadyErr {
	Wait(String),
}

#[derive(Debug)]
pub enum RequestValidationErr {
	CodeTooLarge { actual: usize, max: usize },
	ParamsTooLarge { actual: usize, max: usize },
	WriteData(&'static str),
	Signal(String),
}

#[derive(Debug)]
pub enum WaitForResultErr {
	Wait(String),
	HeaderDecodeErr(String),
}

pub struct HostHandle {
	inner: Inner,
}

impl fmt::Debug for HostHandle {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "HostHandle")
	}
}

impl HostHandle {
	pub fn id(&self) -> &str {
		self.inner.shmem.get_os_id()
	}

	pub fn wait_until_ready(&self, timeout_secs: u64) -> Result<(), WaitUntilReadyErr> {
		self.inner
			.worker_ready_ev
			.wait(Timeout::Val(Duration::from_secs(timeout_secs)))
			.map_err(stringify_err)
			.map_err(WaitUntilReadyErr::Wait)?;
		Ok(())
	}

	pub fn request_validation(
		&mut self,
		code: &[u8],
		params: ValidationParams,
	) -> Result<(), RequestValidationErr> {
		if code.len() > MAX_CODE_MEM {
			return Err(RequestValidationErr::CodeTooLarge {
				actual: code.len(),
				max: MAX_CODE_MEM,
			});
		}

		let params = params.encode();
		if params.len() > MAX_RUNTIME_MEM {
			return Err(RequestValidationErr::ParamsTooLarge {
				actual: params.len(),
				max: MAX_RUNTIME_MEM,
			});
		}

		let mut cur = Cursor::new(self.inner.as_slice_mut());
		ValidationHeader {
			code_size: code.len() as u64,
			params_size: params.len() as u64,
		}
		.encode_to(&mut cur);
		cur.write_all(&params)
			.map_err(|_| RequestValidationErr::WriteData("params"))?;
		cur.write_all(code)
			.map_err(|_| RequestValidationErr::WriteData("code"))?;

		self.inner
			.candidate_ready_ev
			.set(EventState::Signaled)
			.map_err(stringify_err)
			.map_err(RequestValidationErr::Signal)?;

		Ok(())
	}

	pub fn wait_for_result(
		&self,
		execution_timeout: u64,
	) -> Result<ValidationResultHeader, WaitForResultErr> {
		self.inner
			.result_ready_ev
			.wait(Timeout::Val(Duration::from_secs(execution_timeout)))
			.map_err(|e| WaitForResultErr::Wait(format!("{:?}", e)))?;

		let mut cur = self.inner.as_slice();
		let header = ValidationResultHeader::decode(&mut cur)
			.map_err(|e| WaitForResultErr::HeaderDecodeErr(format!("{:?}", e)))?;
		Ok(header)
	}
}

pub fn create() -> Result<HostHandle, String> {
	let mem_size = MAX_RUNTIME_MEM + MAX_CODE_MEM + MAX_VALIDATION_RESULT_HEADER_MEM;
	let shmem = ShmemConf::new()
		.size(mem_size)
		.create()
		.map_err(|e| format!("Error creating shared memory: {:?}", e))?;

	Ok(HostHandle {
		inner: Inner::layout(shmem, Mode::Initialize),
	})
}

pub fn open(id: &str) -> Result<WorkerHandle, String> {
	let shmem = ShmemConf::new()
		.os_id(id)
		.open()
		.map_err(|e| format!("Error opening shared memory: {:?}", e))?;

	Ok(WorkerHandle {
		inner: Inner::layout(shmem, Mode::Attach),
	})
}
