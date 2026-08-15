// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ffi::{CStr, c_void};
use std::ptr::NonNull;
use std::sync::OnceLock;

use libc::c_char;

use super::ffi;

/// Largest transfer a single RDMA descriptor can describe. The
/// `x-amz-rdma-token` carries the window size in a 32-bit field, so a buffer
/// past this cannot be named to the server at all and the caller must split
/// the transfer into parts (multipart upload / ranged read). `usize` is safe:
/// the `rdma` feature only builds for x86_64 and aarch64 (see build.rs), both
/// 64-bit.
pub const RDMA_MAX_MEMORY_REG_SIZE: usize = u32::MAX as usize;

/// What kind of memory backs a pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryType {
    System,
    CudaManaged,
    CudaDevice,
    Unknown,
}

impl MemoryType {
    fn from_raw(v: libc::c_int) -> Self {
        match v {
            ffi::S3RDMA_MEM_SYSTEM => Self::System,
            ffi::S3RDMA_MEM_CUDA_MANAGED => Self::CudaManaged,
            ffi::S3RDMA_MEM_CUDA_DEVICE => Self::CudaDevice,
            ffi::S3RDMA_MEM_UNKNOWN => Self::Unknown,
            _ => Self::Unknown,
        }
    }
}

/// Safe wrapper around a `libs3rdma` client context.
///
/// Owns the handle returned by `s3rdma_client_init`; `Drop` releases it. The
/// singleton accessor below shares one process-wide, which is what an
/// application wants: the handle owns an RDMA device context and a DCT, and
/// one per transfer would open a queue pair per transfer.
#[derive(Debug)]
pub struct RdmaClient {
    raw: NonNull<c_void>,
}

unsafe impl Send for RdmaClient {}
unsafe impl Sync for RdmaClient {}

impl RdmaClient {
    /// Open a client context on the first usable RDMA device, or on
    /// `$S3RDMA_DEVICE` when that names one. Returns `None` when the host has
    /// no usable device.
    pub fn new() -> Option<Self> {
        let raw = unsafe { ffi::s3rdma_client_init(std::ptr::null(), std::ptr::null_mut(), 0) };
        NonNull::new(raw).map(|raw| Self { raw })
    }

    /// Whether this client can mint tokens. True for any live client: DC is
    /// connectionless, so this reports a usable local device rather than a
    /// reachable server. A server that will not serve RDMA declines per
    /// request with `x-amz-rdma-reply: 501`.
    pub fn is_ready(&self) -> bool {
        unsafe { ffi::s3rdma_client_ready(self.raw.as_ptr()) != 0 }
    }

    /// # Safety
    /// `ptr` must point to a `size`-byte region valid for the duration of
    /// the RDMA op and not concurrently mutated.
    pub unsafe fn register(&self, ptr: *mut c_void, size: usize) -> bool {
        unsafe { ffi::s3rdma_client_register(self.raw.as_ptr(), ptr, size) == 0 }
    }

    /// # Safety
    /// `ptr` must match a previously-successful `register` call.
    pub unsafe fn deregister(&self, ptr: *mut c_void) -> bool {
        unsafe { ffi::s3rdma_client_deregister(self.raw.as_ptr(), ptr) == 0 }
    }

    /// Mint an RDMA token for a registered buffer. Returns `None` on failure.
    ///
    /// # Safety
    /// Caller must have a live registration for `ptr` (via `register`).
    pub unsafe fn get_rdma_token(
        &self,
        ptr: *mut c_void,
        size: usize,
        offset: usize,
    ) -> Option<RdmaToken> {
        let mut token: *mut c_char = std::ptr::null_mut();
        let rc = unsafe {
            ffi::s3rdma_client_get_token(self.raw.as_ptr(), ptr, size, offset, &mut token)
        };
        if rc != 0 || token.is_null() {
            return None;
        }
        Some(RdmaToken { ptr: token })
    }

    /// Classify `ptr` as host or CUDA memory. Resolved inside libs3rdma
    /// through `dlopen("libcuda.so.1")`, so neither the SDK nor the library
    /// links CUDA; a host without the driver reports every pointer as
    /// [`MemoryType::System`].
    ///
    /// # Safety
    /// `ptr` is passed to CUDA's pointer-classification routine, which may
    /// inspect the address against the driver's mapping tables.
    pub unsafe fn memory_type(ptr: *const c_void) -> MemoryType {
        MemoryType::from_raw(unsafe { ffi::s3rdma_client_memory_type(ptr) })
    }
}

impl Drop for RdmaClient {
    fn drop(&mut self) {
        unsafe { ffi::s3rdma_client_free(self.raw.as_ptr()) };
    }
}

/// RAII wrapper for a minted RDMA token, released to libs3rdma on drop.
pub struct RdmaToken {
    ptr: *mut c_char,
}

impl RdmaToken {
    pub fn as_cstr(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.ptr) }
    }
}

impl Drop for RdmaToken {
    fn drop(&mut self) {
        unsafe { ffi::s3rdma_client_free_token(self.ptr) };
    }
}

/// Process-wide shared [`RdmaClient`]. One device context and DCT per process
/// is the supported pattern: the handle is `Sync`, and every concurrent
/// transfer mints its own token against it.
pub fn shared() -> Option<&'static RdmaClient> {
    static INSTANCE: OnceLock<Option<RdmaClient>> = OnceLock::new();
    INSTANCE.get_or_init(RdmaClient::new).as_ref()
}

/// RAII buffer registration. Holds the memory pinned for RDMA until dropped.
pub struct ScopedRegistration<'a> {
    client: &'a RdmaClient,
    ptr: *mut c_void,
    released: bool,
}

impl<'a> ScopedRegistration<'a> {
    /// # Safety
    /// `ptr` + `size` must describe a region valid for the registration's lifetime.
    pub unsafe fn register(client: &'a RdmaClient, ptr: *mut c_void, size: usize) -> Option<Self> {
        if unsafe { client.register(ptr, size) } {
            Some(Self {
                client,
                ptr,
                released: false,
            })
        } else {
            None
        }
    }

    pub fn release(mut self) {
        if !self.released {
            unsafe { self.client.deregister(self.ptr) };
            self.released = true;
        }
    }
}

impl<'a> Drop for ScopedRegistration<'a> {
    fn drop(&mut self) {
        if !self.released {
            unsafe { self.client.deregister(self.ptr) };
        }
    }
}
