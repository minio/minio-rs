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

/// What kind of memory backs a pointer, as classified by libcuobjclient.
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
            ffi::MINIORS_CUOBJ_MEM_SYSTEM => Self::System,
            ffi::MINIORS_CUOBJ_MEM_CUDA_MANAGED => Self::CudaManaged,
            ffi::MINIORS_CUOBJ_MEM_CUDA_DEVICE => Self::CudaDevice,
            ffi::MINIORS_CUOBJ_MEM_UNKNOWN => Self::Unknown,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OpType {
    Get,
    Put,
}

impl OpType {
    fn as_raw(self) -> libc::c_int {
        match self {
            Self::Get => ffi::MINIORS_CUOBJ_OP_GET,
            Self::Put => ffi::MINIORS_CUOBJ_OP_PUT,
        }
    }
}

/// Safe wrapper around the NVIDIA `cuObjClient` C++ class.
///
/// Owns a heap-allocated `cuObjClient` instance; `Drop` releases it. Safe
/// to clone via `Arc`; the singleton accessor below shares one process-wide.
#[derive(Debug)]
pub struct CuObjClient {
    raw: NonNull<ffi::miniors_cuobj_client>,
}

unsafe impl Send for CuObjClient {}
unsafe impl Sync for CuObjClient {}

impl CuObjClient {
    pub fn new() -> Option<Self> {
        let raw = unsafe { ffi::miniors_cuobj_client_new() };
        NonNull::new(raw).map(|raw| Self { raw })
    }

    pub fn is_connected(&self) -> bool {
        unsafe { ffi::miniors_cuobj_is_connected(self.raw.as_ptr()) != 0 }
    }

    /// # Safety
    /// `ptr` must point to a `size`-byte region valid for the duration of
    /// the RDMA op and not concurrently mutated.
    pub unsafe fn get_descriptor(&self, ptr: *mut c_void, size: usize) -> bool {
        unsafe {
            ffi::miniors_cuobj_get_descriptor(self.raw.as_ptr(), ptr, size)
                == ffi::MINIORS_CUOBJ_SUCCESS
        }
    }

    /// # Safety
    /// `ptr` must match a previously-successful `get_descriptor` call.
    pub unsafe fn put_descriptor(&self, ptr: *mut c_void) -> bool {
        unsafe {
            ffi::miniors_cuobj_put_descriptor(self.raw.as_ptr(), ptr) == ffi::MINIORS_CUOBJ_SUCCESS
        }
    }

    /// Mint an RDMA token for a registered buffer. Returns `None` on failure.
    ///
    /// # Safety
    /// Caller must have a live descriptor for `ptr` (via `get_descriptor`).
    pub unsafe fn get_rdma_token(
        &self,
        ptr: *mut c_void,
        size: usize,
        offset: usize,
        op: OpType,
    ) -> Option<RdmaToken<'_>> {
        let mut token: *mut c_char = std::ptr::null_mut();
        let rc = unsafe {
            ffi::miniors_cuobj_get_rdma_token(
                self.raw.as_ptr(),
                ptr,
                size,
                offset,
                op.as_raw(),
                &mut token,
            )
        };
        if rc != ffi::MINIORS_CUOBJ_SUCCESS || token.is_null() {
            return None;
        }
        Some(RdmaToken {
            client: self,
            ptr: token,
        })
    }

    /// # Safety
    /// `ptr` is passed to NVIDIA's pointer-classification routine, which may
    /// inspect the address against CUDA's GPU mapping tables.
    pub unsafe fn memory_type(ptr: *const c_void) -> MemoryType {
        MemoryType::from_raw(unsafe { ffi::miniors_cuobj_memory_type(ptr) })
    }
}

impl Drop for CuObjClient {
    fn drop(&mut self) {
        unsafe { ffi::miniors_cuobj_client_free(self.raw.as_ptr()) };
    }
}

/// RAII wrapper for a minted RDMA token. Releases via cuObjClient on drop.
pub struct RdmaToken<'a> {
    client: &'a CuObjClient,
    ptr: *mut c_char,
}

impl<'a> RdmaToken<'a> {
    pub fn as_cstr(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.ptr) }
    }
}

impl<'a> Drop for RdmaToken<'a> {
    fn drop(&mut self) {
        unsafe {
            ffi::miniors_cuobj_put_rdma_token(self.client.raw.as_ptr(), self.ptr);
        }
    }
}

/// Process-wide shared `cuObjClient`. Mirrors minio-cpp's `SharedRDMAClient()`
/// Meyers singleton: constructing per-call was racy and corrupted malloc state
/// under concurrent workers. One instance per process is the supported pattern.
pub fn shared() -> Option<&'static CuObjClient> {
    static INSTANCE: OnceLock<Option<CuObjClient>> = OnceLock::new();
    INSTANCE.get_or_init(CuObjClient::new).as_ref()
}

/// RAII descriptor registration. Holds a live `cuMemObjGetDescriptor` until
/// dropped, then releases via `cuMemObjPutDescriptor`.
pub struct ScopedRegistration<'a> {
    client: &'a CuObjClient,
    ptr: *mut c_void,
    released: bool,
}

impl<'a> ScopedRegistration<'a> {
    /// # Safety
    /// `ptr` + `size` must describe a region valid for the registration's lifetime.
    pub unsafe fn register(client: &'a CuObjClient, ptr: *mut c_void, size: usize) -> Option<Self> {
        if unsafe { client.get_descriptor(ptr, size) } {
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
            unsafe { self.client.put_descriptor(self.ptr) };
            self.released = true;
        }
    }
}

impl<'a> Drop for ScopedRegistration<'a> {
    fn drop(&mut self) {
        if !self.released {
            unsafe { self.client.put_descriptor(self.ptr) };
        }
    }
}
