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

use libc::c_void;

/// Raw RDMA-registrable buffer. Mirrors the `void* buf, size_t size` pair the
/// C++ API uses: host (page-aligned `posix_memalign`) or GPU (`cudaMalloc` /
/// `cuMemAlloc`) memory. The owning allocation lives in the caller.
#[derive(Debug, Clone, Copy)]
pub struct RdmaBuffer {
    ptr: *mut c_void,
    len: usize,
}

unsafe impl Send for RdmaBuffer {}
unsafe impl Sync for RdmaBuffer {}

impl RdmaBuffer {
    /// # Safety
    /// Caller guarantees `ptr` is valid for `len` bytes for the duration of
    /// the RDMA operation, and is either page-aligned host memory or a
    /// GPU device pointer (CUdeviceptr cast).
    pub const unsafe fn from_raw(ptr: *mut c_void, len: usize) -> Self {
        Self { ptr, len }
    }

    pub const fn ptr(&self) -> *mut c_void {
        self.ptr
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}
