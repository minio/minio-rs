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
//
// Declarations for the client half of `libs3rdma.so`. See
// `vendor/s3rdma/include/s3rdma.h` for the contract these mirror.

use libc::{c_char, c_int, c_void, size_t};

pub const S3RDMA_MEM_SYSTEM: c_int = 0;
pub const S3RDMA_MEM_CUDA_MANAGED: c_int = 1;
pub const S3RDMA_MEM_CUDA_DEVICE: c_int = 2;
pub const S3RDMA_MEM_UNKNOWN: c_int = 3;

unsafe extern "C" {
    pub fn s3rdma_client_init(
        device: *const c_char,
        err_buf: *mut c_char,
        err_buf_len: size_t,
    ) -> *mut c_void;
    pub fn s3rdma_client_free(handle: *mut c_void);
    pub fn s3rdma_client_ready(handle: *mut c_void) -> c_int;
    pub fn s3rdma_client_register(handle: *mut c_void, ptr: *mut c_void, size: size_t) -> c_int;
    pub fn s3rdma_client_deregister(handle: *mut c_void, ptr: *mut c_void) -> c_int;
    pub fn s3rdma_client_get_token(
        handle: *mut c_void,
        ptr: *mut c_void,
        size: size_t,
        offset: size_t,
        token_out: *mut *mut c_char,
    ) -> c_int;
    pub fn s3rdma_client_free_token(token: *mut c_char);
    pub fn s3rdma_client_memory_type(ptr: *const c_void) -> c_int;
    pub fn s3rdma_client_nic_count(handle: *mut c_void) -> c_int;
    pub fn s3rdma_client_healthy_nic_count(handle: *mut c_void) -> c_int;
    pub fn s3rdma_client_report_token_failure(handle: *mut c_void, token: *const c_char) -> c_int;
}
