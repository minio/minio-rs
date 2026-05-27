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

#![allow(non_camel_case_types)]

use libc::{c_char, c_int, c_void, size_t};

#[repr(C)]
pub struct miniors_cuobj_client {
    _private: [u8; 0],
}

pub const MINIORS_CUOBJ_SUCCESS: c_int = 0;

pub const MINIORS_CUOBJ_OP_GET: c_int = 0;
pub const MINIORS_CUOBJ_OP_PUT: c_int = 1;

pub const MINIORS_CUOBJ_MEM_SYSTEM: c_int = 0;
pub const MINIORS_CUOBJ_MEM_CUDA_MANAGED: c_int = 1;
pub const MINIORS_CUOBJ_MEM_CUDA_DEVICE: c_int = 2;
pub const MINIORS_CUOBJ_MEM_UNKNOWN: c_int = 3;

unsafe extern "C" {
    pub fn miniors_cuobj_client_new() -> *mut miniors_cuobj_client;
    pub fn miniors_cuobj_client_free(client: *mut miniors_cuobj_client);
    pub fn miniors_cuobj_is_connected(client: *mut miniors_cuobj_client) -> c_int;
    pub fn miniors_cuobj_get_descriptor(
        client: *mut miniors_cuobj_client,
        ptr: *mut c_void,
        size: size_t,
    ) -> c_int;
    pub fn miniors_cuobj_put_descriptor(
        client: *mut miniors_cuobj_client,
        ptr: *mut c_void,
    ) -> c_int;
    pub fn miniors_cuobj_get_rdma_token(
        client: *mut miniors_cuobj_client,
        ptr: *mut c_void,
        size: size_t,
        offset: size_t,
        op: c_int,
        token_out: *mut *mut c_char,
    ) -> c_int;
    pub fn miniors_cuobj_put_rdma_token(
        client: *mut miniors_cuobj_client,
        token: *mut c_char,
    ) -> c_int;
    pub fn miniors_cuobj_memory_type(ptr: *const c_void) -> c_int;
}
