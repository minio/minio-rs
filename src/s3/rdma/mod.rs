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

//! RDMA + NVIDIA GPU Direct Storage data path via NVIDIA `libcuobjclient`.
//!
//! The HTTP control plane carries an `x-amz-rdma-token` header, the actual
//! payload moves out-of-band over RDMA (host memory or GPU device memory) to
//! a cuObjServer-aware MinIO endpoint. On a 501 reply the server is declining
//! RDMA for this object and the caller should retry on the HTTP fast path
//! ([`MinioClient::put_object`](crate::s3::client::MinioClient::put_object) /
//! [`MinioClient::get_object`](crate::s3::client::MinioClient::get_object)).
//!
//! GPU memory is allocated by the application; this crate does not link CUDA.

mod buffer;
mod client;
mod cuobj;
mod ffi;
mod protocol;

pub use buffer::RdmaBuffer;
pub use client::{RdmaError, RdmaMultipartResponse, RdmaPart, RdmaResponse, crc64nvme_base64};
pub use cuobj::{
    CuObjClient, MemoryType, OpType, ScopedRegistration, shared as shared_cuobj_client,
};
pub use protocol::{
    RDMA_NOT_SUPPORTED, RDMA_REPLY_NOT_IMPLEMENTED, RdmaOutcome, S3RdmaClientCtx,
    parse_client_nic_from_token, parse_rdma_reply, rdma_get, rdma_get_with_retry, rdma_put,
    rdma_put_with_retry,
};
