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

//! RDMA data path via `libs3rdma`.
//!
//! The HTTP control plane carries an `x-amz-rdma-token` header, the actual
//! payload moves out-of-band over RDMA (host memory or GPU device memory) to
//! an RDMA-aware MinIO endpoint. On a 501 reply the server is declining
//! RDMA for this object and the caller should retry on the HTTP fast path
//! ([`MinioClient::put_object`](crate::s3::client::MinioClient::put_object) /
//! [`MinioClient::get_object`](crate::s3::client::MinioClient::get_object)).
//!
//! The transport is `libs3rdma` (vendored under `vendor/s3rdma/`): plain IBTA
//! verbs over RoCE or native InfiniBand, into any memory `ibv_reg_mr` accepts —
//! host RAM, hugepages, mmap, GPU device memory. The application allocates the
//! buffer; RDMA reaches device memory the same way it reaches host memory, and
//! the SDK links no CUDA.
//!
//! It mints DC tokens, so it needs a DC-capable HCA (mlx5, ConnectX-4 and
//! later). On a device without DC,
//! [`MinioClient::rdma_available`](crate::s3::client::MinioClient::rdma_available)
//! is false and every transfer takes the ordinary HTTP path.

mod buffer;
mod client;
mod ffi;
mod protocol;
mod transport;

pub use buffer::RdmaBuffer;
pub use client::{RdmaError, RdmaMultipartResponse, RdmaPart, RdmaResponse, crc64nvme_base64};
pub use protocol::{
    RDMA_NOT_SUPPORTED, RDMA_REPLY_NOT_IMPLEMENTED, RdmaOutcome, S3RdmaClientCtx,
    parse_client_nic_from_token, parse_rdma_reply, rdma_get, rdma_get_with_retry, rdma_put,
    rdma_put_with_retry,
};
pub use transport::{
    MemoryType, RdmaClient, RdmaToken, ScopedRegistration, init_error as rdma_init_error,
    shared as shared_rdma_client,
};
