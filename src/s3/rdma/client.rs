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

use crate::s3::client::{DEFAULT_REGION, MinioClient};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::response_traits::HasEtagFromHeaders;
use crate::s3::types::{BucketName, ETag, ObjectKey, PartInfo, Region, S3Api, UploadId};
use crate::s3::utils::ChecksumAlgorithm;

use super::buffer::RdmaBuffer;
use super::cuobj::{ScopedRegistration, shared};
use super::protocol::{RdmaOutcome, S3RdmaClientCtx, rdma_get_with_retry, rdma_put_with_retry};

/// Successful RDMA transfer result.
#[derive(Debug, Clone)]
pub struct RdmaResponse {
    pub etag: String,
    pub bytes_transferred: usize,
    pub checksum_crc64nvme: Option<String>,
}

/// Errors specific to the RDMA fast path.
#[derive(Debug, thiserror::Error)]
pub enum RdmaError {
    #[error("RDMA not available: cuObjClient not connected (no cuObjServer reachable)")]
    NotConnected,
    #[error("RDMA buffer registration failed (cuMemObjGetDescriptor returned failure)")]
    RegistrationFailed,
    #[error("server declined RDMA (x-amz-rdma-reply: 501); fall back to HTTP PutObject/GetObject")]
    Declined,
    #[error("RDMA transfer failed after retries; fall back to HTTP PutObject/GetObject")]
    TransferFailed,
    #[error("multipart upload requires at least one part")]
    EmptyParts,
    #[error("multipart part number {0} out of range (1..=10000)")]
    InvalidPartNumber(u32),
    #[error(transparent)]
    Validation(#[from] ValidationErr),
    #[error("S3 control-plane call failed: {0}")]
    Control(String),
}

impl From<Error> for RdmaError {
    fn from(e: Error) -> Self {
        Self::Control(e.to_string())
    }
}

/// One part of an [`MinioClient::rdma_put_object_multipart`] upload.
///
/// The server requires a per-part CRC64NVME checksum on RDMA UploadPart
/// requests; supply it base64-encoded. The SDK does not compute it for you
/// because GPU buffers are not CPU-readable — callers using host memory can
/// use [`crc64nvme_base64`] for the common case.
#[derive(Debug, Clone)]
pub struct RdmaPart {
    pub buffer: RdmaBuffer,
    pub checksum_crc64nvme: Option<String>,
}

impl RdmaPart {
    pub const fn new(buffer: RdmaBuffer) -> Self {
        Self {
            buffer,
            checksum_crc64nvme: None,
        }
    }

    pub fn with_checksum(buffer: RdmaBuffer, checksum_crc64nvme: String) -> Self {
        Self {
            buffer,
            checksum_crc64nvme: Some(checksum_crc64nvme),
        }
    }
}

/// Compute the base64-encoded CRC64NVME of a host-memory slice, the format
/// the server expects in `x-amz-checksum-crc64nvme`.
pub fn crc64nvme_base64(data: &[u8]) -> String {
    use base64::Engine as _;
    let crc = crate::s3::utils::crc64nvme(data);
    base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
}

/// Result of [`MinioClient::rdma_put_object_multipart`].
#[derive(Debug, Clone)]
pub struct RdmaMultipartResponse {
    pub etag: String,
    pub upload_id: String,
    pub total_bytes_transferred: usize,
    pub parts: Vec<PartInfo>,
}

impl MinioClient {
    /// Upload `buffer` to `bucket`/`object` over RDMA via cuObjClient.
    ///
    /// The buffer is registered once (mirrors C++ `ScopedRDMARegistration`),
    /// the RDMA token is minted per attempt with one NIC-failover retry, and
    /// the HTTP control plane carries the token to the server. On a 501
    /// "RDMA not supported" reply, returns [`RdmaError::Declined`] so the
    /// caller can fall back to [`MinioClient::put_object`].
    ///
    /// # Safety
    /// `buffer` must wrap a live allocation valid for the call duration.
    pub async fn rdma_put_object<B, O>(
        &self,
        bucket: B,
        object: O,
        buffer: RdmaBuffer,
    ) -> Result<RdmaResponse, RdmaError>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        O: TryInto<ObjectKey>,
        O::Error: Into<ValidationErr>,
    {
        let bucket: BucketName = bucket.try_into().map_err(Into::into)?;
        let object: ObjectKey = object.try_into().map_err(Into::into)?;
        let rdma = shared().ok_or(RdmaError::NotConnected)?;
        if !rdma.is_connected() {
            return Err(RdmaError::NotConnected);
        }

        let _reg = unsafe { ScopedRegistration::register(rdma, buffer.ptr(), buffer.len()) }
            .ok_or(RdmaError::RegistrationFailed)?;

        let mut ctx = S3RdmaClientCtx {
            bucket,
            object,
            region: self.resolve_rdma_region(),
            upload_id: None,
            part_number: 0,
            checksum_crc64nvme: None,
            etag: String::new(),
        };

        match rdma_put_with_retry(rdma, self, &mut ctx, buffer.ptr(), buffer.len()).await {
            RdmaOutcome::Ok(n) => Ok(RdmaResponse {
                etag: ctx.etag,
                bytes_transferred: n,
                checksum_crc64nvme: ctx.checksum_crc64nvme,
            }),
            RdmaOutcome::Declined => Err(RdmaError::Declined),
            RdmaOutcome::Failed => Err(RdmaError::TransferFailed),
        }
    }

    /// Download `bucket`/`object` directly into `buffer` over RDMA.
    ///
    /// # Safety
    /// `buffer` must wrap a live allocation valid for the call duration. The
    /// transferred byte count (which may be less than `buffer.len()` for ranged
    /// reads) is reported in the returned [`RdmaResponse`].
    pub async fn rdma_get_object<B, O>(
        &self,
        bucket: B,
        object: O,
        buffer: RdmaBuffer,
    ) -> Result<RdmaResponse, RdmaError>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        O: TryInto<ObjectKey>,
        O::Error: Into<ValidationErr>,
    {
        let bucket: BucketName = bucket.try_into().map_err(Into::into)?;
        let object: ObjectKey = object.try_into().map_err(Into::into)?;
        let rdma = shared().ok_or(RdmaError::NotConnected)?;
        if !rdma.is_connected() {
            return Err(RdmaError::NotConnected);
        }

        let _reg = unsafe { ScopedRegistration::register(rdma, buffer.ptr(), buffer.len()) }
            .ok_or(RdmaError::RegistrationFailed)?;

        let mut ctx = S3RdmaClientCtx {
            bucket,
            object,
            region: self.resolve_rdma_region(),
            upload_id: None,
            part_number: 0,
            checksum_crc64nvme: None,
            etag: String::new(),
        };

        match rdma_get_with_retry(rdma, self, &mut ctx, buffer.ptr(), buffer.len()).await {
            RdmaOutcome::Ok(n) => Ok(RdmaResponse {
                etag: ctx.etag,
                bytes_transferred: n,
                checksum_crc64nvme: ctx.checksum_crc64nvme,
            }),
            RdmaOutcome::Declined => Err(RdmaError::Declined),
            RdmaOutcome::Failed => Err(RdmaError::TransferFailed),
        }
    }

    /// Upload a single multipart part over RDMA. Mirrors C++
    /// `BaseClient::UploadPart` RDMA path in src/baseclient.cc — registers the
    /// buffer, mints a token, and dispatches with `uploadId` + `partNumber`
    /// set on the context. The caller drives `create_multipart_upload` and
    /// `complete_multipart_upload` separately (or uses the high-level
    /// [`MinioClient::rdma_put_object_multipart`]).
    pub async fn rdma_upload_part<B, O, U>(
        &self,
        bucket: B,
        object: O,
        upload_id: U,
        part_number: u16,
        buffer: RdmaBuffer,
        checksum_crc64nvme: Option<String>,
    ) -> Result<RdmaResponse, RdmaError>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        O: TryInto<ObjectKey>,
        O::Error: Into<ValidationErr>,
        U: TryInto<UploadId>,
        U::Error: Into<ValidationErr>,
    {
        if part_number == 0 || part_number > 10000 {
            return Err(RdmaError::InvalidPartNumber(part_number as u32));
        }

        let bucket: BucketName = bucket.try_into().map_err(Into::into)?;
        let object: ObjectKey = object.try_into().map_err(Into::into)?;
        let upload_id: UploadId = upload_id.try_into().map_err(Into::into)?;
        let rdma = shared().ok_or(RdmaError::NotConnected)?;
        if !rdma.is_connected() {
            return Err(RdmaError::NotConnected);
        }

        let _reg = unsafe { ScopedRegistration::register(rdma, buffer.ptr(), buffer.len()) }
            .ok_or(RdmaError::RegistrationFailed)?;

        let mut ctx = S3RdmaClientCtx {
            bucket,
            object,
            region: self.resolve_rdma_region(),
            upload_id: Some(upload_id.into_inner()),
            part_number: part_number as u32,
            checksum_crc64nvme,
            etag: String::new(),
        };

        match rdma_put_with_retry(rdma, self, &mut ctx, buffer.ptr(), buffer.len()).await {
            RdmaOutcome::Ok(n) => Ok(RdmaResponse {
                etag: ctx.etag,
                bytes_transferred: n,
                checksum_crc64nvme: ctx.checksum_crc64nvme,
            }),
            RdmaOutcome::Declined => Err(RdmaError::Declined),
            RdmaOutcome::Failed => Err(RdmaError::TransferFailed),
        }
    }

    /// High-level multipart RDMA upload: create → upload each part over RDMA →
    /// complete. Aborts the upload on any per-part failure (best-effort) and
    /// returns the original error. Parts are uploaded sequentially in input
    /// order; the corresponding `partNumber` is 1-indexed.
    pub async fn rdma_put_object_multipart<B, O>(
        &self,
        bucket: B,
        object: O,
        parts: &[RdmaPart],
    ) -> Result<RdmaMultipartResponse, RdmaError>
    where
        B: TryInto<BucketName> + Clone,
        B::Error: Into<ValidationErr>,
        O: TryInto<ObjectKey> + Clone,
        O::Error: Into<ValidationErr>,
    {
        if parts.is_empty() {
            return Err(RdmaError::EmptyParts);
        }
        if parts.len() > 10000 {
            return Err(RdmaError::InvalidPartNumber(parts.len() as u32));
        }

        let bucket: BucketName = bucket.clone().try_into().map_err(Into::into)?;
        let object: ObjectKey = object.clone().try_into().map_err(Into::into)?;

        if !self.rdma_available() {
            return Err(RdmaError::NotConnected);
        }

        let create_resp = self
            .create_multipart_upload(bucket.clone(), object.clone())?
            .checksum_algorithm(Some(ChecksumAlgorithm::CRC64NVME))
            .build()
            .send()
            .await?;
        let upload_id = create_resp.upload_id().await?;

        let mut part_infos: Vec<PartInfo> = Vec::with_capacity(parts.len());
        let mut total: usize = 0;
        let mut upload_err: Option<RdmaError> = None;

        for (i, part) in parts.iter().enumerate() {
            let part_number = (i + 1) as u16;
            match self
                .rdma_upload_part(
                    bucket.clone(),
                    object.clone(),
                    upload_id.clone(),
                    part_number,
                    part.buffer,
                    part.checksum_crc64nvme.clone(),
                )
                .await
            {
                Ok(resp) => {
                    total += resp.bytes_transferred;
                    let etag = match ETag::new(resp.etag) {
                        Ok(e) => e,
                        Err(e) => {
                            upload_err = Some(e.into());
                            break;
                        }
                    };
                    let checksum = resp
                        .checksum_crc64nvme
                        .map(|v| (ChecksumAlgorithm::CRC64NVME, v));
                    part_infos.push(PartInfo::new(
                        part_number,
                        etag,
                        part.buffer.len() as u64,
                        checksum,
                    ));
                }
                Err(e) => {
                    upload_err = Some(e);
                    break;
                }
            }
        }

        if let Some(err) = upload_err {
            let _ = self
                .abort_multipart_upload(bucket, object, upload_id)?
                .build()
                .send()
                .await;
            return Err(err);
        }

        let complete_resp = self
            .complete_multipart_upload(bucket, object, upload_id.clone(), part_infos.clone())?
            .build()
            .send()
            .await?;

        let final_etag = complete_resp.etag()?.into_inner();

        Ok(RdmaMultipartResponse {
            etag: final_etag,
            upload_id: upload_id.into_inner(),
            total_bytes_transferred: total,
            parts: part_infos,
        })
    }

    /// Returns `true` when the process-wide cuObjClient is connected to a
    /// cuObjServer — i.e., an RDMA transfer is likely to succeed.
    pub fn rdma_available(&self) -> bool {
        shared().map(|c| c.is_connected()).unwrap_or(false)
    }

    fn resolve_rdma_region(&self) -> Region {
        self.get_region_from_url()
            .and_then(|r| Region::new(r).ok())
            .unwrap_or_else(|| DEFAULT_REGION.clone())
    }
}
