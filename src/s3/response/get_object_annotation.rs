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

use crate::s3::error::ValidationErr;
use crate::s3::response_traits::{
    HasBucket, HasChecksumHeaders, HasEtagFromHeaders, HasObject, HasRegion, HasVersion,
};
use crate::s3::types::S3Request;
use crate::s3::utils::{ChecksumAlgorithm, compute_checksum};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;

/// Response of
/// [get_object_annotation()](crate::s3::client::MinioClient::get_object_annotation)
/// API. The response body is the annotation payload, accessible via
/// [`payload`](Self::payload) / [`into_payload`](Self::into_payload).
#[derive(Clone, Debug)]
pub struct GetObjectAnnotationResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetObjectAnnotationResponse);
impl_has_s3fields!(GetObjectAnnotationResponse);

impl GetObjectAnnotationResponse {
    /// The annotation payload bytes.
    #[inline]
    pub fn payload(&self) -> &Bytes {
        &self.body
    }

    /// Consume the response, returning the annotation payload bytes.
    #[inline]
    #[must_use]
    pub fn into_payload(self) -> Bytes {
        self.body
    }

    /// Checks the payload against the checksum the server returned, which it
    /// does for a request with `enable_checksum`. A response without a
    /// checksum passes.
    pub fn verify_checksum(&self) -> Result<(), ValidationErr> {
        verify_payload(&self.body, |algorithm| self.get_checksum(algorithm))
    }
}

const ANNOTATION_CHECKSUM_ALGORITHMS: [ChecksumAlgorithm; 10] = [
    ChecksumAlgorithm::CRC32,
    ChecksumAlgorithm::CRC32C,
    ChecksumAlgorithm::CRC64NVME,
    ChecksumAlgorithm::SHA1,
    ChecksumAlgorithm::SHA256,
    ChecksumAlgorithm::MD5,
    ChecksumAlgorithm::SHA512,
    ChecksumAlgorithm::XXHash64,
    ChecksumAlgorithm::XXHash3,
    ChecksumAlgorithm::XXHash128,
];

fn verify_payload(
    payload: &[u8],
    checksum: impl Fn(ChecksumAlgorithm) -> Option<String>,
) -> Result<(), ValidationErr> {
    for algorithm in ANNOTATION_CHECKSUM_ALGORITHMS {
        if let Some(expected) = checksum(algorithm) {
            let computed = compute_checksum(algorithm, payload);
            if computed != expected {
                return Err(ValidationErr::ChecksumMismatch { expected, computed });
            }
        }
    }
    Ok(())
}

impl HasBucket for GetObjectAnnotationResponse {}
impl HasRegion for GetObjectAnnotationResponse {}
impl HasObject for GetObjectAnnotationResponse {}
impl HasVersion for GetObjectAnnotationResponse {}
impl HasEtagFromHeaders for GetObjectAnnotationResponse {}
impl HasChecksumHeaders for GetObjectAnnotationResponse {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifies_payload_against_returned_checksum() {
        let payload = b"hello";
        let good = compute_checksum(ChecksumAlgorithm::CRC64NVME, payload);
        let lookup = |value: &str| {
            let value = value.to_string();
            move |a: ChecksumAlgorithm| (a == ChecksumAlgorithm::CRC64NVME).then(|| value.clone())
        };

        assert!(verify_payload(payload, lookup(&good)).is_ok());
        assert!(matches!(
            verify_payload(b"other", lookup(&good)),
            Err(ValidationErr::ChecksumMismatch { .. })
        ));
        assert!(verify_payload(payload, |_| None).is_ok());
    }
}
