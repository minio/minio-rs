// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2025 MinIO, Inc.
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

use crate::s3::response_traits::{HasBucket, HasObject, HasRegion, HasVersion};
use crate::s3::types::S3Request;
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;

/// Response of
/// [update_object_encryption()](crate::s3::client::MinioClient::update_object_encryption)
/// API
#[derive(Clone, Debug)]
pub struct UpdateObjectEncryptionResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(UpdateObjectEncryptionResponse);
impl_has_s3fields!(UpdateObjectEncryptionResponse);

impl HasBucket for UpdateObjectEncryptionResponse {}
impl HasRegion for UpdateObjectEncryptionResponse {}
impl HasObject for UpdateObjectEncryptionResponse {}
impl HasVersion for UpdateObjectEncryptionResponse {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::client::MinioClient;
    use crate::s3::creds::StaticProvider;
    use crate::s3::http::BaseUrl;
    use crate::s3::types::{BucketName, ObjectKey};
    use http::Method;

    fn dummy_request() -> S3Request {
        let base_url: BaseUrl = "http://localhost:9000".parse().unwrap();
        let provider = StaticProvider::new("minioadmin", "minioadmin", None);
        let client = MinioClient::new(base_url, Some(provider), None, None).unwrap();
        S3Request::builder()
            .client(client)
            .method(Method::PUT)
            .bucket(BucketName::new("mybucket").unwrap())
            .object(ObjectKey::new("myobject").unwrap())
            .build()
    }

    #[test]
    fn version_id_extracted_from_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-version-id", "returned-version-id".parse().unwrap());
        let resp = UpdateObjectEncryptionResponse {
            request: dummy_request(),
            headers,
            body: Bytes::new(),
        };
        assert_eq!(
            resp.version_id().map(|v| v.into_inner()),
            Some("returned-version-id".to_string())
        );
    }

    #[test]
    fn version_id_absent_when_header_missing() {
        let resp = UpdateObjectEncryptionResponse {
            request: dummy_request(),
            headers: HeaderMap::new(),
            body: Bytes::new(),
        };
        assert!(resp.version_id().is_none());
    }
}
