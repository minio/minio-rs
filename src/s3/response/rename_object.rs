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

use crate::s3::response_traits::{HasBucket, HasEtagFromHeaders, HasObject, HasRegion};
use crate::s3::types::S3Request;
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;

/// Response from the [`rename_object`](crate::s3::client::MinioClient::rename_object) API (MinIO extension).
///
/// On success the server returns HTTP 200 with an empty body and a quoted
/// `ETag` header for the renamed object.
#[derive(Clone, Debug)]
pub struct RenameObjectResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(RenameObjectResponse);
impl_has_s3fields!(RenameObjectResponse);

impl HasBucket for RenameObjectResponse {}
impl HasObject for RenameObjectResponse {}
impl HasRegion for RenameObjectResponse {}
impl HasEtagFromHeaders for RenameObjectResponse {}

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
            .object(ObjectKey::new("new-name").unwrap())
            .build()
    }

    #[test]
    fn etag_parsed_from_quoted_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "etag",
            "\"d41d8cd98f00b204e9800998ecf8427e\"".parse().unwrap(),
        );
        let resp = RenameObjectResponse {
            request: dummy_request(),
            headers,
            body: Bytes::new(),
        };
        assert_eq!(
            resp.etag().unwrap().as_str(),
            "d41d8cd98f00b204e9800998ecf8427e"
        );
    }

    #[test]
    fn bucket_and_object_exposed() {
        let resp = RenameObjectResponse {
            request: dummy_request(),
            headers: HeaderMap::new(),
            body: Bytes::new(),
        };
        assert_eq!(resp.bucket().map(|b| b.as_str()), Some("mybucket"));
        assert_eq!(resp.object().map(|o| o.as_str()), Some("new-name"));
    }
}
