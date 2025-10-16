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

use crate::impl_has_s3fields;
use crate::s3::error::{Error, S3ServerError, ValidationErr};
use crate::s3::minio_error_response::MinioErrorCode;
use crate::s3::response::a_response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::{FromS3Response, S3Request};
use async_trait::async_trait;
use bytes::Bytes;
use http::HeaderMap;
use std::mem;

/// Represents the response of the `[delete_bucket_replication()](crate::s3::client::MinioClient::delete_bucket_replication) API call.
/// This struct contains metadata and information about the bucket whose replication configuration was removed.
#[derive(Clone, Debug)]
pub struct DeleteBucketReplicationResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_s3fields!(DeleteBucketReplicationResponse);

impl HasBucket for DeleteBucketReplicationResponse {}
impl HasRegion for DeleteBucketReplicationResponse {}

#[async_trait]
impl FromS3Response for DeleteBucketReplicationResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        match response {
            Ok(mut resp) => Ok(Self {
                request,
                headers: mem::take(resp.headers_mut()),
                body: resp.bytes().await.map_err(ValidationErr::from)?,
            }),
            Err(Error::S3Server(S3ServerError::S3Error(mut e)))
                if matches!(
                    e.code(),
                    MinioErrorCode::ReplicationConfigurationNotFoundError
                ) =>
            {
                Ok(Self {
                    request,
                    headers: e.take_headers(),
                    body: Bytes::new(),
                })
            }
            Err(e) => Err(e),
        }
    }
}
