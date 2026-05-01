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

use crate::impl_has_madmin_fields;
use crate::madmin::response::response_traits::HasBucket;
use crate::madmin::types::bucket_target::BucketTargets;
use crate::madmin::types::{FromMadminResponse, MadminRequest};
use crate::s3::error::Error;
use async_trait::async_trait;
use bytes::Bytes;
use http::HeaderMap;
use std::mem;

#[derive(Clone, Debug)]
pub struct ListRemoteTargetsResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_madmin_fields!(ListRemoteTargetsResponse);
impl HasBucket for ListRemoteTargetsResponse {}

impl ListRemoteTargetsResponse {
    /// Returns the bucket targets configuration.
    pub fn bucket_targets(&self) -> Result<BucketTargets, crate::s3::error::ValidationErr> {
        if self.body.is_empty() || &self.body[..] == b"null" {
            Ok(BucketTargets::default())
        } else {
            serde_json::from_slice(&self.body).map_err(crate::s3::error::ValidationErr::JsonError)
        }
    }
}

//TODO did you forget to refactor from_madmin_response here?
#[async_trait]
impl FromMadminResponse for ListRemoteTargetsResponse {
    async fn from_madmin_response(
        request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut r = response?;
        Ok(ListRemoteTargetsResponse {
            request,
            headers: mem::take(r.headers_mut()),
            body: r
                .bytes()
                .await
                .map_err(crate::s3::error::ValidationErr::HttpError)?,
        })
    }
}
