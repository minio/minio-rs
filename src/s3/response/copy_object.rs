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

use crate::s3::error::Error;
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::get_text;
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

#[derive(Clone, Debug)]
pub struct UploadPartCopyResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,

    pub object: String,
    pub etag: String,
    pub version_id: Option<String>,
}

#[async_trait]
impl FromS3Response for UploadPartCopyResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let bucket: String = match req.bucket {
            None => return Err(Error::InvalidBucketName("no bucket specified".to_string())),
            Some(v) => v,
        };
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());

        let etag: String = {
            let body: Bytes = resp.bytes().await?;
            let root = Element::parse(body.reader())?;
            get_text(&root, "ETag")?.trim_matches('"').to_string()
        };

        let version_id: Option<String> = match headers.get("x-amz-version-id") {
            Some(v) => Some(v.to_str()?.to_string()),
            None => None,
        };

        Ok(UploadPartCopyResponse {
            headers,
            region: req.inner_region,
            bucket,
            object: req.object.unwrap(),
            etag,
            version_id,
        })
    }
}

#[derive(Clone, Debug)]
pub struct CopyObjectInternalResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,

    pub object: String,
    pub etag: String,
    pub version_id: Option<String>,
}

#[async_trait]
impl FromS3Response for CopyObjectInternalResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let bucket: String = match req.bucket {
            None => return Err(Error::InvalidBucketName("no bucket specified".to_string())),
            Some(v) => v,
        };
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());

        let etag: String = {
            let body: Bytes = resp.bytes().await?;
            let root = Element::parse(body.reader())?;
            get_text(&root, "ETag")?.trim_matches('"').to_string()
        };

        let version_id: Option<String> = match headers.get("x-amz-version-id") {
            Some(v) => Some(v.to_str()?.to_string()),
            None => None,
        };

        Ok(CopyObjectInternalResponse {
            headers,
            region: req.inner_region,
            bucket,
            object: req.object.unwrap(),
            etag,
            version_id,
        })
    }
}

/// Response of
/// [copy_object()](crate::s3::client::Client::copy_object_old)
/// API
#[derive(Clone, Debug)]
pub struct CopyObjectResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,

    pub object: String,
    pub etag: String,
    pub version_id: Option<String>,
}

/// Response of [compose_object()](crate::s3::client::Client::compose_object) API
#[derive(Debug, Clone)]
pub struct ComposeObjectResponse {
    pub headers: HeaderMap,
    pub bucket: String,
    pub object: String,

    pub region: String,
    pub etag: String,
    pub version_id: Option<String>,
}
