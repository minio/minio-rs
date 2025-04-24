// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2023 MinIO, Inc.
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

use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

use crate::s3::utils::{take_bucket, take_object};
use crate::s3::{
    error::Error,
    types::{FromS3Response, S3Request},
    utils::get_text,
};

/// Response of [put_object_api()](crate::s3::client::Client::put_object) API
#[derive(Debug, Clone)]
pub struct PutObjectResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,
    pub bucket: String,
    pub object: String,
    pub region: String,
    pub etag: String,
    pub version_id: Option<String>,
}

#[async_trait]
impl FromS3Response for PutObjectResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());

        let etag: String = headers
            .get("etag")
            .and_then(|v| v.to_str().ok()) // Convert to &str safely
            .map(|s| s.trim_matches('"').to_string()) // Trim and convert to String
            .unwrap_or_default();

        let version_id: Option<String> = headers
            .get("x-amz-version-id")
            .and_then(|v| v.to_str().ok().map(String::from));

        Ok(Self {
            headers,
            bucket: take_bucket(req.bucket)?,
            object: take_object(req.object)?,
            region: req.inner_region,
            etag,
            version_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CreateMultipartUploadResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub object: String,
    pub upload_id: String,
}

#[async_trait]
impl FromS3Response for CreateMultipartUploadResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());
        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;
        let upload_id: String =
            get_text(&root, "UploadId").map_err(|e| Error::InvalidUploadId(e.to_string()))?;

        Ok(Self {
            headers,
            region: req.inner_region,
            bucket: take_bucket(req.bucket)?,
            object: take_object(req.object)?,
            upload_id,
        })
    }
}

pub type AbortMultipartUploadResponse = CreateMultipartUploadResponse;

pub type CompleteMultipartUploadResponse = PutObjectResponse;

pub type UploadPartResponse = PutObjectResponse;

#[derive(Debug, Clone)]
pub struct PutObjectContentResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket containing the object.
    pub bucket: String,

    /// Key (path) identifying the object within the bucket.
    pub object: String,

    /// Size of the object in bytes.
    pub object_size: u64,

    /// Entity tag representing a specific version of the object.
    pub etag: String,

    /// Version ID of the object, if versioning is enabled. Value of the `x-amz-version-id` header.
    pub version_id: Option<String>,
}
