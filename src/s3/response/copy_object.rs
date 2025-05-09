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
use crate::s3::utils::{get_text, take_bucket, take_object};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Represents the response of the `upload_part_copy` API call.
/// This struct contains metadata and information about the part being copied during a multipart upload.
///
/// # Fields
///
/// * `headers` - HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
/// * `region` - The AWS region where the bucket resides.
/// * `bucket` - Name of the bucket containing the object.
/// * `object` - Key (path) identifying the object within the bucket.
/// * `etag` - Entity tag representing a specific version of the object.
/// * `version_id` - Version ID of the object, if versioning is enabled. Value of the `x-amz-version-id` header.
#[derive(Clone, Debug)]
pub struct UploadPartCopyResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket containing the object.
    pub bucket: String,

    /// Key (path) identifying the object within the bucket.
    pub object: String,

    /// Entity tag representing a specific version of the object.
    pub etag: String,

    /// Version ID of the object, if versioning is enabled. Value of the `x-amz-version-id` header.
    pub version_id: Option<String>,
}

#[async_trait]
impl FromS3Response for UploadPartCopyResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());

        let etag: String = {
            let body: Bytes = resp.bytes().await?;
            let root = Element::parse(body.reader())?;
            get_text(&root, "ETag")?.trim_matches('"').to_string()
        };

        let version_id: Option<String> = headers
            .get("x-amz-version-id")
            .and_then(|v| v.to_str().ok().map(String::from));

        Ok(Self {
            headers,
            region: req.inner_region,
            bucket: take_bucket(req.bucket)?,
            object: take_object(req.object)?,
            etag,
            version_id,
        })
    }
}

#[derive(Clone, Debug)]
pub struct CopyObjectInternalResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
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
        let bucket = req
            .bucket
            .ok_or_else(|| Error::InvalidBucketName("no bucket specified".into()))?;
        let object = req
            .object
            .ok_or_else(|| Error::InvalidObjectName("no object specified".into()))?;
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());

        let etag: String = {
            let body: Bytes = resp.bytes().await?;
            let root = Element::parse(body.reader())?;
            get_text(&root, "ETag")?.trim_matches('"').to_string()
        };

        let version_id: Option<String> = headers
            .get("x-amz-version-id")
            .and_then(|v| v.to_str().ok().map(String::from));

        Ok(CopyObjectInternalResponse {
            headers,
            region: req.inner_region,
            bucket,
            object,
            etag,
            version_id,
        })
    }
}

/// Represents the response of the `copy_object` API call.
/// This struct contains metadata and information about the object being copied.
///
/// # Fields
///
/// * `headers` - HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
/// * `region` - The AWS region where the bucket resides.
/// * `bucket` - Name of the bucket containing the object.
/// * `object` - Key (path) identifying the object within the bucket.
/// * `etag` - Entity tag representing a specific version of the object.
/// * `version_id` - Version ID of the object, if versioning is enabled. Value of the `x-amz-version-id` header.
#[derive(Clone, Debug)]
pub struct CopyObjectResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket containing the object.
    pub bucket: String,

    /// Key (path) identifying the object within the bucket.
    pub object: String,

    /// Entity tag representing a specific version of the object.
    pub etag: String,

    /// Version ID of the object, if versioning is enabled. Value of the `x-amz-version-id` header.
    pub version_id: Option<String>,
}

/// Represents the response of the `[compose_object()](crate::s3::client::Client::compose_object) API call.
/// This struct contains metadata and information about the composed object.
///
/// # Fields
///
/// * `headers` - HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
/// * `bucket` - Name of the bucket containing the composed object.
/// * `object` - Key (path) identifying the composed object within the bucket.
/// * `region` - The AWS region where the bucket resides.
/// * `etag` - Entity tag representing a specific version of the composed object.
/// * `version_id` - Version ID of the composed object, if versioning is enabled.
#[derive(Debug, Clone)]
pub struct ComposeObjectResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// Name of the bucket containing the composed object.
    pub bucket: String,

    /// Key (path) identifying the composed object within the bucket.
    pub object: String,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Entity tag representing a specific version of the composed object.
    pub etag: String,

    /// Version ID of the composed object, if versioning is enabled.
    pub version_id: Option<String>,
}
