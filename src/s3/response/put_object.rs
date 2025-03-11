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

use crate::s3::{
    error::Error,
    types::{FromS3Response, S3Request},
    utils::get_text,
};

/// Response of [put_object_api()](crate::s3::client::Client::put_object) API
#[derive(Debug, Clone)]
pub struct PutObjectResponse {
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
        let bucket: String = match req.bucket {
            None => return Err(Error::InvalidBucketName("no bucket specified".to_string())),
            Some(v) => v.to_string(),
        };
        let object: String = match req.object {
            None => {
                return Err(Error::InvalidObjectName(
                    "Missing object name in request".into(),
                ));
            }
            Some(v) => v.to_string(),
        };

        let mut resp = resp?;
        let headers: HeaderMap = mem::take(resp.headers_mut());

        let etag: String = match headers.get("etag") {
            Some(v) => v.to_str()?.to_string().trim_matches('"').to_string(),
            None => String::new(),
        };

        let version_id: Option<String> = match headers.get("x-amz-version-id") {
            Some(v) => Some(v.to_str()?.to_string()),
            None => None,
        };

        Ok(PutObjectResponse {
            headers,
            bucket,
            object,
            region: req.region.unwrap_or("".to_string()),
            etag,
            version_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CreateMultipartUploadResponse {
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
        let bucket: String = match req.bucket {
            None => return Err(Error::InvalidBucketName("no bucket specified".to_string())),
            Some(v) => v.to_string(),
        };
        let object: String = match req.object {
            None => {
                return Err(Error::InvalidObjectName(
                    "Missing object name in request".into(),
                ));
            }
            Some(v) => v.to_string(),
        };

        let mut resp = resp?;
        let headers: HeaderMap = mem::take(resp.headers_mut());
        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;

        let region: String = req.region.unwrap_or("".to_string()); // Keep this since it defaults to an empty string

        let upload_id: String = get_text(&root, "UploadId")?;

        Ok(CreateMultipartUploadResponse {
            headers,
            region,
            bucket,
            object,
            upload_id,
        })
    }
}

pub type AbortMultipartUploadResponse = CreateMultipartUploadResponse;

pub type CompleteMultipartUploadResponse = PutObjectResponse;

pub type UploadPartResponse = PutObjectResponse;

#[derive(Debug, Clone)]
pub struct PutObjectContentResponse {
    pub headers: HeaderMap,
    pub bucket: String,
    pub object: String,
    pub region: String,
    pub object_size: u64,
    pub etag: String,
    pub version_id: Option<String>,
}
