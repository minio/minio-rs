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
    pub bucket_name: String,
    pub object_name: String,
    pub location: String,
    pub etag: String,
    pub version_id: Option<String>,
}

#[async_trait]
impl FromS3Response for PutObjectResponse {
    async fn from_s3response(
        req: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let response = response?;
        let header_map = response.headers();

        Ok(PutObjectResponse {
            headers: header_map.clone(),
            bucket_name: req.bucket.unwrap().to_string(),
            object_name: req.object.unwrap().to_string(),
            location: req.region.unwrap_or("".to_string()),
            etag: match header_map.get("etag") {
                Some(v) => v.to_str()?.to_string().trim_matches('"').to_string(),
                _ => String::new(),
            },
            version_id: match header_map.get("x-amz-version-id") {
                Some(v) => Some(v.to_str()?.to_string()),
                None => None,
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct CreateMultipartUploadResponse2 {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub object_name: String,
    pub upload_id: String,
}

#[async_trait]
impl FromS3Response for CreateMultipartUploadResponse2 {
    async fn from_s3response(
        req: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let response = response?;
        let headers = response.headers().clone();
        let body = response.bytes().await?;
        let root = Element::parse(body.reader())?;

        let region: String = req.region.unwrap_or("".to_string()); // Keep this since it defaults to an empty string

        let bucket_name: String = req
            .bucket
            .ok_or_else(|| {
                Error::InvalidBucketName(String::from("Missing bucket name in request"))
            })?
            .to_string();

        let object_name: String = req
            .object
            .ok_or_else(|| {
                Error::InvalidObjectName(String::from("Missing object name in request"))
            })?
            .to_string();

        let upload_id: String = get_text(&root, "UploadId")?;

        Ok(CreateMultipartUploadResponse2 {
            headers,
            region,
            bucket_name,
            object_name,
            upload_id,
        })
    }
}

pub type AbortMultipartUploadResponse2 = CreateMultipartUploadResponse2;

pub type CompleteMultipartUploadResponse2 = PutObjectResponse;

pub type UploadPartResponse2 = PutObjectResponse;

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
