// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022-2024 MinIO, Inc.
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

//! Responses for RemoveObject APIs.

use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use xmltree::Element;

use crate::s3::{
    error::Error,
    types::{FromS3Response, S3Request},
    utils::{get_default_text, get_option_text, get_text},
};

#[derive(Debug, Clone)]
pub struct RemoveObjectResponse {
    pub headers: HeaderMap,
    /// Value of the `x-amz-delete-marker` header.
    pub is_delete_marker: bool,
    /// If a delete marker was created, this field will contain the version_id
    /// of the delete marker. Value of the `x-amz-version-id` header.
    pub version_id: Option<String>,
}

#[async_trait]
impl FromS3Response for RemoveObjectResponse {
    async fn from_s3response<'a>(
        _req: S3Request<'a>,
        resp: reqwest::Response,
    ) -> Result<Self, Error> {
        let headers = resp.headers().clone();
        let is_delete_marker = headers
            .get("x-amz-delete-marker")
            .map(|v| v == "true")
            .unwrap_or(false);

        let version_id = headers
            .get("x-amz-version-id")
            .map(|v| v.to_str().unwrap().to_string());

        Ok(RemoveObjectResponse {
            headers,
            is_delete_marker,
            version_id,
        })
    }
}

/// Error info returned by the S3 API when an object could not be deleted.
#[derive(Clone, Debug)]
pub struct DeleteError {
    pub code: String,
    pub message: String,
    pub object_name: String,
    pub version_id: Option<String>,
}

/// Information about an object that was deleted.
#[derive(Clone, Debug)]
pub struct DeletedObject {
    pub name: String,
    pub version_id: Option<String>,
    pub delete_marker: bool,
    pub delete_marker_version_id: Option<String>,
}

/// Response of
/// [delete_objects()](crate::s3::client_core::ClientCore::delete_objects)
/// S3 API. It is also returned by the
/// [remove_objects()](crate::s3::client::Client::remove_objects) API in the
/// form of a stream.
#[derive(Clone, Debug)]
pub struct RemoveObjectsResponse {
    pub headers: HeaderMap,
    pub result: Vec<DeleteResult>,
}

/// Result of deleting an object.
#[derive(Clone, Debug)]
pub enum DeleteResult {
    Deleted(DeletedObject),
    Error(DeleteError),
}

impl From<DeleteResult> for Result<DeletedObject, DeleteError> {
    fn from(result: DeleteResult) -> Self {
        match result {
            DeleteResult::Deleted(obj) => Ok(obj),
            DeleteResult::Error(err) => Err(err),
        }
    }
}

impl DeleteResult {
    pub fn is_deleted(&self) -> bool {
        matches!(self, DeleteResult::Deleted(_))
    }

    pub fn is_error(&self) -> bool {
        matches!(self, DeleteResult::Error(_))
    }
}

#[async_trait]
impl FromS3Response for RemoveObjectsResponse {
    async fn from_s3response<'a>(
        _req: S3Request<'a>,
        resp: reqwest::Response,
    ) -> Result<Self, Error> {
        let headers = resp.headers().clone();

        let body = resp.bytes().await?;

        let root = Element::parse(body.reader())?;
        let result = root
            .children
            .iter()
            .map(|elem| elem.as_element().unwrap())
            .map(|elem| {
                if elem.name == "Deleted" {
                    Ok(DeleteResult::Deleted(DeletedObject {
                        name: get_text(elem, "Key")?,
                        version_id: get_option_text(elem, "VersionId"),
                        delete_marker: get_default_text(elem, "DeleteMarker").to_lowercase()
                            == "true",
                        delete_marker_version_id: get_option_text(elem, "DeleteMarkerVersionId"),
                    }))
                } else {
                    assert_eq!(elem.name, "Error");
                    Ok(DeleteResult::Error(DeleteError {
                        code: get_text(elem, "Code")?,
                        message: get_text(elem, "Message")?,
                        object_name: get_text(elem, "Key")?,
                        version_id: get_option_text(elem, "VersionId"),
                    }))
                }
            })
            .collect::<Result<Vec<DeleteResult>, Error>>()?;

        Ok(RemoveObjectsResponse { headers, result })
    }
}
