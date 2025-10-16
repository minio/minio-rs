// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022-2025 MinIO, Inc.
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

use crate::s3::error::{Error, ValidationErr};
use crate::s3::response::a_response_traits::{
    HasBucket, HasIsDeleteMarker, HasRegion, HasS3Fields, HasVersion,
};
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::{get_text_default, get_text_option, get_text_result};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

#[derive(Clone, Debug)]
pub struct DeleteObjectResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(DeleteObjectResponse);
impl_has_s3fields!(DeleteObjectResponse);

impl HasBucket for DeleteObjectResponse {}
impl HasRegion for DeleteObjectResponse {}
impl HasVersion for DeleteObjectResponse {}
impl HasIsDeleteMarker for DeleteObjectResponse {}

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

/// Response of
/// [delete_objects()](crate::s3::client::MinioClient::delete_objects)
/// S3 API. It is also returned by the
/// [remove_objects()](crate::s3::client::MinioClient::delete_objects_streaming) API in the
/// form of a stream.
#[derive(Clone, Debug)]
pub struct DeleteObjectsResponse {
    request: S3Request,
    pub(crate) headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(DeleteObjectsResponse);
impl_has_s3fields!(DeleteObjectsResponse);

impl DeleteObjectsResponse {
    /// Returns the bucket name for which the delete operation was performed.
    pub fn result(&self) -> Result<Vec<DeleteResult>, Error> {
        let root = Element::parse(self.body.clone().reader()).map_err(ValidationErr::from)?;
        let result = root
            .children
            .iter()
            .map(|elem| elem.as_element().unwrap())
            .map(|elem| {
                if elem.name == "Deleted" {
                    Ok(DeleteResult::Deleted(DeletedObject {
                        name: get_text_result(elem, "Key")?,
                        version_id: get_text_option(elem, "VersionId"),
                        delete_marker: get_text_default(elem, "DeleteMarker").to_lowercase()
                            == "true",
                        delete_marker_version_id: get_text_option(elem, "DeleteMarkerVersionId"),
                    }))
                } else {
                    assert_eq!(elem.name, "Error");
                    Ok(DeleteResult::Error(DeleteError {
                        code: get_text_result(elem, "Code")?,
                        message: get_text_result(elem, "Message")?,
                        object_name: get_text_result(elem, "Key")?,
                        version_id: get_text_option(elem, "VersionId"),
                    }))
                }
            })
            .collect::<Result<Vec<DeleteResult>, Error>>()?;
        Ok(result)
    }
}
