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

use crate::s3::error::{Error, S3ServerError, ValidationErr};
use crate::s3::minio_error_response::MinioErrorResponse;
use crate::s3::response_traits::{HasBucket, HasIsDeleteMarker, HasRegion, HasVersion};
use crate::s3::types::S3Request;
use crate::s3::utils::{get_text_default, get_text_option, get_text_result};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::{Buf, Bytes};
use http::HeaderMap;
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

/// Response of the [`delete_objects()`](crate::s3::client::MinioClient::delete_objects) S3 API.
///
/// It is also returned by the
/// [`delete_objects_streaming()`](crate::s3::client::MinioClient::delete_objects_streaming) API
/// in the form of a stream.
#[derive(Clone, Debug)]
pub struct DeleteObjectsResponse {
    request: S3Request,
    pub(crate) headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(DeleteObjectsResponse);
impl_has_s3fields!(DeleteObjectsResponse);

impl HasBucket for DeleteObjectsResponse {}
impl HasRegion for DeleteObjectsResponse {}

impl DeleteObjectsResponse {
    /// Parses the per-object delete results from the response body.
    ///
    /// A whole-request error can be returned by the server as a root-level `<Error>`
    /// element even on an HTTP 200 OK response (e.g. `SlowDownWrite`). In that case the
    /// element's children are `<Code>`/`<Message>` rather than `<Deleted>`/`<Error>`,
    /// so it is surfaced as an `Err` instead of being parsed as per-object results.
    pub fn result(&self) -> Result<Vec<DeleteResult>, Error> {
        parse_delete_objects(&self.body, &self.headers)
    }
}

fn parse_delete_objects(body: &Bytes, headers: &HeaderMap) -> Result<Vec<DeleteResult>, Error> {
    let root = Element::parse(body.clone().reader()).map_err(ValidationErr::from)?;

    if root.name == "Error" {
        let e = MinioErrorResponse::new_from_body(body.clone(), headers.clone())?;
        return Err(Error::S3Server(S3ServerError::S3Error(Box::new(e))));
    }

    root.children
        .iter()
        .filter_map(|node| node.as_element())
        .map(|elem| {
            if elem.name == "Deleted" {
                Ok(DeleteResult::Deleted(DeletedObject {
                    name: get_text_result(elem, "Key")?,
                    version_id: get_text_option(elem, "VersionId"),
                    delete_marker: get_text_default(elem, "DeleteMarker").to_lowercase() == "true",
                    delete_marker_version_id: get_text_option(elem, "DeleteMarkerVersionId"),
                }))
            } else if elem.name == "Error" {
                Ok(DeleteResult::Error(DeleteError {
                    code: get_text_result(elem, "Code")?,
                    message: get_text_result(elem, "Message")?,
                    object_name: get_text_result(elem, "Key")?,
                    version_id: get_text_option(elem, "VersionId"),
                }))
            } else {
                Err(ValidationErr::xml_error(format!(
                    "unexpected element '{}' in DeleteObjects response",
                    elem.name
                ))
                .into())
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_delete_objects_normal() {
        let body = Bytes::from_static(
            br#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult>
  <Deleted><Key>obj1</Key></Deleted>
  <Error><Key>obj2</Key><Code>AccessDenied</Code><Message>nope</Message></Error>
</DeleteResult>"#,
        );
        let results = parse_delete_objects(&body, &HeaderMap::new()).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].is_deleted());
        assert!(results[1].is_error());
    }

    #[test]
    fn test_parse_delete_objects_root_error_returns_err() {
        let body = Bytes::from_static(
            br#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>SlowDownWrite</Code>
  <Message>Resource requested is unwritable, please reduce your request rate</Message>
</Error>"#,
        );
        let err = parse_delete_objects(&body, &HeaderMap::new()).unwrap_err();
        assert!(matches!(err, Error::S3Server(S3ServerError::S3Error(_))));
    }

    #[test]
    fn test_parse_delete_objects_unexpected_element_returns_err() {
        let body = Bytes::from_static(
            br#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult>
  <Unexpected><Key>obj1</Key></Unexpected>
</DeleteResult>"#,
        );
        let err = parse_delete_objects(&body, &HeaderMap::new()).unwrap_err();
        assert!(matches!(
            err,
            Error::Validation(ValidationErr::XmlError { .. })
        ));
    }
}
