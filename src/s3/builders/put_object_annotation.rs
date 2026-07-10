// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::header_constants::X_AMZ_OBJECT_IF_MATCH;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::PutObjectAnnotationResponse;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{BucketName, ObjectKey, Region, S3Api, S3Request, ToS3Request, VersionId};
use crate::s3::utils::{insert, validate_annotation_name, validate_annotation_payload_len};
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the PutObjectAnnotation AIStor API operation.
///
/// Creates or overwrites a named annotation (a 1 byte to 1 MiB payload) on an
/// object version. The parent object's data and ETag are never modified. This
/// struct constructs the parameters required for the
/// [`MinioClient::put_object_annotation`](crate::s3::client::MinioClient::put_object_annotation) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct PutObjectAnnotation {
    #[builder(!default)] // force required
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<Region>,
    #[builder(setter(into), !default)]
    bucket: BucketName,
    #[builder(setter(into), !default)]
    object: ObjectKey,
    #[builder(setter(into), !default)]
    annotation_name: String,
    #[builder(setter(into), !default)]
    payload: Bytes,
    #[builder(default, setter(into))]
    version_id: Option<VersionId>,
    /// When set, apply the annotation only if the parent object's ETag matches
    /// (sent as `x-amz-object-if-match`).
    #[builder(default, setter(into))]
    if_match: Option<String>,
}

/// Builder type for [`PutObjectAnnotation`] returned by
/// [`MinioClient::put_object_annotation`](crate::s3::client::MinioClient::put_object_annotation).
pub type PutObjectAnnotationBldr = PutObjectAnnotationBuilder<(
    (MinioClient,),
    (),
    (),
    (),
    (BucketName,),
    (ObjectKey,),
    (String,),
    (Bytes,),
    (),
    (),
)>;

impl S3Api for PutObjectAnnotation {
    type S3Response = PutObjectAnnotationResponse;
}

impl ToS3Request for PutObjectAnnotation {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        validate_annotation_name(&self.annotation_name)?;
        validate_annotation_payload_len(self.payload.len())?;

        let mut query_params: Multimap = insert(self.extra_query_params, "annotation");
        query_params.add("annotationName", self.annotation_name);
        query_params.add_version(self.version_id);

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        if let Some(etag) = self.if_match {
            headers.add(X_AMZ_OBJECT_IF_MATCH, etag);
        }

        let body = Arc::new(SegmentedBytes::from(self.payload));

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::PUT)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(query_params)
            .object(self.object)
            .headers(headers)
            .body(body)
            .build())
    }
}
