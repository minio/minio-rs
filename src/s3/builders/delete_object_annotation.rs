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
use crate::s3::response::DeleteObjectAnnotationResponse;
use crate::s3::types::{
    AnnotationName, BucketName, ObjectKey, Region, S3Api, S3Request, ToS3Request, VersionId,
};
use crate::s3::utils::insert;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the DeleteObjectAnnotation AIStor API operation.
///
/// Permanently deletes a single named annotation (irreversible; annotations
/// have no version history). This struct constructs the parameters required for
/// the [`MinioClient::delete_object_annotation`](crate::s3::client::MinioClient::delete_object_annotation) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct DeleteObjectAnnotation {
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
    annotation_name: AnnotationName,
    #[builder(default, setter(into))]
    version_id: Option<VersionId>,
    /// When set, delete the annotation only if the parent object's ETag matches
    /// (sent as `x-amz-object-if-match`).
    #[builder(default, setter(into))]
    if_match: Option<String>,
}

/// Builder type for [`DeleteObjectAnnotation`] returned by
/// [`MinioClient::delete_object_annotation`](crate::s3::client::MinioClient::delete_object_annotation).
pub type DeleteObjectAnnotationBldr = DeleteObjectAnnotationBuilder<(
    (MinioClient,),
    (),
    (),
    (),
    (BucketName,),
    (ObjectKey,),
    (AnnotationName,),
    (),
    (),
)>;

impl S3Api for DeleteObjectAnnotation {
    type S3Response = DeleteObjectAnnotationResponse;
}

impl ToS3Request for DeleteObjectAnnotation {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        let mut query_params: Multimap = insert(self.extra_query_params, "annotation");
        query_params.add("annotationName", self.annotation_name.into_inner());
        query_params.add_version(self.version_id);

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        if let Some(etag) = self.if_match {
            headers.add(X_AMZ_OBJECT_IF_MATCH, etag);
        }

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::DELETE)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(query_params)
            .object(self.object)
            .headers(headers)
            .build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::creds::StaticProvider;
    use crate::s3::http::BaseUrl;

    fn test_client() -> MinioClient {
        let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
        let provider = StaticProvider::new("minioadmin", "minioadmin", None);
        MinioClient::new(base_url, Some(provider), None, None).unwrap()
    }

    #[test]
    fn builds_delete_request() {
        let req = test_client()
            .delete_object_annotation("bucket", "object", "review")
            .unwrap()
            .version_id(Some(VersionId::new("v1").unwrap()))
            .build()
            .to_s3request()
            .unwrap();
        assert_eq!(req.method, Method::DELETE);
        assert!(req.query_params.contains_key("annotation"));
        assert_eq!(
            req.query_params.get("annotationName").map(String::as_str),
            Some("review")
        );
        assert_eq!(
            req.query_params.get("versionId").map(String::as_str),
            Some("v1")
        );
    }

    #[test]
    fn if_match_sets_exact_conditional_header() {
        let req = test_client()
            .delete_object_annotation("bucket", "object", "review")
            .unwrap()
            .if_match(Some("\"etag\"".to_string()))
            .build()
            .to_s3request()
            .unwrap();
        assert_eq!(
            req.headers.get(X_AMZ_OBJECT_IF_MATCH).map(String::as_str),
            Some("\"etag\"")
        );
    }
}
