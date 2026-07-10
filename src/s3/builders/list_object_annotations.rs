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
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::ListObjectAnnotationsResponse;
use crate::s3::types::{BucketName, ObjectKey, Region, S3Api, S3Request, ToS3Request, VersionId};
use crate::s3::utils::insert;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the ListObjectAnnotations AIStor API operation.
///
/// Lists the annotations attached to an object version. This struct constructs
/// the parameters required for the
/// [`MinioClient::list_object_annotations`](crate::s3::client::MinioClient::list_object_annotations) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct ListObjectAnnotations {
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
    #[builder(default, setter(into))]
    version_id: Option<VersionId>,
}

/// Builder type for [`ListObjectAnnotations`] returned by
/// [`MinioClient::list_object_annotations`](crate::s3::client::MinioClient::list_object_annotations).
pub type ListObjectAnnotationsBldr =
    ListObjectAnnotationsBuilder<((MinioClient,), (), (), (), (BucketName,), (ObjectKey,), ())>;

impl S3Api for ListObjectAnnotations {
    type S3Response = ListObjectAnnotationsResponse;
}

impl ToS3Request for ListObjectAnnotations {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        let mut query_params: Multimap = insert(self.extra_query_params, "annotation");
        query_params.add_version(self.version_id);

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::GET)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(query_params)
            .object(self.object)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::creds::StaticProvider;
    use crate::s3::http::BaseUrl;
    use crate::s3::types::VersionId;

    fn test_client() -> MinioClient {
        let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
        let provider = StaticProvider::new("minioadmin", "minioadmin", None);
        MinioClient::new(base_url, Some(provider), None, None).unwrap()
    }

    #[test]
    fn sets_annotation_query_without_name() {
        let req = test_client()
            .list_object_annotations("bucket", "object")
            .unwrap()
            .build()
            .to_s3request()
            .unwrap();
        assert_eq!(req.method, Method::GET);
        assert!(req.query_params.contains_key("annotation"));
        assert!(!req.query_params.contains_key("annotationName"));
    }

    #[test]
    fn version_id_is_propagated() {
        let req = test_client()
            .list_object_annotations("bucket", "object")
            .unwrap()
            .version_id(Some(VersionId::new("v1").unwrap()))
            .build()
            .to_s3request()
            .unwrap();
        assert_eq!(
            req.query_params.get("versionId").map(String::as_str),
            Some("v1")
        );
    }
}
