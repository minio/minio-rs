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

use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::header_constants::{
    X_AMZ_MAX_PARTS, X_AMZ_OBJECT_ATTRIBUTES, X_AMZ_PART_NUMBER_MARKER,
};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::GetObjectAttributesResponse;
use crate::s3::sse::{Sse, SseCustomerKey};
use crate::s3::types::{BucketName, ObjectKey, Region, S3Api, S3Request, ToS3Request, VersionId};
use crate::s3::utils::{check_ssec, insert};
use http::Method;
use typed_builder::TypedBuilder;

const DEFAULT_OBJECT_ATTRIBUTES: &str = "ETag,Checksum,ObjectParts,StorageClass,ObjectSize";

fn build_object_attributes_headers(
    extra_headers: Option<Multimap>,
    max_parts: Option<u32>,
    part_number_marker: Option<u32>,
    ssec: Option<&SseCustomerKey>,
) -> Multimap {
    let mut headers: Multimap = extra_headers.unwrap_or_default();
    headers.add(X_AMZ_OBJECT_ATTRIBUTES, DEFAULT_OBJECT_ATTRIBUTES);

    if let Some(v) = max_parts {
        headers.add(X_AMZ_MAX_PARTS, v.to_string());
    }

    if let Some(v) = part_number_marker {
        headers.add(X_AMZ_PART_NUMBER_MARKER, v.to_string());
    }

    if let Some(v) = ssec {
        headers.add_multimap(v.headers());
    }

    headers
}

/// Argument builder for the [`GetObjectAttributes`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::get_object_attributes`](crate::s3::client::MinioClient::get_object_attributes) method.
#[derive(Debug, Clone, TypedBuilder)]
pub struct ObjectAttributes {
    #[builder(!default)] // force required
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<Region>,
    #[builder(setter(into))]
    bucket: BucketName,
    #[builder(setter(into))]
    object: ObjectKey,
    #[builder(default)]
    version_id: Option<String>,
    #[builder(default, setter(into))]
    max_parts: Option<u32>,
    #[builder(default, setter(into))]
    part_number_marker: Option<u32>,
    #[builder(default, setter(into))]
    ssec: Option<SseCustomerKey>,
}

/// Builder type alias for [`ObjectAttributes`].
///
/// Constructed via [`ObjectAttributes::builder()`](ObjectAttributes::builder) and used to build an [`ObjectAttributes`] instance.
pub type GetObjectAttributesBldr = ObjectAttributesBuilder<(
    (MinioClient,),
    (),
    (),
    (),
    (BucketName,),
    (ObjectKey,),
    (),
    (),
    (),
    (),
)>;

impl S3Api for ObjectAttributes {
    type S3Response = GetObjectAttributesResponse;
}

impl ToS3Request for ObjectAttributes {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_ssec(&self.ssec, &self.client)?;

        let headers: Multimap = build_object_attributes_headers(
            self.extra_headers,
            self.max_parts,
            self.part_number_marker,
            self.ssec.as_ref(),
        );

        let mut query_params: Multimap = insert(self.extra_query_params, "attributes");
        let version_id = self
            .version_id
            .map(|v| VersionId::new(v).expect("valid version id"));
        query_params.add_version(version_id);

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::GET)
            .region(self.region)
            .bucket(self.bucket)
            .object(self.object)
            .query_params(query_params)
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
    fn request_sets_attributes_query() {
        let req = test_client()
            .get_object_attributes("test-bucket", "test-object")
            .unwrap()
            .build()
            .to_s3request()
            .unwrap();

        assert!(req.query_params.contains_key("attributes"));
    }

    #[test]
    fn headers_set_attributes_only_by_default() {
        let headers = build_object_attributes_headers(None, None, None, None);

        assert_eq!(
            headers.get(X_AMZ_OBJECT_ATTRIBUTES).map(String::as_str),
            Some(DEFAULT_OBJECT_ATTRIBUTES)
        );
        assert!(headers.get(X_AMZ_MAX_PARTS).is_none());
        assert!(headers.get(X_AMZ_PART_NUMBER_MARKER).is_none());
    }

    #[test]
    fn headers_set_part_pagination() {
        let headers = build_object_attributes_headers(None, Some(100), Some(5), None);

        assert_eq!(
            headers.get(X_AMZ_MAX_PARTS).map(String::as_str),
            Some("100")
        );
        assert_eq!(
            headers.get(X_AMZ_PART_NUMBER_MARKER).map(String::as_str),
            Some("5")
        );
    }
}
