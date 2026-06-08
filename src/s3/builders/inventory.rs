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

use crate::s3::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::header_constants::CONTENT_MD5;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::{
    DeleteBucketInventoryConfigurationResponse, GenerateInventoryConfigYamlResponse,
    GetBucketInventoryConfigurationResponse, GetBucketInventoryJobStatusResponse,
    ListBucketInventoryConfigurationsResponse, PutBucketInventoryConfigurationResponse,
};
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{BucketName, Region, S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, insert, md5sum_hash};
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

const INVENTORY: &str = "minio-inventory";

fn empty_id_error() -> ValidationErr {
    ValidationErr::StrError {
        message: "inventory ID cannot be empty".into(),
        source: None,
    }
}

/// Argument builder for the `GenerateInventoryConfigYAML` operation (MinIO extension).
///
/// This is a MinIO-specific extension that generates a YAML template for an
/// inventory configuration. There is no AWS S3 equivalent.
/// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
///
/// This struct constructs the parameters required for the
/// [`Client::generate_inventory_config_yaml`](crate::s3::client::MinioClient::generate_inventory_config_yaml) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct GenerateInventoryConfigYaml {
    #[builder(!default)]
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<Region>,
    #[builder(!default)]
    bucket: BucketName,
    #[builder(setter(into))]
    id: String,
}

/// Builder type for [`GenerateInventoryConfigYaml`] returned by
/// [`MinioClient::generate_inventory_config_yaml`](crate::s3::client::MinioClient::generate_inventory_config_yaml).
pub type GenerateInventoryConfigYamlBldr =
    GenerateInventoryConfigYamlBuilder<((MinioClient,), (), (), (), (BucketName,), (String,))>;

impl S3Api for GenerateInventoryConfigYaml {
    type S3Response = GenerateInventoryConfigYamlResponse;
}

impl ToS3Request for GenerateInventoryConfigYaml {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;
        if self.id.is_empty() {
            return Err(empty_id_error());
        }

        let mut query_params = insert(self.extra_query_params, INVENTORY);
        query_params.add("generate", "");
        query_params.add("id", self.id);

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::GET)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

/// Argument builder for the `PutBucketInventoryConfiguration` operation (MinIO extension).
///
/// This is a MinIO-specific extension that creates or updates an inventory
/// configuration for a bucket. There is no AWS S3 equivalent.
/// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
///
/// This struct constructs the parameters required for the
/// [`Client::put_bucket_inventory_configuration`](crate::s3::client::MinioClient::put_bucket_inventory_configuration) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct PutBucketInventoryConfiguration {
    #[builder(!default)]
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<Region>,
    #[builder(!default)]
    bucket: BucketName,
    #[builder(setter(into))]
    id: String,
    #[builder(setter(into))]
    yaml_def: String,
}

/// Builder type for [`PutBucketInventoryConfiguration`] returned by
/// [`MinioClient::put_bucket_inventory_configuration`](crate::s3::client::MinioClient::put_bucket_inventory_configuration).
pub type PutBucketInventoryConfigurationBldr = PutBucketInventoryConfigurationBuilder<(
    (MinioClient,),
    (),
    (),
    (),
    (BucketName,),
    (String,),
    (String,),
)>;

impl S3Api for PutBucketInventoryConfiguration {
    type S3Response = PutBucketInventoryConfigurationResponse;
}

impl ToS3Request for PutBucketInventoryConfiguration {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;
        if self.id.is_empty() {
            return Err(empty_id_error());
        }
        if self.yaml_def.is_empty() {
            return Err(ValidationErr::StrError {
                message: "YAML definition cannot be empty".into(),
                source: None,
            });
        }

        let mut query_params = insert(self.extra_query_params, INVENTORY);
        query_params.add("id", self.id);

        let bytes = Bytes::from(self.yaml_def);
        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        headers.add(CONTENT_MD5, md5sum_hash(bytes.as_ref()));
        let body = Arc::new(SegmentedBytes::from(bytes));

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::PUT)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(query_params)
            .headers(headers)
            .body(body)
            .build())
    }
}

/// Argument builder for the `GetBucketInventoryConfiguration` operation (MinIO extension).
///
/// This is a MinIO-specific extension that retrieves an inventory configuration
/// for a bucket. There is no AWS S3 equivalent.
/// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
///
/// This struct constructs the parameters required for the
/// [`Client::get_bucket_inventory_configuration`](crate::s3::client::MinioClient::get_bucket_inventory_configuration) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct GetBucketInventoryConfiguration {
    #[builder(!default)]
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<Region>,
    #[builder(!default)]
    bucket: BucketName,
    #[builder(setter(into))]
    id: String,
}

/// Builder type for [`GetBucketInventoryConfiguration`] returned by
/// [`MinioClient::get_bucket_inventory_configuration`](crate::s3::client::MinioClient::get_bucket_inventory_configuration).
pub type GetBucketInventoryConfigurationBldr =
    GetBucketInventoryConfigurationBuilder<((MinioClient,), (), (), (), (BucketName,), (String,))>;

impl S3Api for GetBucketInventoryConfiguration {
    type S3Response = GetBucketInventoryConfigurationResponse;
}

impl ToS3Request for GetBucketInventoryConfiguration {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;
        if self.id.is_empty() {
            return Err(empty_id_error());
        }

        let mut query_params = insert(self.extra_query_params, INVENTORY);
        query_params.add("id", self.id);

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::GET)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

/// Argument builder for the `DeleteBucketInventoryConfiguration` operation (MinIO extension).
///
/// This is a MinIO-specific extension that deletes an inventory configuration
/// from a bucket. There is no AWS S3 equivalent.
/// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
///
/// This struct constructs the parameters required for the
/// [`Client::delete_bucket_inventory_configuration`](crate::s3::client::MinioClient::delete_bucket_inventory_configuration) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct DeleteBucketInventoryConfiguration {
    #[builder(!default)]
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<Region>,
    #[builder(!default)]
    bucket: BucketName,
    #[builder(setter(into))]
    id: String,
}

/// Builder type for [`DeleteBucketInventoryConfiguration`] returned by
/// [`MinioClient::delete_bucket_inventory_configuration`](crate::s3::client::MinioClient::delete_bucket_inventory_configuration).
pub type DeleteBucketInventoryConfigurationBldr = DeleteBucketInventoryConfigurationBuilder<(
    (MinioClient,),
    (),
    (),
    (),
    (BucketName,),
    (String,),
)>;

impl S3Api for DeleteBucketInventoryConfiguration {
    type S3Response = DeleteBucketInventoryConfigurationResponse;
}

impl ToS3Request for DeleteBucketInventoryConfiguration {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;
        if self.id.is_empty() {
            return Err(empty_id_error());
        }

        let mut query_params = insert(self.extra_query_params, INVENTORY);
        query_params.add("id", self.id);

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::DELETE)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

/// Argument builder for the `ListBucketInventoryConfigurations` operation (MinIO extension).
///
/// This is a MinIO-specific extension that lists up to 100 inventory
/// configurations for a bucket. There is no AWS S3 equivalent.
/// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
///
/// This struct constructs the parameters required for the
/// [`Client::list_bucket_inventory_configurations`](crate::s3::client::MinioClient::list_bucket_inventory_configurations) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct ListBucketInventoryConfigurations {
    #[builder(!default)]
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<Region>,
    #[builder(!default)]
    bucket: BucketName,
    #[builder(default, setter(into))]
    continuation_token: String,
}

/// Builder type for [`ListBucketInventoryConfigurations`] returned by
/// [`MinioClient::list_bucket_inventory_configurations`](crate::s3::client::MinioClient::list_bucket_inventory_configurations).
pub type ListBucketInventoryConfigurationsBldr =
    ListBucketInventoryConfigurationsBuilder<((MinioClient,), (), (), (), (BucketName,), ())>;

impl S3Api for ListBucketInventoryConfigurations {
    type S3Response = ListBucketInventoryConfigurationsResponse;
}

impl ToS3Request for ListBucketInventoryConfigurations {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        let mut query_params = insert(self.extra_query_params, INVENTORY);
        query_params.add("continuation-token", self.continuation_token);

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::GET)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

/// Argument builder for the `GetBucketInventoryJobStatus` operation (MinIO extension).
///
/// This is a MinIO-specific extension that retrieves the status of an inventory
/// job for a bucket. There is no AWS S3 equivalent.
/// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
///
/// This struct constructs the parameters required for the
/// [`Client::get_bucket_inventory_job_status`](crate::s3::client::MinioClient::get_bucket_inventory_job_status) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct GetBucketInventoryJobStatus {
    #[builder(!default)]
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<Region>,
    #[builder(!default)]
    bucket: BucketName,
    #[builder(setter(into))]
    id: String,
}

/// Builder type for [`GetBucketInventoryJobStatus`] returned by
/// [`MinioClient::get_bucket_inventory_job_status`](crate::s3::client::MinioClient::get_bucket_inventory_job_status).
pub type GetBucketInventoryJobStatusBldr =
    GetBucketInventoryJobStatusBuilder<((MinioClient,), (), (), (), (BucketName,), (String,))>;

impl S3Api for GetBucketInventoryJobStatus {
    type S3Response = GetBucketInventoryJobStatusResponse;
}

impl ToS3Request for GetBucketInventoryJobStatus {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;
        if self.id.is_empty() {
            return Err(empty_id_error());
        }

        let mut query_params = insert(self.extra_query_params, INVENTORY);
        query_params.add("id", self.id);
        query_params.add("status", "");

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::GET)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::creds::StaticProvider;
    use crate::s3::http::BaseUrl;

    fn dummy_client() -> MinioClient {
        let base_url: BaseUrl = "http://localhost:9000".parse().unwrap();
        let provider = StaticProvider::new("minioadmin", "minioadmin", None);
        MinioClient::new(base_url, Some(provider), None, None).unwrap()
    }

    fn bucket() -> BucketName {
        BucketName::new("mybucket").unwrap()
    }

    #[test]
    fn generate_request_path_and_query() {
        let req = GenerateInventoryConfigYaml::builder()
            .client(dummy_client())
            .bucket(bucket())
            .id("inv-1")
            .build()
            .to_s3request()
            .unwrap();

        assert_eq!(
            req.query_params.get(INVENTORY).map(String::as_str),
            Some("")
        );
        assert_eq!(
            req.query_params.get("generate").map(String::as_str),
            Some("")
        );
        assert_eq!(
            req.query_params.get("id").map(String::as_str),
            Some("inv-1")
        );
    }

    #[test]
    fn generate_empty_id_errors() {
        let err = GenerateInventoryConfigYaml::builder()
            .client(dummy_client())
            .bucket(bucket())
            .id("")
            .build()
            .to_s3request();
        assert!(err.is_err());
    }

    #[test]
    fn put_request_query_and_body() {
        let req = PutBucketInventoryConfiguration::builder()
            .client(dummy_client())
            .bucket(bucket())
            .id("inv-1")
            .yaml_def("id: inv-1\n")
            .build()
            .to_s3request()
            .unwrap();

        assert_eq!(
            req.query_params.get(INVENTORY).map(String::as_str),
            Some("")
        );
        assert_eq!(
            req.query_params.get("id").map(String::as_str),
            Some("inv-1")
        );
        assert!(req.query_params.get("generate").is_none());
    }

    #[test]
    fn put_empty_yaml_errors() {
        let err = PutBucketInventoryConfiguration::builder()
            .client(dummy_client())
            .bucket(bucket())
            .id("inv-1")
            .yaml_def("")
            .build()
            .to_s3request();
        assert!(err.is_err());
    }

    #[test]
    fn get_request_query() {
        let req = GetBucketInventoryConfiguration::builder()
            .client(dummy_client())
            .bucket(bucket())
            .id("inv-1")
            .build()
            .to_s3request()
            .unwrap();

        assert_eq!(
            req.query_params.get(INVENTORY).map(String::as_str),
            Some("")
        );
        assert_eq!(
            req.query_params.get("id").map(String::as_str),
            Some("inv-1")
        );
        assert!(req.query_params.get("status").is_none());
    }

    #[test]
    fn delete_request_query() {
        let req = DeleteBucketInventoryConfiguration::builder()
            .client(dummy_client())
            .bucket(bucket())
            .id("inv-1")
            .build()
            .to_s3request()
            .unwrap();

        assert_eq!(
            req.query_params.get(INVENTORY).map(String::as_str),
            Some("")
        );
        assert_eq!(
            req.query_params.get("id").map(String::as_str),
            Some("inv-1")
        );
    }

    #[test]
    fn list_request_sets_continuation_token_even_when_empty() {
        let req = ListBucketInventoryConfigurations::builder()
            .client(dummy_client())
            .bucket(bucket())
            .build()
            .to_s3request()
            .unwrap();

        assert_eq!(
            req.query_params.get(INVENTORY).map(String::as_str),
            Some("")
        );
        assert_eq!(
            req.query_params
                .get("continuation-token")
                .map(String::as_str),
            Some("")
        );
    }

    #[test]
    fn list_request_with_continuation_token() {
        let req = ListBucketInventoryConfigurations::builder()
            .client(dummy_client())
            .bucket(bucket())
            .continuation_token("tok-2")
            .build()
            .to_s3request()
            .unwrap();

        assert_eq!(
            req.query_params
                .get("continuation-token")
                .map(String::as_str),
            Some("tok-2")
        );
    }

    #[test]
    fn job_status_request_query() {
        let req = GetBucketInventoryJobStatus::builder()
            .client(dummy_client())
            .bucket(bucket())
            .id("inv-1")
            .build()
            .to_s3request()
            .unwrap();

        assert_eq!(
            req.query_params.get(INVENTORY).map(String::as_str),
            Some("")
        );
        assert_eq!(
            req.query_params.get("id").map(String::as_str),
            Some("inv-1")
        );
        assert_eq!(req.query_params.get("status").map(String::as_str), Some(""));
    }
}
