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
use crate::s3::builders::{BucketCommon, BucketCommonBuilder};
use crate::s3::error::ValidationErr;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::{
    GetBucketQOSMetricsResponse, GetBucketQOSResponse, SetBucketQOSResponse,
};
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{BucketName, Region, S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, insert};
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Current version of the QoS configuration.
pub const QOS_CONFIG_VERSION_CURRENT: &str = "v1";

/// A single QoS rule within a [`QOSConfig`].
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QOSRule {
    #[serde(rename = "id", alias = "ID")]
    pub id: String,
    #[serde(
        rename = "label",
        alias = "Label",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub label: String,
    #[serde(rename = "priority", alias = "Priority")]
    pub priority: i64,
    #[serde(rename = "objectPrefix", alias = "ObjectPrefix")]
    pub object_prefix: String,
    #[serde(rename = "api", alias = "API")]
    pub api: String,
    #[serde(rename = "rate", alias = "Rate")]
    pub rate: i64,
    #[serde(rename = "burst", alias = "Burst")]
    pub burst: i64,
    #[serde(rename = "limit", alias = "Limit")]
    pub limit: String,
}

/// Quality of Service (QoS) configuration for a bucket.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QOSConfig {
    #[serde(rename = "version", alias = "Version")]
    pub version: String,
    #[serde(rename = "rules", alias = "Rules", default)]
    pub rules: Vec<QOSRule>,
}

impl Default for QOSConfig {
    fn default() -> Self {
        Self {
            version: QOS_CONFIG_VERSION_CURRENT.to_string(),
            rules: Vec::new(),
        }
    }
}

impl QOSConfig {
    /// Creates a new empty QoS configuration at the current version.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Argument builder for the `GetBucketQOS` operation (MinIO extension).
///
/// This is a MinIO-specific extension that retrieves the Quality of Service
/// configuration for a bucket. There is no AWS S3 equivalent.
/// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
///
/// This struct constructs the parameters required for the [`Client::get_bucket_qos`](crate::s3::client::MinioClient::get_bucket_qos) method.
pub type GetBucketQOS = BucketCommon<GetBucketQOSPhantomData>;

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct GetBucketQOSPhantomData;

impl S3Api for GetBucketQOS {
    type S3Response = GetBucketQOSResponse;
}

/// Builder type for [`GetBucketQOS`] returned by [`MinioClient::get_bucket_qos`](crate::s3::client::MinioClient::get_bucket_qos).
pub type GetBucketQOSBldr =
    BucketCommonBuilder<GetBucketQOSPhantomData, ((MinioClient,), (), (), (), (BucketName,), ())>;

impl ToS3Request for GetBucketQOS {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::GET)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(insert(self.extra_query_params, "qos"))
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

/// Argument builder for the `SetBucketQOS` operation (MinIO extension).
///
/// This is a MinIO-specific extension that applies a Quality of Service
/// configuration to a bucket. There is no AWS S3 equivalent.
/// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
///
/// This struct constructs the parameters required for the [`Client::set_bucket_qos`](crate::s3::client::MinioClient::set_bucket_qos) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct SetBucketQOS {
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
    #[builder(default)]
    qos_config: QOSConfig,
}

/// Builder type for [`SetBucketQOS`] returned by [`MinioClient::set_bucket_qos`](crate::s3::client::MinioClient::set_bucket_qos).
pub type SetBucketQOSBldr = SetBucketQOSBuilder<((MinioClient,), (), (), (), (BucketName,), ())>;

impl S3Api for SetBucketQOS {
    type S3Response = SetBucketQOSResponse;
}

impl ToS3Request for SetBucketQOS {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        let data =
            serde_yaml::to_string(&self.qos_config).map_err(|e| ValidationErr::InvalidYaml {
                message: e.to_string(),
            })?;
        let body = Arc::new(SegmentedBytes::from(Bytes::from(data)));

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::PUT)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(insert(self.extra_query_params, "qos"))
            .headers(self.extra_headers.unwrap_or_default())
            .body(body)
            .build())
    }
}

/// Argument builder for the `GetBucketQOSMetrics` operation (MinIO extension).
///
/// This is a MinIO-specific extension that retrieves Quality of Service
/// metrics for a bucket. There is no AWS S3 equivalent.
/// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
///
/// This struct constructs the parameters required for the [`Client::get_bucket_qos_metrics`](crate::s3::client::MinioClient::get_bucket_qos_metrics) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct GetBucketQOSMetrics {
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
    node: Option<String>,
}

/// Builder type for [`GetBucketQOSMetrics`] returned by [`MinioClient::get_bucket_qos_metrics`](crate::s3::client::MinioClient::get_bucket_qos_metrics).
pub type GetBucketQOSMetricsBldr =
    GetBucketQOSMetricsBuilder<((MinioClient,), (), (), (), (BucketName,), ())>;

impl S3Api for GetBucketQOSMetrics {
    type S3Response = GetBucketQOSMetricsResponse;
}

impl ToS3Request for GetBucketQOSMetrics {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        let mut query_params = insert(self.extra_query_params, "qos-metrics");
        if let Some(node) = self.node.filter(|n| !n.is_empty()) {
            query_params.add("node", node);
        }

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

    fn sample_config() -> QOSConfig {
        QOSConfig {
            version: "v1".to_string(),
            rules: vec![QOSRule {
                id: "rule-1".to_string(),
                label: "ingest".to_string(),
                priority: 10,
                object_prefix: "logs/".to_string(),
                api: "PutObject".to_string(),
                rate: 100,
                burst: 200,
                limit: "rps".to_string(),
            }],
        }
    }

    #[test]
    fn test_qos_config_default() {
        let cfg = QOSConfig::new();
        assert_eq!(cfg.version, QOS_CONFIG_VERSION_CURRENT);
        assert!(cfg.rules.is_empty());
    }

    #[test]
    fn test_qos_config_yaml_round_trip() {
        let cfg = sample_config();
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: QOSConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(cfg, parsed);
    }

    #[test]
    fn test_qos_config_yaml_wire_format() {
        let yaml = "\
version: v1
rules:
  - id: rule-1
    label: ingest
    priority: 10
    objectPrefix: logs/
    api: PutObject
    rate: 100
    burst: 200
    limit: rps
";
        let parsed: QOSConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed, sample_config());
    }

    #[test]
    fn test_qos_config_yaml_omits_empty_label() {
        let mut cfg = sample_config();
        cfg.rules[0].label = String::new();
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        assert!(!yaml.contains("label"));
    }

    fn test_client() -> MinioClient {
        use crate::s3::creds::StaticProvider;
        use crate::s3::http::BaseUrl;
        let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
        let provider = StaticProvider::new("minioadmin", "minioadmin", None);
        MinioClient::new(base_url, Some(provider), None, None).unwrap()
    }

    #[test]
    fn get_bucket_qos_request() {
        let req = test_client()
            .get_bucket_qos("test-bucket")
            .unwrap()
            .build()
            .to_s3request()
            .unwrap();
        assert!(req.query_params.contains_key("qos"));
        assert_eq!(req.bucket.as_ref().map(|b| b.as_ref()), Some("test-bucket"));
    }

    #[test]
    fn set_bucket_qos_request() {
        let req = test_client()
            .set_bucket_qos("test-bucket")
            .unwrap()
            .qos_config(sample_config())
            .build()
            .to_s3request()
            .unwrap();
        assert!(req.query_params.contains_key("qos"));
        assert_eq!(req.bucket.as_ref().map(|b| b.as_ref()), Some("test-bucket"));
    }

    #[test]
    fn get_bucket_qos_metrics_request_without_node() {
        let req = test_client()
            .get_bucket_qos_metrics("test-bucket")
            .unwrap()
            .build()
            .to_s3request()
            .unwrap();
        assert!(req.query_params.contains_key("qos-metrics"));
        assert!(!req.query_params.contains_key("node"));
    }

    #[test]
    fn get_bucket_qos_metrics_request_with_node() {
        let req = test_client()
            .get_bucket_qos_metrics("test-bucket")
            .unwrap()
            .node(Some(String::from("node-1")))
            .build()
            .to_s3request()
            .unwrap();
        assert!(req.query_params.contains_key("qos-metrics"));
        assert_eq!(
            req.query_params.get("node").map(String::as_str),
            Some("node-1")
        );
    }

    #[test]
    fn get_bucket_qos_metrics_empty_node_skipped() {
        let req = test_client()
            .get_bucket_qos_metrics("test-bucket")
            .unwrap()
            .node(Some(String::new()))
            .build()
            .to_s3request()
            .unwrap();
        assert!(!req.query_params.contains_key("node"));
    }
}
