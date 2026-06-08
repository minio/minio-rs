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

use crate::s3::builders::{QOSConfig, QOSRule};
use crate::s3::error::ValidationErr;
use crate::s3::response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::S3Request;
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;

/// Counter metric tracking values over rolling windows.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CounterMetric {
    #[serde(rename = "last1m", default)]
    pub last1m: u64,
    #[serde(rename = "last1hr", default)]
    pub last1hr: u64,
    #[serde(rename = "total", default)]
    pub total: u64,
}

/// Metric for a single QoS rule per bucket.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QOSMetric {
    #[serde(rename = "apiName")]
    pub api_name: String,
    #[serde(rename = "rule")]
    pub rule: QOSRule,
    #[serde(rename = "totals", default)]
    pub totals: CounterMetric,
    #[serde(rename = "throttleCount", default)]
    pub throttled: CounterMetric,
    #[serde(rename = "exceededRateLimitCount", default)]
    pub exceeded_rate_limit: CounterMetric,
    #[serde(rename = "clientDisconnectCount", default)]
    pub client_disconn_count: CounterMetric,
    #[serde(rename = "reqTimeoutCount", default)]
    pub req_timeout_count: CounterMetric,
}

/// QoS stats for a bucket on a single node.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QOSNodeStats {
    #[serde(rename = "stats", default)]
    pub stats: Vec<QOSMetric>,
    #[serde(rename = "node")]
    pub node_name: String,
}

/// Response of the [`get_bucket_qos`](crate::s3::client::MinioClient::get_bucket_qos) API call (MinIO extension).
#[derive(Clone, Debug)]
pub struct GetBucketQOSResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetBucketQOSResponse);
impl_has_s3fields!(GetBucketQOSResponse);

impl HasBucket for GetBucketQOSResponse {}
impl HasRegion for GetBucketQOSResponse {}

impl GetBucketQOSResponse {
    /// Returns the QoS configuration parsed from the YAML response body.
    pub fn config(&self) -> Result<QOSConfig, ValidationErr> {
        if self.body().is_empty() {
            return Ok(QOSConfig::default());
        }
        serde_yaml::from_slice(self.body()).map_err(|e| ValidationErr::InvalidYaml {
            message: e.to_string(),
        })
    }
}

/// Response of the [`set_bucket_qos`](crate::s3::client::MinioClient::set_bucket_qos) API call (MinIO extension).
#[derive(Clone, Debug)]
pub struct SetBucketQOSResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(SetBucketQOSResponse);
impl_has_s3fields!(SetBucketQOSResponse);

impl HasBucket for SetBucketQOSResponse {}
impl HasRegion for SetBucketQOSResponse {}

/// Response of the [`get_bucket_qos_metrics`](crate::s3::client::MinioClient::get_bucket_qos_metrics) API call (MinIO extension).
#[derive(Clone, Debug)]
pub struct GetBucketQOSMetricsResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetBucketQOSMetricsResponse);
impl_has_s3fields!(GetBucketQOSMetricsResponse);

impl HasBucket for GetBucketQOSMetricsResponse {}
impl HasRegion for GetBucketQOSMetricsResponse {}

impl GetBucketQOSMetricsResponse {
    /// Returns the per-node QoS metrics parsed from the JSON response body.
    pub fn metrics(&self) -> Result<Vec<QOSNodeStats>, ValidationErr> {
        Ok(serde_json::from_slice(self.body())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qos_metrics_json_wire_format() {
        let json = r#"[
          {
            "node": "node-1",
            "stats": [
              {
                "apiName": "PutObject",
                "rule": {
                  "id": "rule-1",
                  "priority": 10,
                  "objectPrefix": "logs/",
                  "api": "PutObject",
                  "rate": 100,
                  "burst": 200,
                  "limit": "rps"
                },
                "totals": {"last1m": 1, "last1hr": 2, "total": 3},
                "throttleCount": {"last1m": 0, "last1hr": 0, "total": 5},
                "exceededRateLimitCount": {"last1m": 0, "last1hr": 0, "total": 0},
                "clientDisconnectCount": {"last1m": 0, "last1hr": 0, "total": 0},
                "reqTimeoutCount": {"last1m": 0, "last1hr": 0, "total": 0}
              }
            ]
          }
        ]"#;

        let stats: Vec<QOSNodeStats> = serde_json::from_str(json).unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].node_name, "node-1");
        assert_eq!(stats[0].stats.len(), 1);
        let m = &stats[0].stats[0];
        assert_eq!(m.api_name, "PutObject");
        assert_eq!(m.rule.id, "rule-1");
        assert_eq!(m.rule.object_prefix, "logs/");
        assert_eq!(m.totals.total, 3);
        assert_eq!(m.throttled.total, 5);
    }

    #[test]
    fn test_qos_metric_rule_capitalized_aliases() {
        let json = r#"{
          "apiName": "GetObject",
          "rule": {
            "ID": "r2",
            "Priority": 1,
            "ObjectPrefix": "data/",
            "API": "GetObject",
            "Rate": 50,
            "Burst": 0,
            "Limit": "concurrency"
          },
          "totals": {},
          "throttleCount": {},
          "exceededRateLimitCount": {},
          "clientDisconnectCount": {},
          "reqTimeoutCount": {}
        }"#;

        let m: QOSMetric = serde_json::from_str(json).unwrap();
        assert_eq!(m.rule.id, "r2");
        assert_eq!(m.rule.object_prefix, "data/");
        assert_eq!(m.rule.limit, "concurrency");
    }
}
