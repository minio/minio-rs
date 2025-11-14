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

use crate::impl_from_madmin_response;
use crate::impl_has_madmin_fields;
use crate::madmin::types::batch::{
    BatchJobResult, BatchJobType, GenerateBatchJobOpts, ListBatchJobsResult,
};
use crate::madmin::types::{FromMadminResponse, MadminRequest};
use crate::s3::error::{Error, ValidationErr};
use async_trait::async_trait;
use bytes::Bytes;
use http::HeaderMap;
use serde::{Deserialize, Serialize};

//TODO why are the function are all in this file instead of separate files like other responses?

// Constants for batch job templates
const BATCH_JOB_REPLICATE_TEMPLATE: &str = r#"replicate:
  apiVersion: v1
  source:
    type: TYPE
    bucket: BUCKET
    prefix: PREFIX
    endpoint: "http[s]://HOSTNAME:PORT"
    credentials:
      accessKey: ACCESS-KEY
      secretKey: SECRET-KEY
  target:
    type: TYPE
    bucket: BUCKET
    prefix: PREFIX
    endpoint: "http[s]://HOSTNAME:PORT"
    credentials:
      accessKey: ACCESS-KEY
      secretKey: SECRET-KEY
"#;

const BATCH_JOB_KEYROTATE_TEMPLATE: &str = r#"keyrotate:
  apiVersion: v1
  bucket: BUCKET
  prefix: PREFIX
  encryption:
    type: sse-s3
    key: <new-kms-key>
    context: <new-kms-key-context>
"#;

const BATCH_JOB_EXPIRE_TEMPLATE: &str = r#"expire:
  apiVersion: v1
  bucket: mybucket
  prefix: myprefix
  rules:
    - type: object
      name: NAME
      olderThan: 70h
      createdBefore: "2006-01-02T15:04:05.00Z"
"#;

#[derive(Debug, Clone)]
pub struct StartBatchJobResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(StartBatchJobResponse);
impl_has_madmin_fields!(StartBatchJobResponse);

impl StartBatchJobResponse {
    /// Returns the batch job result.
    pub fn result(&self) -> Result<BatchJobResult, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }
}

#[derive(Debug, Clone)]
pub struct ListBatchJobsResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(ListBatchJobsResponse);
impl_has_madmin_fields!(ListBatchJobsResponse);

impl ListBatchJobsResponse {
    /// Returns the list of batch jobs.
    pub fn jobs(&self) -> Result<ListBatchJobsResult, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }
}

#[derive(Debug, Clone)]
pub struct CancelBatchJobResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(CancelBatchJobResponse);
impl_has_madmin_fields!(CancelBatchJobResponse);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchJobStatusData {
    #[serde(rename = "LastMetric")]
    pub last_metric: serde_json::Value, // JobMetric is complex, using Value for now
}

#[derive(Debug, Clone)]
pub struct BatchJobStatusResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(BatchJobStatusResponse);
impl_has_madmin_fields!(BatchJobStatusResponse);

impl BatchJobStatusResponse {
    /// Returns the batch job status data.
    pub fn status(&self) -> Result<BatchJobStatusData, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }

    /// Returns the last metric value.
    pub fn last_metric(&self) -> Result<serde_json::Value, ValidationErr> {
        let status = self.status()?;
        Ok(status.last_metric)
    }
}

#[derive(Debug, Clone)]
pub struct DescribeBatchJobResponse {
    pub job_yaml: String,
}

#[async_trait]
impl FromMadminResponse for DescribeBatchJobResponse {
    async fn from_madmin_response(
        _request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let resp = response?;
        let body_bytes = resp
            .bytes()
            .await
            .map_err(crate::s3::error::ValidationErr::HttpError)?;
        let job_yaml = String::from_utf8(body_bytes.to_vec()).map_err(|e| {
            Error::Validation(crate::s3::error::ValidationErr::StrError {
                message: format!("Invalid UTF-8 in response: {}", e),
                source: Some(Box::new(e)),
            })
        })?;
        Ok(DescribeBatchJobResponse { job_yaml })
    }
}

#[derive(Debug, Clone)]
pub struct GenerateBatchJobResponse {
    pub template: String,
}

impl GenerateBatchJobResponse {
    pub fn from_opts(opts: &GenerateBatchJobOpts) -> Self {
        let template = match opts.job_type {
            BatchJobType::Replicate => BATCH_JOB_REPLICATE_TEMPLATE,
            BatchJobType::KeyRotate => BATCH_JOB_KEYROTATE_TEMPLATE,
            BatchJobType::Expire => BATCH_JOB_EXPIRE_TEMPLATE,
            BatchJobType::Catalog => "# Catalog template not available\n",
        };
        GenerateBatchJobResponse {
            template: template.to_string(),
        }
    }
}

#[async_trait]
impl FromMadminResponse for GenerateBatchJobResponse {
    async fn from_madmin_response(
        _request: MadminRequest,
        _response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        // This is a local operation, not an HTTP API call
        // The response is generated in the client method
        Err(Error::Validation(
            crate::s3::error::ValidationErr::StrError {
                message: "GenerateBatchJob is a local operation".to_string(),
                source: None,
            },
        ))
    }
}

#[derive(Debug, Clone)]
pub struct GenerateBatchJobV2Response {
    pub template: String,
    pub api_unavailable: bool,
}

#[async_trait]
impl FromMadminResponse for GenerateBatchJobV2Response {
    async fn from_madmin_response(
        _request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let resp = response?;
        let status = resp.status();

        // Check if API is unavailable
        if status == http::StatusCode::NOT_FOUND || status == http::StatusCode::UPGRADE_REQUIRED {
            return Ok(GenerateBatchJobV2Response {
                template: String::new(),
                api_unavailable: true,
            });
        }

        if !status.is_success() {
            return Err(Error::Validation(
                crate::s3::error::ValidationErr::StrError {
                    message: format!("HTTP {}", status),
                    source: None,
                },
            ));
        }

        let body_bytes = resp
            .bytes()
            .await
            .map_err(crate::s3::error::ValidationErr::HttpError)?;
        let template = String::from_utf8(body_bytes.to_vec()).map_err(|e| {
            Error::Validation(crate::s3::error::ValidationErr::StrError {
                message: format!("Invalid UTF-8 in response: {}", e),
                source: Some(Box::new(e)),
            })
        })?;

        Ok(GenerateBatchJobV2Response {
            template,
            api_unavailable: false,
        })
    }
}

#[derive(Debug, Clone)]
pub struct GetSupportedBatchJobTypesResponse {
    pub supported_types: Vec<BatchJobType>,
    pub api_unavailable: bool,
}

#[async_trait]
impl FromMadminResponse for GetSupportedBatchJobTypesResponse {
    async fn from_madmin_response(
        _request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let resp = response?;
        let status = resp.status();

        // Check if API is unavailable
        if status == http::StatusCode::NOT_FOUND || status == http::StatusCode::UPGRADE_REQUIRED {
            return Ok(GetSupportedBatchJobTypesResponse {
                supported_types: vec![],
                api_unavailable: true,
            });
        }

        if !status.is_success() {
            return Err(Error::Validation(
                crate::s3::error::ValidationErr::StrError {
                    message: format!("HTTP {}", status),
                    source: None,
                },
            ));
        }

        let body_bytes = resp
            .bytes()
            .await
            .map_err(crate::s3::error::ValidationErr::HttpError)?;
        let supported_types: Vec<BatchJobType> = serde_json::from_slice(&body_bytes)
            .map_err(|e| Error::Validation(crate::s3::error::ValidationErr::JsonError(e)))?;

        Ok(GetSupportedBatchJobTypesResponse {
            supported_types,
            api_unavailable: false,
        })
    }
}
