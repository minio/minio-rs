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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Describes batch job types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BatchJobType {
    Replicate,
    #[serde(rename = "keyrotate")]
    KeyRotate,
    Expire,
    Catalog,
}

/// Describes batch job statuses
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BatchJobStatusType {
    Completed,
    Failed,
    #[serde(rename = "in-progress")]
    InProgress,
    Unknown,
}

/// Result returned by StartBatchJob
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchJobResult {
    pub id: String,
    #[serde(rename = "type")]
    pub job_type: BatchJobType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    pub started: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<BatchJobStatusType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Contains entries for all current jobs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListBatchJobsResult {
    #[serde(default, deserialize_with = "deserialize_nullable_jobs")]
    pub jobs: Vec<BatchJobResult>,
}

/// Filtering parameters for listing batch jobs
#[derive(Debug, Clone, Default)]
pub struct ListBatchJobsFilter {
    pub by_job_type: Option<String>,
    pub by_bucket: Option<String>,
}

impl ListBatchJobsFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_job_type(mut self, job_type: String) -> Self {
        self.by_job_type = Some(job_type);
        self
    }

    pub fn with_bucket(mut self, bucket: String) -> Self {
        self.by_bucket = Some(bucket);
        self
    }
}

/// Options for generating a batch job template
#[derive(Debug, Clone)]
pub struct GenerateBatchJobOpts {
    pub job_type: BatchJobType,
}

impl GenerateBatchJobOpts {
    pub fn new(job_type: BatchJobType) -> Self {
        Self { job_type }
    }
}

/// Deserialize jobs list that may be null, defaulting to empty vec
fn deserialize_nullable_jobs<'de, D>(deserializer: D) -> Result<Vec<BatchJobResult>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<Vec<BatchJobResult>>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_job_type_serialization() {
        let job_type = BatchJobType::Replicate;
        let json = serde_json::to_string(&job_type).unwrap();
        assert_eq!(json, "\"replicate\"");

        let job_type = BatchJobType::KeyRotate;
        let json = serde_json::to_string(&job_type).unwrap();
        assert_eq!(json, "\"keyrotate\"");
    }

    #[test]
    fn test_batch_job_status_serialization() {
        let status = BatchJobStatusType::InProgress;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"in-progress\"");

        let status = BatchJobStatusType::Completed;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"completed\"");
    }

    #[test]
    fn test_list_batch_jobs_filter_builder() {
        let filter = ListBatchJobsFilter::new()
            .with_job_type("replicate".to_string())
            .with_bucket("mybucket".to_string());

        assert_eq!(filter.by_job_type, Some("replicate".to_string()));
        assert_eq!(filter.by_bucket, Some("mybucket".to_string()));
    }
}
