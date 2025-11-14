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

//! YAML serialization and deserialization support for inventory configurations.

use crate::s3::error::{Error, ValidationErr};
use crate::s3::inventory::JobDefinition;

/// Serializes a job definition to YAML string.
///
/// # Arguments
///
/// * `job` - The job definition to serialize
///
/// # Returns
///
/// A YAML-formatted string representation of the job definition.
///
/// # Errors
///
/// Returns an error if the job definition cannot be serialized.
pub fn serialize_job_definition(job: &JobDefinition) -> Result<String, Error> {
    job.validate().map_err(|e| {
        Error::Validation(ValidationErr::InvalidConfig {
            message: format!("Job validation failed: {e}"),
        })
    })?;
    serde_yaml::to_string(job).map_err(|e| {
        Error::Validation(ValidationErr::InvalidConfig {
            message: format!("Failed to serialize job definition: {e}"),
        })
    })
}

/// Deserializes a YAML string into a job definition.
///
/// # Arguments
///
/// * `yaml` - The YAML string to deserialize
///
/// # Returns
///
/// A validated job definition.
///
/// # Errors
///
/// Returns an error if the YAML cannot be parsed or validation fails.
pub fn deserialize_job_definition(yaml: &str) -> Result<JobDefinition, Error> {
    let job: JobDefinition = serde_yaml::from_str(yaml).map_err(|e| {
        Error::Validation(ValidationErr::InvalidConfig {
            message: format!("Failed to deserialize job definition: {e}"),
        })
    })?;
    job.validate().map_err(|e| {
        Error::Validation(ValidationErr::InvalidConfig {
            message: format!("Job validation failed: {e}"),
        })
    })?;
    Ok(job)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::inventory::{
        DestinationSpec, ModeSpec, OnOrOff, OutputFormat, Schedule, VersionsSpec,
    };

    #[test]
    fn test_serialize_job_definition() {
        let job = JobDefinition {
            api_version: "v1".to_string(),
            id: "test-job".to_string(),
            destination: DestinationSpec {
                bucket: "dest-bucket".to_string(),
                prefix: Some("prefix/".to_string()),
                format: OutputFormat::CSV,
                compression: OnOrOff::On,
                max_file_size_hint: None,
            },
            schedule: Schedule::Once,
            mode: ModeSpec::Fast,
            versions: VersionsSpec::Current,
            include_fields: vec![],
            filters: None,
        };

        let yaml = serialize_job_definition(&job).unwrap();
        assert!(yaml.contains("apiVersion: v1"));
        assert!(yaml.contains("id: test-job"));
        assert!(yaml.contains("bucket: dest-bucket"));
    }

    #[test]
    fn test_deserialize_job_definition() {
        let yaml = r#"
apiVersion: v1
id: test-job
destination:
  bucket: dest-bucket
  prefix: prefix/
  format: csv
  compression: on
schedule: once
mode: fast
versions: current
"#;

        let job = deserialize_job_definition(yaml).unwrap();
        assert_eq!(job.api_version, "v1");
        assert_eq!(job.id, "test-job");
        assert_eq!(job.destination.bucket, "dest-bucket");
        assert_eq!(job.schedule, Schedule::Once);
    }

    #[test]
    fn test_roundtrip() {
        let original = JobDefinition {
            api_version: "v1".to_string(),
            id: "roundtrip-test".to_string(),
            destination: DestinationSpec {
                bucket: "bucket".to_string(),
                prefix: None,
                format: OutputFormat::JSON,
                compression: OnOrOff::Off,
                max_file_size_hint: Some(1024 * 1024),
            },
            schedule: Schedule::Daily,
            mode: ModeSpec::Strict,
            versions: VersionsSpec::All,
            include_fields: vec![],
            filters: None,
        };

        let yaml = serialize_job_definition(&original).unwrap();
        let deserialized = deserialize_job_definition(&yaml).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_invalid_yaml() {
        let invalid_yaml = "this is not valid yaml: {[}";
        assert!(deserialize_job_definition(invalid_yaml).is_err());
    }

    #[test]
    fn test_validation_failure() {
        let invalid_yaml = r#"
apiVersion: v2
id: test-job
destination:
  bucket: dest-bucket
  format: csv
  compression: on
schedule: once
mode: fast
versions: current
"#;

        assert!(deserialize_job_definition(invalid_yaml).is_err());
    }
}
