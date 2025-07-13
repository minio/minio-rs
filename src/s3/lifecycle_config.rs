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

use crate::s3::error::{MinioError, Result};
use crate::s3::types::Filter;
use crate::s3::utils::to_iso8601utc;
use xmltree::Element;

#[derive(PartialEq, Clone, Debug, Default)]
/// Lifecycle configuration
pub struct LifecycleConfig {
    pub rules: Vec<LifecycleRule>,
}

impl LifecycleConfig {
    pub fn from_xml(root: &Element) -> Result<LifecycleConfig> {
        let mut config = LifecycleConfig { rules: Vec::new() };

        // Process all Rule elements in the XML
        for rule_elem in root.children.iter().filter_map(|c| c.as_element()) {
            if rule_elem.name == "Rule" {
                config.rules.push(LifecycleRule::from_xml(rule_elem)?);
            }
        }

        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        // Skip validation if empty
        if self.rules.is_empty() {
            return Ok(());
        }

        for rule in &self.rules {
            rule.validate()?;
        }

        Ok(())
    }

    pub fn empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<LifecycleConfiguration>");

        for rule in &self.rules {
            data.push_str("<Rule>");

            // ID should come earlier in XML based on Go ordering
            if !rule.id.is_empty() {
                data.push_str("<ID>");
                data.push_str(&rule.id);
                data.push_str("</ID>");
            }

            // Status comes next
            data.push_str("<Status>");
            if rule.status {
                data.push_str("Enabled");
            } else {
                data.push_str("Disabled");
            }
            data.push_str("</Status>");

            // Filter
            data.push_str(&rule.filter.to_xml());

            // AbortIncompleteMultipartUpload
            if let Some(days) = rule.abort_incomplete_multipart_upload_days_after_initiation {
                data.push_str("<AbortIncompleteMultipartUpload><DaysAfterInitiation>");
                data.push_str(&days.to_string());
                data.push_str("</DaysAfterInitiation></AbortIncompleteMultipartUpload>");
            }

            // Expiration
            let has_expiration = rule.expiration_date.is_some()
                || rule.expiration_days.is_some()
                || rule.expiration_expired_object_delete_marker.is_some()
                || rule.expiration_expired_object_all_versions.is_some();

            if has_expiration {
                data.push_str("<Expiration>");
                if let Some(date) = rule.expiration_date {
                    data.push_str("<Date>");
                    data.push_str(&to_iso8601utc(date));
                    data.push_str("</Date>");
                }
                if let Some(days) = rule.expiration_days {
                    data.push_str("<Days>");
                    data.push_str(&days.to_string());
                    data.push_str("</Days>");
                }
                if let Some(delete_marker) = rule.expiration_expired_object_delete_marker {
                    if delete_marker {
                        data.push_str(
                            "<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>",
                        );
                    }
                }
                if let Some(delete_all) = rule.expiration_expired_object_all_versions {
                    if delete_all {
                        data.push_str("<ExpiredObjectAllVersions>true</ExpiredObjectAllVersions>");
                    }
                }
                data.push_str("</Expiration>");
            }

            // DelMarkerExpiration
            if let Some(days) = rule.del_marker_expiration_days {
                data.push_str("<DelMarkerExpiration><Days>");
                data.push_str(&days.to_string());
                data.push_str("</Days></DelMarkerExpiration>");
            }

            // AllVersionsExpiration
            if let Some(days) = rule.all_versions_expiration_days {
                data.push_str("<AllVersionsExpiration><Days>");
                data.push_str(&days.to_string());
                data.push_str("</Days>");

                if let Some(delete_marker) = rule.all_versions_expiration_delete_marker {
                    if delete_marker {
                        data.push_str("<DeleteMarker>true</DeleteMarker>");
                    }
                }

                data.push_str("</AllVersionsExpiration>");
            }

            // NoncurrentVersionExpiration
            if let Some(days) = rule.noncurrent_version_expiration_noncurrent_days {
                data.push_str("<NoncurrentVersionExpiration><NoncurrentDays>");
                data.push_str(&days.to_string());
                data.push_str("</NoncurrentDays>");

                if let Some(versions) = rule.noncurrent_version_expiration_newer_versions {
                    data.push_str("<NewerNoncurrentVersions>");
                    data.push_str(&versions.to_string());
                    data.push_str("</NewerNoncurrentVersions>");
                }

                data.push_str("</NoncurrentVersionExpiration>");
            }

            // NoncurrentVersionTransition
            let has_noncurrent_transition =
                rule.noncurrent_version_transition_noncurrent_days.is_some()
                    || rule.noncurrent_version_transition_storage_class.is_some()
                    || rule.noncurrent_version_transition_newer_versions.is_some();

            if has_noncurrent_transition {
                data.push_str("<NoncurrentVersionTransition>");

                if let Some(days) = rule.noncurrent_version_transition_noncurrent_days {
                    data.push_str("<NoncurrentDays>");
                    data.push_str(&days.to_string());
                    data.push_str("</NoncurrentDays>");
                }

                if let Some(storage_class) = &rule.noncurrent_version_transition_storage_class {
                    data.push_str("<StorageClass>");
                    data.push_str(storage_class);
                    data.push_str("</StorageClass>");
                }

                if let Some(versions) = rule.noncurrent_version_transition_newer_versions {
                    data.push_str("<NewerNoncurrentVersions>");
                    data.push_str(&versions.to_string());
                    data.push_str("</NewerNoncurrentVersions>");
                }

                data.push_str("</NoncurrentVersionTransition>");
            }

            // Transition
            let has_transition = rule.transition_date.is_some()
                || rule.transition_days.is_some()
                || rule.transition_storage_class.is_some();

            if has_transition {
                data.push_str("<Transition>");

                if let Some(date) = rule.transition_date {
                    data.push_str("<Date>");
                    data.push_str(&to_iso8601utc(date));
                    data.push_str("</Date>");
                }

                if let Some(days) = rule.transition_days {
                    data.push_str("<Days>");
                    data.push_str(&days.to_string());
                    data.push_str("</Days>");
                }

                if let Some(storage_class) = &rule.transition_storage_class {
                    data.push_str("<StorageClass>");
                    data.push_str(storage_class);
                    data.push_str("</StorageClass>");
                }

                data.push_str("</Transition>");
            }

            data.push_str("</Rule>");
        }

        data.push_str("</LifecycleConfiguration>");
        data
    }
}

#[derive(PartialEq, Clone, Debug, Default)]
pub struct LifecycleRule {
    // Common
    pub id: String,
    pub status: bool,
    pub filter: Filter,

    // Expiration
    pub expiration_days: Option<u32>,
    pub expiration_date: Option<chrono::DateTime<chrono::Utc>>,
    pub expiration_expired_object_delete_marker: Option<bool>,
    pub expiration_expired_object_all_versions: Option<bool>,

    // DelMarkerExpiration
    pub del_marker_expiration_days: Option<u32>,

    // AllVersionsExpiration
    pub all_versions_expiration_days: Option<u32>,
    pub all_versions_expiration_delete_marker: Option<bool>,

    // Transition
    pub transition_days: Option<u32>,
    pub transition_date: Option<chrono::DateTime<chrono::Utc>>,
    pub transition_storage_class: Option<String>,

    // NoncurrentVersionExpiration
    pub noncurrent_version_expiration_noncurrent_days: Option<u32>,
    pub noncurrent_version_expiration_newer_versions: Option<u32>,

    // NoncurrentVersionTransition
    pub noncurrent_version_transition_noncurrent_days: Option<u32>,
    pub noncurrent_version_transition_storage_class: Option<String>,
    pub noncurrent_version_transition_newer_versions: Option<u32>,

    // AbortIncompleteMultipartUpload
    pub abort_incomplete_multipart_upload_days_after_initiation: Option<u32>,
}

impl LifecycleRule {
    pub fn from_xml(rule_elem: &Element) -> Result<Self> {
        let mut rule = LifecycleRule::default();

        // Parse ID
        if let Some(id_elem) = rule_elem.get_child("ID") {
            if let Some(id_text) = id_elem.get_text() {
                rule.id = id_text.to_string();
            }
        }

        // Parse Status
        if let Some(status_elem) = rule_elem.get_child("Status") {
            if let Some(status_text) = status_elem.get_text() {
                rule.status = status_text == "Enabled";
            }
        } else {
            return Err(MinioError::XmlError("Missing <Status> element".to_string()));
        }

        // Parse Filter
        if let Some(filter_elem) = rule_elem.get_child("Filter") {
            rule.filter = Filter::from_xml(filter_elem)?;
        }

        // Parse AbortIncompleteMultipartUpload
        if let Some(abort_elem) = rule_elem.get_child("AbortIncompleteMultipartUpload") {
            if let Some(days_elem) = abort_elem.get_child("DaysAfterInitiation") {
                if let Some(days_text) = days_elem.get_text() {
                    rule.abort_incomplete_multipart_upload_days_after_initiation =
                        Some(days_text.parse().map_err(|_| {
                            MinioError::XmlError("Invalid DaysAfterInitiation value".to_string())
                        })?);
                }
            }
        }

        // Parse Expiration
        if let Some(expiration_elem) = rule_elem.get_child("Expiration") {
            // Date
            if let Some(date_elem) = expiration_elem.get_child("Date") {
                if let Some(date_text) = date_elem.get_text() {
                    // Assume a function that parses ISO8601 to DateTime<Utc>
                    rule.expiration_date = Some(parse_iso8601(&date_text)?);
                }
            }

            // Days
            if let Some(days_elem) = expiration_elem.get_child("Days") {
                if let Some(days_text) = days_elem.get_text() {
                    rule.expiration_days = Some(days_text.parse().map_err(|_| {
                        MinioError::XmlError("Invalid Expiration Days value".to_string())
                    })?);
                }
            }

            // ExpiredObjectDeleteMarker
            if let Some(delete_marker_elem) = expiration_elem.get_child("ExpiredObjectDeleteMarker")
            {
                if let Some(delete_marker_text) = delete_marker_elem.get_text() {
                    rule.expiration_expired_object_delete_marker =
                        Some(delete_marker_text == "true");
                }
            }

            // ExpiredObjectAllVersions
            if let Some(all_versions_elem) = expiration_elem.get_child("ExpiredObjectAllVersions") {
                if let Some(all_versions_text) = all_versions_elem.get_text() {
                    rule.expiration_expired_object_all_versions = Some(all_versions_text == "true");
                }
            }
        }

        // Parse DelMarkerExpiration
        if let Some(del_marker_elem) = rule_elem.get_child("DelMarkerExpiration") {
            if let Some(days_elem) = del_marker_elem.get_child("Days") {
                if let Some(days_text) = days_elem.get_text() {
                    rule.del_marker_expiration_days = Some(days_text.parse().map_err(|_| {
                        MinioError::XmlError("Invalid DelMarkerExpiration Days value".to_string())
                    })?);
                }
            }
        }

        // Parse AllVersionsExpiration
        if let Some(all_versions_elem) = rule_elem.get_child("AllVersionsExpiration") {
            if let Some(days_elem) = all_versions_elem.get_child("Days") {
                if let Some(days_text) = days_elem.get_text() {
                    rule.all_versions_expiration_days = Some(days_text.parse().map_err(|_| {
                        MinioError::XmlError("Invalid AllVersionsExpiration Days value".to_string())
                    })?);
                }
            }

            if let Some(delete_marker_elem) = all_versions_elem.get_child("DeleteMarker") {
                if let Some(delete_marker_text) = delete_marker_elem.get_text() {
                    rule.all_versions_expiration_delete_marker = Some(delete_marker_text == "true");
                }
            }
        }

        // Parse NoncurrentVersionExpiration
        if let Some(noncurrent_exp_elem) = rule_elem.get_child("NoncurrentVersionExpiration") {
            if let Some(days_elem) = noncurrent_exp_elem.get_child("NoncurrentDays") {
                if let Some(days_text) = days_elem.get_text() {
                    rule.noncurrent_version_expiration_noncurrent_days =
                        Some(days_text.parse().map_err(|_| {
                            MinioError::XmlError(
                                "Invalid NoncurrentVersionExpiration NoncurrentDays value"
                                    .to_string(),
                            )
                        })?);
                }
            }

            if let Some(versions_elem) = noncurrent_exp_elem.get_child("NewerNoncurrentVersions") {
                if let Some(versions_text) = versions_elem.get_text() {
                    rule.noncurrent_version_expiration_newer_versions =
                        Some(versions_text.parse().map_err(|_| {
                            MinioError::XmlError(
                                "Invalid NewerNoncurrentVersions value".to_string(),
                            )
                        })?);
                }
            }
        }

        // Parse NoncurrentVersionTransition
        if let Some(noncurrent_trans_elem) = rule_elem.get_child("NoncurrentVersionTransition") {
            if let Some(days_elem) = noncurrent_trans_elem.get_child("NoncurrentDays") {
                if let Some(days_text) = days_elem.get_text() {
                    rule.noncurrent_version_transition_noncurrent_days =
                        Some(days_text.parse().map_err(|_| {
                            MinioError::XmlError(
                                "Invalid NoncurrentVersionTransition NoncurrentDays value"
                                    .to_string(),
                            )
                        })?);
                }
            }

            if let Some(storage_elem) = noncurrent_trans_elem.get_child("StorageClass") {
                if let Some(storage_text) = storage_elem.get_text() {
                    rule.noncurrent_version_transition_storage_class =
                        Some(storage_text.to_string());
                }
            }

            if let Some(versions_elem) = noncurrent_trans_elem.get_child("NewerNoncurrentVersions")
            {
                if let Some(versions_text) = versions_elem.get_text() {
                    rule.noncurrent_version_transition_newer_versions =
                        Some(versions_text.parse().map_err(|_| {
                            MinioError::XmlError(
                                "Invalid NewerNoncurrentVersions value".to_string(),
                            )
                        })?);
                }
            }
        }

        // Parse Transition
        if let Some(transition_elem) = rule_elem.get_child("Transition") {
            // Date
            if let Some(date_elem) = transition_elem.get_child("Date") {
                if let Some(date_text) = date_elem.get_text() {
                    rule.transition_date = Some(parse_iso8601(&date_text)?);
                }
            }

            // Days
            if let Some(days_elem) = transition_elem.get_child("Days") {
                if let Some(days_text) = days_elem.get_text() {
                    rule.transition_days = Some(days_text.parse().map_err(|_| {
                        MinioError::XmlError("Invalid Transition Days value".to_string())
                    })?);
                }
            }

            // StorageClass
            if let Some(storage_elem) = transition_elem.get_child("StorageClass") {
                if let Some(storage_text) = storage_elem.get_text() {
                    rule.transition_storage_class = Some(storage_text.to_string());
                }
            }
        }

        Ok(rule)
    }

    pub fn validate(&self) -> Result<()> {
        // Basic validation requirements

        // Ensure ID is present
        if self.id.is_empty() {
            return Err(MinioError::XmlError("Rule ID cannot be empty".to_string()));
        }

        // Validate storage classes in transitions
        if let Some(storage_class) = &self.transition_storage_class {
            if storage_class.is_empty() {
                return Err(MinioError::XmlError(
                    "Transition StorageClass cannot be empty".to_string(),
                ));
            }
        }

        if let Some(storage_class) = &self.noncurrent_version_transition_storage_class {
            if storage_class.is_empty() {
                return Err(MinioError::XmlError(
                    "NoncurrentVersionTransition StorageClass cannot be empty".to_string(),
                ));
            }
        }

        // Check that expiration has either days or date, not both
        if self.expiration_days.is_some() && self.expiration_date.is_some() {
            return Err(MinioError::XmlError(
                "Expiration cannot specify both Days and Date".to_string(),
            ));
        }

        // Check that transition has either days or date, not both
        if self.transition_days.is_some() && self.transition_date.is_some() {
            return Err(MinioError::XmlError(
                "Transition cannot specify both Days and Date".to_string(),
            ));
        }

        Ok(())
    }
}

// Helper function to parse ISO8601 dates
fn parse_iso8601(date_str: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(date_str)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|_| MinioError::XmlError(format!("Invalid date format: {date_str}")))
}
