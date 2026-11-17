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

use crate::s3::error::ValidationErr;
use crate::s3::types::Filter;
use crate::s3::utils::to_iso8601utc;
use crate::s3::xml_body::XmlBody;
use xmltree::Element;

#[derive(PartialEq, Clone, Debug, Default)]
/// Lifecycle configuration
pub struct LifecycleConfig {
    pub rules: Vec<LifecycleRule>,
}

impl LifecycleConfig {
    pub fn from_xml(root: &Element) -> Result<LifecycleConfig, ValidationErr> {
        let mut config = LifecycleConfig { rules: Vec::new() };

        // Process all Rule elements in the XML
        for rule_elem in root.children.iter().filter_map(|c| c.as_element()) {
            if rule_elem.name == "Rule" {
                config.rules.push(LifecycleRule::from_xml(rule_elem)?);
            }
        }

        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ValidationErr> {
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

    pub fn to_xml(&self) -> Result<String, ValidationErr> {
        let number = |v: Option<u32>| v.map(|v| v.to_string());
        let mut body = XmlBody::new();
        body.start("LifecycleConfiguration");
        for rule in &self.rules {
            body.start("Rule");
            if !rule.id.is_empty() {
                body.element("ID", &rule.id);
            }
            body.element("Status", if rule.status { "Enabled" } else { "Disabled" });
            if !rule.filter.is_null() {
                rule.filter.write_xml(&mut body);
            }
            if let Some(days) = rule.abort_incomplete_multipart_upload_days_after_initiation {
                body.start("AbortIncompleteMultipartUpload")
                    .element("DaysAfterInitiation", &days.to_string())
                    .end();
            }
            let has_expiration = rule.expiration_date.is_some()
                || rule.expiration_days.is_some()
                || rule.expiration_expired_object_delete_marker.is_some()
                || rule.expiration_expired_object_all_versions.is_some();
            if has_expiration {
                body.start("Expiration")
                    .optional("Date", rule.expiration_date.map(to_iso8601utc).as_deref())
                    .optional("Days", number(rule.expiration_days).as_deref());
                if rule.expiration_expired_object_delete_marker == Some(true) {
                    body.element("ExpiredObjectDeleteMarker", "true");
                }
                if rule.expiration_expired_object_all_versions == Some(true) {
                    body.element("ExpiredObjectAllVersions", "true");
                }
                body.end();
            }
            if let Some(days) = rule.del_marker_expiration_days {
                body.start("DelMarkerExpiration")
                    .element("Days", &days.to_string())
                    .end();
            }
            if let Some(days) = rule.all_versions_expiration_days {
                body.start("AllVersionsExpiration")
                    .element("Days", &days.to_string());
                if rule.all_versions_expiration_delete_marker == Some(true) {
                    body.element("DeleteMarker", "true");
                }
                body.end();
            }
            if let Some(days) = rule.noncurrent_version_expiration_noncurrent_days {
                body.start("NoncurrentVersionExpiration")
                    .element("NoncurrentDays", &days.to_string())
                    .optional(
                        "NewerNoncurrentVersions",
                        number(rule.noncurrent_version_expiration_newer_versions).as_deref(),
                    )
                    .end();
            }
            let has_noncurrent_transition =
                rule.noncurrent_version_transition_noncurrent_days.is_some()
                    || rule.noncurrent_version_transition_storage_class.is_some()
                    || rule.noncurrent_version_transition_newer_versions.is_some();
            if has_noncurrent_transition {
                body.start("NoncurrentVersionTransition")
                    .optional(
                        "NoncurrentDays",
                        number(rule.noncurrent_version_transition_noncurrent_days).as_deref(),
                    )
                    .optional(
                        "StorageClass",
                        rule.noncurrent_version_transition_storage_class.as_deref(),
                    )
                    .optional(
                        "NewerNoncurrentVersions",
                        number(rule.noncurrent_version_transition_newer_versions).as_deref(),
                    )
                    .end();
            }
            let has_transition = rule.transition_date.is_some()
                || rule.transition_days.is_some()
                || rule.transition_storage_class.is_some();
            if has_transition {
                body.start("Transition")
                    .optional("Date", rule.transition_date.map(to_iso8601utc).as_deref())
                    .optional("Days", number(rule.transition_days).as_deref())
                    .optional("StorageClass", rule.transition_storage_class.as_deref())
                    .end();
            }
            body.end();
        }
        body.end();
        body.finish()
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
    pub fn from_xml(rule_elem: &Element) -> Result<Self, ValidationErr> {
        let mut rule = LifecycleRule::default();

        // Parse ID
        if let Some(id_elem) = rule_elem.get_child("ID")
            && let Some(id_text) = id_elem.get_text()
        {
            rule.id = id_text.to_string();
        }

        // Parse Status
        if let Some(status_elem) = rule_elem.get_child("Status") {
            if let Some(status_text) = status_elem.get_text() {
                rule.status = status_text == "Enabled";
            }
        } else {
            return Err(ValidationErr::xml_error("Missing <Status> element"));
        }

        // Parse Filter
        if let Some(filter_elem) = rule_elem.get_child("Filter") {
            rule.filter = Filter::from_xml(filter_elem)?;
        }

        // Parse AbortIncompleteMultipartUpload
        if let Some(abort_elem) = rule_elem.get_child("AbortIncompleteMultipartUpload")
            && let Some(days_elem) = abort_elem.get_child("DaysAfterInitiation")
            && let Some(days_text) = days_elem.get_text()
        {
            rule.abort_incomplete_multipart_upload_days_after_initiation =
                Some(days_text.parse().map_err(|e| {
                    ValidationErr::xml_error_with_source("Invalid DaysAfterInitiation value", e)
                })?)
        };

        // Parse Expiration
        if let Some(expiration_elem) = rule_elem.get_child("Expiration") {
            // Date
            if let Some(date_elem) = expiration_elem.get_child("Date")
                && let Some(date_text) = date_elem.get_text()
            {
                // Assume a function that parses ISO8601 to DateTime<Utc>
                rule.expiration_date = Some(parse_iso8601(&date_text)?);
            }

            // Days
            if let Some(days_elem) = expiration_elem.get_child("Days")
                && let Some(days_text) = days_elem.get_text()
            {
                rule.expiration_days = Some(days_text.parse().map_err(|e| {
                    ValidationErr::xml_error_with_source("Invalid Expiration Days value", e)
                })?);
            }

            // ExpiredObjectDeleteMarker
            if let Some(delete_marker_elem) = expiration_elem.get_child("ExpiredObjectDeleteMarker")
                && let Some(delete_marker_text) = delete_marker_elem.get_text()
            {
                rule.expiration_expired_object_delete_marker = Some(delete_marker_text == "true");
            }

            // ExpiredObjectAllVersions
            if let Some(all_versions_elem) = expiration_elem.get_child("ExpiredObjectAllVersions")
                && let Some(all_versions_text) = all_versions_elem.get_text()
            {
                rule.expiration_expired_object_all_versions = Some(all_versions_text == "true");
            }
        }

        // Parse DelMarkerExpiration
        if let Some(del_marker_elem) = rule_elem.get_child("DelMarkerExpiration")
            && let Some(days_elem) = del_marker_elem.get_child("Days")
            && let Some(days_text) = days_elem.get_text()
        {
            rule.del_marker_expiration_days = Some(days_text.parse().map_err(|e| {
                ValidationErr::xml_error_with_source("Invalid DelMarkerExpiration Days value", e)
            })?);
        }

        // Parse AllVersionsExpiration
        if let Some(all_versions_elem) = rule_elem.get_child("AllVersionsExpiration") {
            if let Some(days_elem) = all_versions_elem.get_child("Days")
                && let Some(days_text) = days_elem.get_text()
            {
                rule.all_versions_expiration_days = Some(days_text.parse().map_err(|e| {
                    ValidationErr::xml_error_with_source(
                        "Invalid AllVersionsExpiration Days value",
                        e,
                    )
                })?);
            }

            if let Some(delete_marker_elem) = all_versions_elem.get_child("DeleteMarker")
                && let Some(delete_marker_text) = delete_marker_elem.get_text()
            {
                rule.all_versions_expiration_delete_marker = Some(delete_marker_text == "true");
            }
        }

        // Parse NoncurrentVersionExpiration
        if let Some(noncurrent_exp_elem) = rule_elem.get_child("NoncurrentVersionExpiration") {
            if let Some(days_elem) = noncurrent_exp_elem.get_child("NoncurrentDays")
                && let Some(days_text) = days_elem.get_text()
            {
                rule.noncurrent_version_expiration_noncurrent_days =
                    Some(days_text.parse().map_err(|e| {
                        ValidationErr::xml_error_with_source(
                            "Invalid NoncurrentVersionExpiration NoncurrentDays value",
                            e,
                        )
                    })?);
            }

            if let Some(versions_elem) = noncurrent_exp_elem.get_child("NewerNoncurrentVersions")
                && let Some(versions_text) = versions_elem.get_text()
            {
                rule.noncurrent_version_expiration_newer_versions =
                    Some(versions_text.parse().map_err(|e| {
                        ValidationErr::xml_error_with_source(
                            "Invalid NewerNoncurrentVersions value",
                            e,
                        )
                    })?);
            }
        }

        // Parse NoncurrentVersionTransition
        if let Some(noncurrent_trans_elem) = rule_elem.get_child("NoncurrentVersionTransition") {
            if let Some(days_elem) = noncurrent_trans_elem.get_child("NoncurrentDays")
                && let Some(days_text) = days_elem.get_text()
            {
                rule.noncurrent_version_transition_noncurrent_days =
                    Some(days_text.parse().map_err(|e| {
                        ValidationErr::xml_error_with_source(
                            "Invalid NoncurrentVersionTransition NoncurrentDays value",
                            e,
                        )
                    })?);
            }

            if let Some(storage_elem) = noncurrent_trans_elem.get_child("StorageClass")
                && let Some(storage_text) = storage_elem.get_text()
            {
                rule.noncurrent_version_transition_storage_class = Some(storage_text.to_string());
            }

            if let Some(versions_elem) = noncurrent_trans_elem.get_child("NewerNoncurrentVersions")
                && let Some(versions_text) = versions_elem.get_text()
            {
                rule.noncurrent_version_transition_newer_versions =
                    Some(versions_text.parse().map_err(|e| {
                        ValidationErr::xml_error_with_source(
                            "Invalid NewerNoncurrentVersions value",
                            e,
                        )
                    })?);
            }
        }

        // Parse Transition
        if let Some(transition_elem) = rule_elem.get_child("Transition") {
            // Date
            if let Some(date_elem) = transition_elem.get_child("Date")
                && let Some(date_text) = date_elem.get_text()
            {
                rule.transition_date = Some(parse_iso8601(&date_text)?);
            }

            // Days
            if let Some(days_elem) = transition_elem.get_child("Days")
                && let Some(days_text) = days_elem.get_text()
            {
                rule.transition_days = Some(days_text.parse().map_err(|e| {
                    ValidationErr::xml_error_with_source("Invalid Transition Days value", e)
                })?);
            }

            // StorageClass
            if let Some(storage_elem) = transition_elem.get_child("StorageClass")
                && let Some(storage_text) = storage_elem.get_text()
            {
                rule.transition_storage_class = Some(storage_text.to_string());
            }
        }

        Ok(rule)
    }

    pub fn validate(&self) -> Result<(), ValidationErr> {
        // Basic validation requirements

        // Ensure ID is present
        if self.id.is_empty() {
            return Err(ValidationErr::xml_error("Rule ID cannot be empty"));
        }

        // Validate storage classes in transitions
        if let Some(storage_class) = &self.transition_storage_class
            && storage_class.is_empty()
        {
            return Err(ValidationErr::xml_error(
                "Transition StorageClass cannot be empty",
            ));
        }

        if let Some(storage_class) = &self.noncurrent_version_transition_storage_class
            && storage_class.is_empty()
        {
            return Err(ValidationErr::xml_error(
                "NoncurrentVersionTransition StorageClass cannot be empty",
            ));
        }

        // Check that expiration has either days or date, not both
        if self.expiration_days.is_some() && self.expiration_date.is_some() {
            return Err(ValidationErr::xml_error(
                "Expiration cannot specify both Days and Date",
            ));
        }

        // Check that transition has either days or date, not both
        if self.transition_days.is_some() && self.transition_date.is_some() {
            return Err(ValidationErr::xml_error(
                "Transition cannot specify both Days and Date",
            ));
        }

        Ok(())
    }
}

// Helper function to parse ISO8601 dates
fn parse_iso8601(date_str: &str) -> Result<chrono::DateTime<chrono::Utc>, ValidationErr> {
    chrono::DateTime::parse_from_rfc3339(date_str)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|e| {
            ValidationErr::xml_error_with_source(format!("Invalid date format: {date_str}"), e)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_filter_prefix_reaches_the_server_unchanged() {
        let config = LifecycleConfig {
            rules: vec![LifecycleRule {
                id: "R&D".to_string(),
                status: true,
                filter: Filter {
                    prefix: Some("R&D <archive>/".to_string()),
                    ..Default::default()
                },
                expiration_days: Some(30),
                ..Default::default()
            }],
        };
        let xml = config.to_xml().unwrap();
        let root = xmltree::Element::parse(xml.as_bytes()).unwrap();
        let rule = root.get_child("Rule").unwrap();
        assert_eq!(rule.get_child("ID").unwrap().get_text().unwrap(), "R&D");
        let filter = rule.get_child("Filter").unwrap();
        assert_eq!(
            filter.get_child("Prefix").unwrap().get_text().unwrap(),
            "R&D <archive>/"
        );
    }

    #[test]
    fn empty_filter_omits_filter_element() {
        let config = LifecycleConfig {
            rules: vec![LifecycleRule {
                id: "rule1".to_string(),
                status: true,
                filter: Filter::default(),
                expiration_days: Some(30),
                ..Default::default()
            }],
        };

        let xml = config.to_xml().unwrap();
        assert!(!xml.contains("<Filter>"));
        assert!(!xml.contains("</Filter>"));
        assert!(xml.contains("<Days>30</Days>"));
    }

    #[test]
    fn non_empty_filter_emits_filter_element() {
        let config = LifecycleConfig {
            rules: vec![LifecycleRule {
                id: "rule1".to_string(),
                status: true,
                filter: Filter {
                    and_operator: None,
                    prefix: Some("logs/".to_string()),
                    tag: None,
                },
                expiration_days: Some(30),
                ..Default::default()
            }],
        };

        let xml = config.to_xml().unwrap();
        assert!(xml.contains("<Filter><Prefix>logs/</Prefix></Filter>"));
    }
}
