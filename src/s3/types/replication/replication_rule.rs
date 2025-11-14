// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

//! Replication rule information

use super::super::notification::Filter;
use super::destination::Destination;
use super::source_selection_criteria::SourceSelectionCriteria;
use crate::s3::error::ValidationErr;
use crate::s3::utils::{get_text_option, get_text_result};
use xmltree::Element;

#[derive(PartialEq, Clone, Debug, Default)]
pub struct ReplicationRule {
    pub destination: Destination,
    pub delete_marker_replication_status: Option<bool>,
    pub existing_object_replication_status: Option<bool>,
    pub filter: Option<Filter>,
    pub id: Option<String>,
    pub prefix: Option<String>,
    pub priority: Option<i32>,
    pub source_selection_criteria: Option<SourceSelectionCriteria>,
    pub delete_replication_status: Option<bool>,
    pub status: bool,
}

impl ReplicationRule {
    pub fn from_xml(element: &Element) -> Result<ReplicationRule, ValidationErr> {
        Ok(ReplicationRule {
            destination: Destination::from_xml(
                element
                    .get_child("Destination")
                    .ok_or(ValidationErr::xml_error("<Destination> tag not found"))?,
            )?,
            delete_marker_replication_status: match element.get_child("DeleteMarkerReplication") {
                Some(v) => Some(get_text_result(v, "Status")? == "Enabled"),
                _ => None,
            },
            existing_object_replication_status: match element.get_child("ExistingObjectReplication")
            {
                Some(v) => Some(get_text_result(v, "Status")? == "Enabled"),
                _ => None,
            },
            filter: match element.get_child("Filter") {
                Some(v) => Some(Filter::from_xml(v)?),
                _ => None,
            },
            id: get_text_option(element, "ID"),
            prefix: get_text_option(element, "Prefix"),
            priority: match get_text_option(element, "Priority") {
                Some(v) => Some(v.parse::<i32>()?),
                _ => None,
            },
            source_selection_criteria: match element.get_child("SourceSelectionCriteria") {
                Some(v) => match v.get_child("SseKmsEncryptedObjects") {
                    Some(v) => Some(SourceSelectionCriteria {
                        sse_kms_encrypted_objects_status: Some(
                            get_text_result(v, "Status")? == "Enabled",
                        ),
                    }),
                    _ => Some(SourceSelectionCriteria {
                        sse_kms_encrypted_objects_status: None,
                    }),
                },
                _ => None,
            },
            delete_replication_status: match element.get_child("DeleteReplication") {
                Some(v) => Some(get_text_result(v, "Status")? == "Enabled"),
                _ => None,
            },
            status: get_text_result(element, "Status")? == "Enabled",
        })
    }

    pub fn to_xml(&self) -> String {
        let mut data = self.destination.to_xml();

        if let Some(v) = self.delete_marker_replication_status {
            data.push_str("<DeleteMarkerReplication>");
            data.push_str("<Status>");
            data.push_str(match v {
                true => "Enabled",
                false => "Disabled",
            });
            data.push_str("</Status>");
            data.push_str("</DeleteMarkerReplication>");
        }

        if let Some(v) = self.existing_object_replication_status {
            data.push_str("<ExistingObjectReplication>");
            data.push_str("<Status>");
            data.push_str(match v {
                true => "Enabled",
                false => "Disabled",
            });
            data.push_str("</Status>");
            data.push_str("</ExistingObjectReplication>");
        }

        if let Some(v) = &self.filter {
            data.push_str(&v.to_xml())
        }

        if let Some(v) = &self.id {
            data.push_str("<ID>");
            data.push_str(v);
            data.push_str("</ID>");
        }

        if let Some(v) = &self.prefix {
            data.push_str("<Prefix>");
            data.push_str(v);
            data.push_str("</Prefix>");
        }

        if let Some(v) = self.priority {
            data.push_str("<Priority>");
            data.push_str(&v.to_string());
            data.push_str("</Priority>");
        }

        if let Some(s) = &self.source_selection_criteria {
            data.push_str("<SourceSelectionCriteria>");
            if let Some(v) = s.sse_kms_encrypted_objects_status {
                data.push_str("<SseKmsEncryptedObjects>");
                data.push_str("<Status>");
                data.push_str(match v {
                    true => "Enabled",
                    false => "Disabled",
                });
                data.push_str("</Status>");
                data.push_str("</SseKmsEncryptedObjects>");
            }
            data.push_str("</SourceSelectionCriteria>");
        }

        if let Some(v) = self.delete_replication_status {
            data.push_str("<DeleteReplication>");
            data.push_str("<Status>");
            data.push_str(match v {
                true => "Enabled",
                false => "Disabled",
            });
            data.push_str("</Status>");
            data.push_str("</DeleteReplication>");
        }

        data.push_str("<Status>");
        data.push_str(match self.status {
            true => "Enabled",
            false => "Disabled",
        });
        data.push_str("</Status>");

        data
    }
}
