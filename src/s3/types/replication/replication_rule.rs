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
use crate::s3::xml_body::XmlBody;
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

    pub fn to_xml(&self) -> Result<String, ValidationErr> {
        let mut body = XmlBody::new();
        self.write_xml(&mut body);
        body.finish()
    }

    pub(crate) fn write_xml(&self, body: &mut XmlBody) {
        let status = |enabled: bool| if enabled { "Enabled" } else { "Disabled" };
        self.destination.write_xml(body);
        if let Some(v) = self.delete_marker_replication_status {
            body.start("DeleteMarkerReplication")
                .element("Status", status(v))
                .end();
        }
        if let Some(v) = self.existing_object_replication_status {
            body.start("ExistingObjectReplication")
                .element("Status", status(v))
                .end();
        }
        if let Some(v) = &self.filter {
            v.write_xml(body);
        }
        body.optional("ID", self.id.as_deref())
            .optional("Prefix", self.prefix.as_deref())
            .optional("Priority", self.priority.map(|v| v.to_string()).as_deref());
        if let Some(s) = &self.source_selection_criteria {
            body.start("SourceSelectionCriteria");
            if let Some(v) = s.sse_kms_encrypted_objects_status {
                body.start("SseKmsEncryptedObjects")
                    .element("Status", status(v))
                    .end();
            }
            body.end();
        }
        if let Some(v) = self.delete_replication_status {
            body.start("DeleteReplication")
                .element("Status", status(v))
                .end();
        }
        body.element("Status", status(self.status));
    }
}
