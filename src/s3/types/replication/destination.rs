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

//! Destination information

use super::access_control_translation::AccessControlTranslation;
use super::encryption_config::EncryptionConfig;
use super::metrics::Metrics;
use super::replication_time::ReplicationTime;
use crate::s3::error::ValidationErr;
use crate::s3::utils::{get_text_option, get_text_result};
use crate::s3::xml_body::XmlBody;
use xmltree::Element;

#[derive(PartialEq, Clone, Debug, Default)]
pub struct Destination {
    pub bucket_arn: String,
    pub access_control_translation: Option<AccessControlTranslation>,
    pub account: Option<String>,
    pub encryption_config: Option<EncryptionConfig>,
    pub metrics: Option<Metrics>,
    pub replication_time: Option<ReplicationTime>,
    pub storage_class: Option<String>,
}

impl Destination {
    pub fn from_xml(element: &Element) -> Result<Destination, ValidationErr> {
        Ok(Destination {
            bucket_arn: get_text_result(element, "Bucket")?,
            access_control_translation: match element.get_child("AccessControlTranslation") {
                Some(v) => Some(AccessControlTranslation {
                    owner: get_text_result(v, "Owner")?,
                }),
                _ => None,
            },
            account: get_text_option(element, "Account"),
            encryption_config: element.get_child("EncryptionConfiguration").map(|v| {
                EncryptionConfig {
                    replica_kms_key_id: get_text_option(v, "ReplicaKmsKeyID"),
                }
            }),
            metrics: match element.get_child("Metrics") {
                Some(v) => Some(Metrics {
                    event_threshold_minutes: match get_text_option(
                        v.get_child("EventThreshold")
                            .ok_or(ValidationErr::xml_error("<EventThreshold> tag not found"))?,
                        "Minutes",
                    ) {
                        Some(v) => Some(v.parse::<i32>()?),
                        _ => None,
                    },
                    status: get_text_result(v, "Status")? == "Enabled",
                }),
                _ => None,
            },
            replication_time: match element.get_child("ReplicationTime") {
                Some(v) => Some(ReplicationTime {
                    time_minutes: match get_text_option(v, "Time") {
                        Some(v) => Some(v.parse::<i32>()?),
                        _ => None,
                    },
                    status: get_text_result(v, "Status")? == "Enabled",
                }),
                _ => None,
            },
            storage_class: get_text_option(element, "StorageClass"),
        })
    }

    pub fn to_xml(&self) -> Result<String, ValidationErr> {
        let mut body = XmlBody::new();
        self.write_xml(&mut body);
        body.finish()
    }

    pub(crate) fn write_xml(&self, body: &mut XmlBody) {
        let status = |enabled: bool| if enabled { "Enabled" } else { "Disabled" };
        body.start("Destination")
            .element("Bucket", &self.bucket_arn);
        if let Some(v) = &self.access_control_translation {
            body.start("AccessControlTranslation")
                .element("Owner", &v.owner)
                .end();
        }
        body.optional("Account", self.account.as_deref());
        if let Some(c) = &self.encryption_config {
            body.start("EncryptionConfiguration")
                .optional("ReplicaKmsKeyID", c.replica_kms_key_id.as_deref())
                .end();
        }
        if let Some(m) = &self.metrics {
            body.start("Metrics")
                .start("EventThreshold")
                .optional(
                    "Minutes",
                    m.event_threshold_minutes.map(|v| v.to_string()).as_deref(),
                )
                .element("Status", status(m.status))
                .end()
                .end();
        }
        if let Some(t) = &self.replication_time {
            body.start("ReplicationTime")
                .element(
                    "Time",
                    &t.time_minutes.map(|v| v.to_string()).unwrap_or_default(),
                )
                .element("Status", status(t.status))
                .end();
        }
        body.optional("StorageClass", self.storage_class.as_deref());
        body.end();
    }
}
