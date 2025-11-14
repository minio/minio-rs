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

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<Destination>");

        data.push_str("<Bucket>");
        data.push_str(&self.bucket_arn);
        data.push_str("</Bucket>");

        if let Some(v) = &self.access_control_translation {
            data.push_str("<AccessControlTranslation><Owner>");
            data.push_str(&v.owner);
            data.push_str("</Owner></AccessControlTranslation>");
        }

        if let Some(v) = &self.account {
            data.push_str("<Account>");
            data.push_str(v);
            data.push_str("</Account>");
        }

        if let Some(c) = &self.encryption_config {
            data.push_str("<EncryptionConfiguration>");
            if let Some(v) = &c.replica_kms_key_id {
                data.push_str("<ReplicaKmsKeyID>");
                data.push_str(v);
                data.push_str("</ReplicaKmsKeyID>");
            }
            data.push_str("</EncryptionConfiguration>");
        }

        if let Some(m) = &self.metrics {
            data.push_str("<Metrics><EventThreshold>");

            if let Some(v) = m.event_threshold_minutes {
                data.push_str("<Minutes>");
                data.push_str(&v.to_string());
                data.push_str("</Minutes>");
            }

            data.push_str("<Status>");
            data.push_str(match m.status {
                true => "Enabled",
                false => "Disabled",
            });
            data.push_str("</Status>");

            data.push_str("</EventThreshold></Metrics>");
        }

        if let Some(t) = &self.replication_time {
            data.push_str("<ReplicationTime>");

            data.push_str("<Time>");
            if let Some(v) = t.time_minutes {
                data.push_str(&v.to_string());
            }
            data.push_str("</Time>");

            data.push_str("<Status>");
            data.push_str(match t.status {
                true => "Enabled",
                false => "Disabled",
            });
            data.push_str("</Status>");

            data.push_str("</ReplicationTime>");
        }

        if let Some(v) = &self.storage_class {
            data.push_str("<StorageClass>");
            data.push_str(v);
            data.push_str("</StorageClass>");
        }

        data.push_str("</Destination>");

        data
    }
}
