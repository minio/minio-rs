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

//! Replication configuration information

use super::replication_rule::ReplicationRule;
use crate::s3::error::ValidationErr;
use crate::s3::utils::get_text_option;
use xmltree::Element;

#[derive(PartialEq, Clone, Debug, Default)]
pub struct ReplicationConfig {
    pub role: Option<String>,
    pub rules: Vec<ReplicationRule>,
}

impl ReplicationConfig {
    pub fn from_xml(root: &Element) -> Result<ReplicationConfig, ValidationErr> {
        let mut config = ReplicationConfig {
            role: get_text_option(root, "Role"),
            rules: Vec::new(),
        };

        if let Some(v) = root.get_child("Rule") {
            for rule in &v.children {
                config.rules.push(ReplicationRule::from_xml(
                    rule.as_element()
                        .ok_or(ValidationErr::xml_error("<Rule> tag not found"))?,
                )?);
            }
        }

        Ok(config)
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<ReplicationConfiguration>");

        if let Some(v) = &self.role {
            data.push_str("<Status>");
            data.push_str(v);
            data.push_str("</Status>");
        }

        for rule in &self.rules {
            data.push_str(&rule.to_xml());
        }

        data.push_str("</ReplicationConfiguration>");
        data
    }
}
