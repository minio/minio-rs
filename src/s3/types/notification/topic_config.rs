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

//! Topic configuration information

use super::notification_common::{
    parse_common_notification_config, to_xml_common_notification_config,
};
use super::prefix_filter_rule::PrefixFilterRule;
use super::suffix_filter_rule::SuffixFilterRule;
use crate::s3::error::ValidationErr;
use crate::s3::utils::get_text_result;
use xmltree::Element;

#[derive(PartialEq, Clone, Debug)]
pub struct TopicConfig {
    pub events: Vec<String>,
    pub id: Option<String>,
    pub prefix_filter_rule: Option<PrefixFilterRule>,
    pub suffix_filter_rule: Option<SuffixFilterRule>,
    pub topic: String,
}

impl TopicConfig {
    pub fn from_xml(element: &mut Element) -> Result<TopicConfig, ValidationErr> {
        let (events, id, prefix_filter_rule, suffix_filter_rule) =
            parse_common_notification_config(element)?;
        Ok(TopicConfig {
            events,
            id,
            prefix_filter_rule,
            suffix_filter_rule,
            topic: get_text_result(element, "Topic")?,
        })
    }

    pub fn validate(&self) -> Result<(), ValidationErr> {
        if !self.events.is_empty() && !self.topic.is_empty() {
            return Ok(());
        }

        Err(ValidationErr::InvalidFilter(self.to_xml()))
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<TopicConfiguration>");

        data.push_str("<Topic>");
        data.push_str(&self.topic);
        data.push_str("</Topic>");

        data.push_str(&to_xml_common_notification_config(
            &self.events,
            &self.id,
            &self.prefix_filter_rule,
            &self.suffix_filter_rule,
        ));

        data.push_str("</TopicConfiguration>");

        data
    }
}
