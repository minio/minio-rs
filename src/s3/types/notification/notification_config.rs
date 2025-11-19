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

//! Notification configuration information

use super::cloud_func_config::CloudFuncConfig;
use super::queue_config::QueueConfig;
use super::topic_config::TopicConfig;
use crate::s3::error::ValidationErr;
use xmltree::Element;

#[derive(PartialEq, Clone, Debug, Default)]
pub struct NotificationConfig {
    pub cloud_func_config_list: Option<Vec<CloudFuncConfig>>,
    pub queue_config_list: Option<Vec<QueueConfig>>,
    pub topic_config_list: Option<Vec<TopicConfig>>,
}

impl NotificationConfig {
    pub fn from_xml(root: &mut Element) -> Result<NotificationConfig, ValidationErr> {
        let mut config = NotificationConfig {
            cloud_func_config_list: None,
            queue_config_list: None,
            topic_config_list: None,
        };

        let mut cloud_func_config_list = Vec::new();
        while let Some(mut v) = root.take_child("CloudFunctionConfiguration") {
            cloud_func_config_list.push(CloudFuncConfig::from_xml(&mut v)?);
        }
        if !cloud_func_config_list.is_empty() {
            config.cloud_func_config_list = Some(cloud_func_config_list);
        }

        let mut queue_config_list = Vec::new();
        while let Some(mut v) = root.take_child("QueueConfiguration") {
            queue_config_list.push(QueueConfig::from_xml(&mut v)?);
        }
        if !queue_config_list.is_empty() {
            config.queue_config_list = Some(queue_config_list);
        }

        let mut topic_config_list = Vec::new();
        while let Some(mut v) = root.take_child("TopicConfiguration") {
            topic_config_list.push(TopicConfig::from_xml(&mut v)?);
        }
        if !topic_config_list.is_empty() {
            config.topic_config_list = Some(topic_config_list);
        }

        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ValidationErr> {
        if let Some(v) = &self.cloud_func_config_list {
            for rule in v {
                rule.validate()?;
            }
        }

        if let Some(v) = &self.queue_config_list {
            for rule in v {
                rule.validate()?;
            }
        }

        if let Some(v) = &self.topic_config_list {
            for rule in v {
                rule.validate()?;
            }
        }

        Ok(())
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<NotificationConfiguration>");

        if let Some(v) = &self.cloud_func_config_list {
            for rule in v {
                data.push_str(&rule.to_xml())
            }
        }

        if let Some(v) = &self.queue_config_list {
            for rule in v {
                data.push_str(&rule.to_xml())
            }
        }

        if let Some(v) = &self.topic_config_list {
            for rule in v {
                data.push_str(&rule.to_xml())
            }
        }

        data.push_str("</NotificationConfiguration>");
        data
    }
}
