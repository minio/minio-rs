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

//! Object lock configuration information

use super::super::basic_types::RetentionMode;
use crate::s3::error::ValidationErr;
use crate::s3::utils::{get_text_option, get_text_result};
use xmltree::Element;

#[derive(Clone, Debug, Default)]
pub struct ObjectLockConfig {
    pub retention_mode: Option<RetentionMode>,
    pub retention_duration_days: Option<i32>,
    pub retention_duration_years: Option<i32>,
}

impl ObjectLockConfig {
    pub fn new(
        mode: RetentionMode,
        days: Option<i32>,
        years: Option<i32>,
    ) -> Result<Self, ValidationErr> {
        if days.is_some() ^ years.is_some() {
            return Ok(Self {
                retention_mode: Some(mode),
                retention_duration_days: days,
                retention_duration_years: years,
            });
        }

        Err(ValidationErr::InvalidObjectLockConfig(
            "only one field 'days' or 'years' must be set".into(),
        ))
    }

    pub fn from_xml(root: &Element) -> Result<ObjectLockConfig, ValidationErr> {
        let mut config = ObjectLockConfig {
            retention_mode: None,
            retention_duration_days: None,
            retention_duration_years: None,
        };

        if let Some(r) = root.get_child("Rule") {
            let default_retention = r
                .get_child("DefaultRetention")
                .ok_or(ValidationErr::xml_error("<DefaultRetention> tag not found"))?;
            config.retention_mode = Some(RetentionMode::parse(&get_text_result(
                default_retention,
                "Mode",
            )?)?);

            if let Some(v) = get_text_option(default_retention, "Days") {
                config.retention_duration_days = Some(v.parse::<i32>()?);
            }

            if let Some(v) = get_text_option(default_retention, "Years") {
                config.retention_duration_years = Some(v.parse::<i32>()?);
            }
        }

        Ok(config)
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<ObjectLockConfiguration>");
        data.push_str("<ObjectLockEnabled>Enabled</ObjectLockEnabled>");
        if let Some(v) = &self.retention_mode {
            data.push_str("<Rule><DefaultRetention>");
            data.push_str("<Mode>");
            data.push_str(&v.to_string());
            data.push_str("</Mode>");
            if let Some(d) = self.retention_duration_days {
                data.push_str("<Days>");
                data.push_str(&d.to_string());
                data.push_str("</Days>");
            }
            if let Some(d) = self.retention_duration_years {
                data.push_str("<Years>");
                data.push_str(&d.to_string());
                data.push_str("</Years>");
            }
            data.push_str("</DefaultRetention></Rule>");
        }
        data.push_str("</ObjectLockConfiguration>");

        data
    }
}
