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

//! Notification record information

use super::super::s3_struct::S3;
use super::request_parameters::RequestParameters;
use super::response_elements::ResponseElements;
use super::source::Source;
use super::user_identity::UserIdentity;
use crate::s3::utils::UtcTime;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NotificationRecord {
    #[serde(alias = "eventVersion")]
    pub event_version: String,
    #[serde(alias = "eventSource")]
    pub event_source: String,
    #[serde(alias = "awsRegion")]
    pub aws_region: String,
    #[serde(
        alias = "eventTime",
        default,
        with = "crate::s3::utils::aws_date_format"
    )]
    pub event_time: UtcTime,
    #[serde(alias = "eventName")]
    pub event_name: String,
    #[serde(alias = "userIdentity")]
    pub user_identity: UserIdentity,
    #[serde(alias = "requestParameters")]
    pub request_parameters: Option<RequestParameters>,
    #[serde(alias = "responseElements")]
    pub response_elements: ResponseElements,
    #[serde(alias = "s3")]
    pub s3: S3,
    #[serde(alias = "source")]
    pub source: Source,
}
