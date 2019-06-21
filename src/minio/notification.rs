/*
 * MinIO Rust Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;

use serde_derive::Deserialize;

/// Notification event object metadata.
#[derive(Deserialize, Debug)]
pub struct ObjectMeta {
    #[serde(rename(deserialize = "key"))]
    pub key: String,
    #[serde(rename(deserialize = "size"))]
    pub size: Option<i64>,
    #[serde(rename(deserialize = "eTag"))]
    pub e_tag: Option<String>,
    #[serde(rename(deserialize = "versionId"))]
    pub version_id: Option<String>,
    #[serde(rename(deserialize = "sequencer"))]
    pub sequencer: String,
}

/// Notification event bucket metadata.
#[derive(Deserialize, Debug)]
pub struct BucketMeta {
    #[serde(rename(deserialize = "name"))]
    pub name: String,
    #[serde(rename(deserialize = "ownerIdentity"))]
    pub owner_identity: Identity,
    #[serde(rename(deserialize = "arn"))]
    pub arn: String,
}

/// Indentity represents the user id, this is a compliance field.
#[derive(Deserialize, Debug)]
pub struct Identity {
    #[serde(rename(deserialize = "principalId"))]
    pub principal_id: String,
}

//// sourceInfo represents information on the client that
//// triggered the event notification.
#[derive(Deserialize, Debug)]
pub struct SourceInfo {
    #[serde(rename(deserialize = "host"))]
    pub host: String,
    #[serde(rename(deserialize = "port"))]
    pub port: String,
    #[serde(rename(deserialize = "userAgent"))]
    pub user_agent: String,
}

/// Notification event server specific metadata.
#[derive(Deserialize, Debug)]
pub struct EventMeta {
    #[serde(rename(deserialize = "s3SchemaVersion"))]
    pub schema_version: String,
    #[serde(rename(deserialize = "configurationId"))]
    pub configuration_id: String,
    #[serde(rename(deserialize = "bucket"))]
    pub bucket: BucketMeta,
    #[serde(rename(deserialize = "object"))]
    pub object: ObjectMeta,
}

/// NotificationEvent represents an Amazon an S3 bucket notification event.
#[derive(Deserialize, Debug)]
pub struct NotificationEvent {
    #[serde(rename(deserialize = "eventVersion"))]
    pub event_version: String,
    #[serde(rename(deserialize = "eventSource"))]
    pub event_source: String,
    #[serde(rename(deserialize = "awsRegion"))]
    pub aws_region: String,
    #[serde(rename(deserialize = "eventTime"))]
    pub event_time: String,
    #[serde(rename(deserialize = "eventName"))]
    pub event_name: String,
    #[serde(rename(deserialize = "source"))]
    pub source: SourceInfo,
    #[serde(rename(deserialize = "userIdentity"))]
    pub user_identity: Identity,
    #[serde(rename(deserialize = "requestParameters"))]
    pub request_parameters: HashMap<String, String>,
    #[serde(rename(deserialize = "responseElements"))]
    pub response_elements: HashMap<String, String>,
    #[serde(rename(deserialize = "s3"))]
    pub s3: EventMeta,
}

/// NotificationInfo - represents the collection of notification events, additionally
/// also reports errors if any while listening on bucket notifications.
#[derive(Deserialize, Debug)]
pub struct NotificationInfo {
    #[serde(rename(deserialize = "Records"), default = "Vec::new")]
    pub records: Vec<NotificationEvent>,
    pub err: Option<String>,
}
