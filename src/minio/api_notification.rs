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

use crate::minio::net::{Values, ValuesAccess};
use crate::minio::{Client, Err, S3Req, SPACE_BYTE};
use futures::future::Future;
use futures::{stream, Stream};
use hyper::{Body, HeaderMap, Method};
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

impl Client {
    /// listen_bucket_notificaion - Get bucket notifications for the bucket_name.
    pub fn listen_bucket_notificaion(
        &self,
        bucket_name: &str,
        prefix: Option<String>,
        suffix: Option<String>,
        events: Vec<String>,
    ) -> impl Stream<Item = NotificationInfo, Error = Err> {
        // Prepare request query parameters
        let mut query_params: Values = Values::new();
        query_params.set_value("prefix", prefix);
        query_params.set_value("suffix", suffix);
        let opt_events: Vec<Option<String>> = events.into_iter().map(|evt| Some(evt)).collect();
        query_params.insert("events".to_string(), opt_events);

        // build signed request
        let s3_req = S3Req {
            method: Method::GET,
            bucket: Some(bucket_name.to_string()),
            object: None,
            headers: HeaderMap::new(),
            query: query_params,
            body: Body::empty(),
            ts: time::now_utc(),
        };

        self.signed_req_future(s3_req, Ok(Body::empty()))
            .map(|resp| {
                // Read the whole body for bucket location response.
                resp.into_body()
                    .map_err(|e| Err::HyperErr(e))
                    .filter(|c| {
                        // filter out white spaces sent by the server to indicate it's still alive
                        c[0] != SPACE_BYTE[0]
                    })
                    .map(|chunk| {
                        // Split the chunk by lines and process.
                        // TODO: Handle case when partial lines are present in the chunk
                        let chunk_lines = String::from_utf8(chunk.to_vec())
                            .map(|p| {
                                let lines =
                                    p.lines().map(|s| s.to_string()).collect::<Vec<String>>();
                                stream::iter_ok(lines.into_iter())
                            })
                            .map_err(|e| Err::Utf8DecodingErr(e));
                        futures::future::result(chunk_lines).flatten_stream()
                    })
                    .flatten()
                    .map(|line| {
                        // Deserialize the notification
                        let notification_info: NotificationInfo =
                            serde_json::from_str(&line).unwrap();
                        notification_info
                    })
            })
            .flatten_stream()
    }
}
