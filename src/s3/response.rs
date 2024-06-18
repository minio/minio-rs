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

//! Responses for [minio::s3::client::Client](crate::s3::client::Client) APIs

use std::collections::HashMap;
use std::collections::VecDeque;

use reqwest::header::HeaderMap;
use std::io::BufReader;
use xmltree::Element;

use crate::s3::error::Error;
use crate::s3::types::{
    parse_legal_hold, LifecycleConfig, NotificationConfig, ObjectLockConfig, ReplicationConfig,
    RetentionMode, SelectProgress, SseConfig,
};
use crate::s3::utils::{
    copy_slice, crc32, from_http_header_value, from_iso8601utc, get_text, uint32, UtcTime,
};

mod buckets;
mod get_object;
pub(crate) mod list_objects;
mod listen_bucket_notification;
mod put_object;
mod remove_objects;

pub use buckets::{GetBucketVersioningResponse, ListBucketsResponse};
pub use get_object::GetObjectResponse;
pub use list_objects::ListObjectsResponse;
pub use listen_bucket_notification::ListenBucketNotificationResponse;
pub use put_object::{
    AbortMultipartUploadResponse2, CompleteMultipartUploadResponse2,
    CreateMultipartUploadResponse2, PutObjectContentResponse, PutObjectResponse,
    UploadPartResponse2,
};
pub use remove_objects::{DeleteError, DeletedObject, RemoveObjectResponse, RemoveObjectsResponse};

#[derive(Debug)]
/// Base response for bucket operation
pub struct BucketResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
}

/// Response of [make_bucket()](crate::s3::client::Client::make_bucket) API
pub type MakeBucketResponse = BucketResponse;

/// Response of [remove_bucket()](crate::s3::client::Client::remove_bucket) API
pub type RemoveBucketResponse = BucketResponse;

#[derive(Debug)]
/// Base response for object operation
pub struct ObjectResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub object_name: String,
    pub version_id: Option<String>,
}

#[derive(Debug)]
/// Base Upload ID response
pub struct UploadIdResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub object_name: String,
    pub upload_id: String,
}

/// Response of [abort_multipart_upload()](crate::s3::client::Client::abort_multipart_upload) API
pub type AbortMultipartUploadResponse = UploadIdResponse;

/// Response of [create_multipart_upload()](crate::s3::client::Client::create_multipart_upload) API
pub type CreateMultipartUploadResponse = UploadIdResponse;

#[derive(Debug)]
/// Base response for put object
pub struct PutObjectBaseResponse {
    pub headers: HeaderMap,
    pub bucket_name: String,
    pub object_name: String,
    pub location: String,
    pub etag: String,
    pub version_id: Option<String>,
}

/// Response of [complete_multipart_upload()](crate::s3::client::Client::complete_multipart_upload) API
pub type CompleteMultipartUploadResponse = PutObjectBaseResponse;

/// Response of [put_object_api()](crate::s3::client::Client::put_object_api) S3 API
pub type PutObjectApiResponse = PutObjectBaseResponse;

/// Response of [upload_part()](crate::s3::client::Client::upload_part) S3 API
pub type UploadPartResponse = PutObjectApiResponse;

/// Response of [put_object()](crate::s3::client::Client::put_object) API
pub type PutObjectResponseOld = PutObjectApiResponse;

/// Response of [upload_part_copy()](crate::s3::client::Client::upload_part_copy) S3 API
pub type UploadPartCopyResponse = PutObjectApiResponse;

/// Response of [copy_object()](crate::s3::client::Client::copy_object) API
pub type CopyObjectResponse = PutObjectApiResponse;

/// Response of [compose_object()](crate::s3::client::Client::compose_object) API
pub type ComposeObjectResponse = PutObjectApiResponse;

/// Response of [upload_object()](crate::s3::client::Client::upload_object) API
pub type UploadObjectResponse = PutObjectApiResponse;

#[derive(Debug)]
/// Response of [stat_object()](crate::s3::client::Client::stat_object) API
pub struct StatObjectResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub object_name: String,
    pub size: usize,
    pub etag: String,
    pub version_id: Option<String>,
    pub last_modified: Option<UtcTime>,
    pub retention_mode: Option<RetentionMode>,
    pub retention_retain_until_date: Option<UtcTime>,
    pub legal_hold: Option<bool>,
    pub delete_marker: Option<bool>,
    pub user_metadata: HashMap<String, String>,
}

impl StatObjectResponse {
    pub fn new(
        headers: &HeaderMap,
        region: &str,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<StatObjectResponse, Error> {
        let size = match headers.get("Content-Length") {
            Some(v) => v.to_str()?.parse::<usize>()?,
            None => 0_usize,
        };

        let etag = match headers.get("ETag") {
            Some(v) => v.to_str()?.trim_matches('"'),
            None => "",
        };

        let version_id = match headers.get("x-amz-version-id") {
            Some(v) => Some(v.to_str()?.to_string()),
            None => None,
        };

        let last_modified = match headers.get("Last-Modified") {
            Some(v) => Some(from_http_header_value(v.to_str()?)?),
            None => None,
        };

        let retention_mode = match headers.get("x-amz-object-lock-mode") {
            Some(v) => Some(RetentionMode::parse(v.to_str()?)?),
            None => None,
        };

        let retention_retain_until_date = match headers.get("x-amz-object-lock-retain-until-date") {
            Some(v) => Some(from_iso8601utc(v.to_str()?)?),
            None => None,
        };

        let legal_hold = match headers.get("x-amz-object-lock-legal-hold") {
            Some(v) => Some(parse_legal_hold(v.to_str()?)?),
            None => None,
        };

        let delete_marker = match headers.get("x-amz-delete-marker") {
            Some(v) => Some(v.to_str()?.parse::<bool>()?),
            None => None,
        };

        let mut user_metadata: HashMap<String, String> = HashMap::new();
        for (key, value) in headers.iter() {
            if let Some(v) = key.as_str().strip_prefix("x-amz-meta-") {
                user_metadata.insert(v.to_string(), value.to_str()?.to_string());
            }
        }

        Ok(StatObjectResponse {
            headers: headers.clone(),
            region: region.to_string(),
            bucket_name: bucket_name.to_string(),
            object_name: object_name.to_string(),
            size,
            etag: etag.to_string(),
            version_id,
            last_modified,
            retention_mode,
            retention_retain_until_date,
            legal_hold,
            delete_marker,
            user_metadata,
        })
    }
}

/// Response of [select_object_content()](crate::s3::client::Client::select_object_content) API
pub struct SelectObjectContentResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub object_name: String,
    pub progress: SelectProgress,

    resp: reqwest::Response,

    done: bool,
    buf: VecDeque<u8>,

    prelude: [u8; 8],
    prelude_read: bool,

    prelude_crc: [u8; 4],
    prelude_crc_read: bool,

    total_length: usize,

    data: Vec<u8>,
    data_read: bool,

    message_crc: [u8; 4],
    message_crc_read: bool,

    payload: Vec<u8>,
    payload_index: usize,
}

impl SelectObjectContentResponse {
    pub fn new(
        resp: reqwest::Response,
        region: &str,
        bucket_name: &str,
        object_name: &str,
    ) -> SelectObjectContentResponse {
        let headers = resp.headers().clone();

        SelectObjectContentResponse {
            headers,
            region: region.to_string(),
            bucket_name: bucket_name.to_string(),
            object_name: object_name.to_string(),
            progress: SelectProgress {
                bytes_scanned: 0,
                bytes_progressed: 0,
                bytes_returned: 0,
            },
            resp,
            done: false,
            buf: VecDeque::<u8>::new(),
            prelude: [0_u8; 8],
            prelude_read: false,
            prelude_crc: [0_u8; 4],
            prelude_crc_read: false,
            total_length: 0_usize,
            data: Vec::<u8>::new(),
            data_read: false,
            message_crc: [0_u8; 4],
            message_crc_read: false,
            payload: Vec::<u8>::new(),
            payload_index: 0,
        }
    }

    fn reset(&mut self) {
        self.buf.clear();

        self.data.clear();
        self.data_read = false;

        self.prelude_read = false;
        self.prelude_crc_read = false;
        self.message_crc_read = false;
    }

    fn read_prelude(&mut self) -> Result<bool, Error> {
        if self.buf.len() < 8 {
            return Ok(false);
        }

        self.prelude_read = true;
        for i in 0..8 {
            self.prelude[i] = self
                .buf
                .pop_front()
                .ok_or(Error::InsufficientData(8, i as u64))?;
        }

        Ok(true)
    }

    fn read_prelude_crc(&mut self) -> Result<bool, Error> {
        if self.buf.len() < 4 {
            return Ok(false);
        }

        self.prelude_crc_read = true;
        for i in 0..4 {
            self.prelude_crc[i] = self
                .buf
                .pop_front()
                .ok_or(Error::InsufficientData(4, i as u64))?;
        }

        Ok(true)
    }

    fn read_data(&mut self) -> Result<bool, Error> {
        let data_length = self.total_length - 8 - 4 - 4;
        if self.buf.len() < data_length {
            return Ok(false);
        }

        self.data = Vec::new();

        self.data_read = true;
        for i in 0..data_length {
            self.data.push(
                self.buf
                    .pop_front()
                    .ok_or(Error::InsufficientData(data_length as u64, i as u64))?,
            );
        }

        Ok(true)
    }

    fn read_message_crc(&mut self) -> Result<bool, Error> {
        if self.buf.len() < 4 {
            return Ok(false);
        }

        self.message_crc_read = true;
        for i in 0..4 {
            self.message_crc[i] = self
                .buf
                .pop_front()
                .ok_or(Error::InsufficientData(4, i as u64))?;
        }

        Ok(true)
    }

    fn decode_header(&mut self, header_length: usize) -> Result<HashMap<String, String>, Error> {
        let mut headers: HashMap<String, String> = HashMap::new();
        let mut offset = 0_usize;
        while offset < header_length {
            let mut length = self.data[offset] as usize;
            offset += 1;
            if length == 0 {
                break;
            }

            let name = String::from_utf8(self.data[offset..offset + length].to_vec())?;
            offset += length;
            if self.data[offset] != 7 {
                return Err(Error::InvalidHeaderValueType(self.data[offset]));
            }
            offset += 1;

            let b0 = self.data[offset] as u16;
            offset += 1;
            let b1 = self.data[offset] as u16;
            offset += 1;
            length = (b0 << 8 | b1) as usize;

            let value = String::from_utf8(self.data[offset..offset + length].to_vec())?;
            offset += length;

            headers.insert(name, value);
        }

        Ok(headers)
    }

    async fn do_read(&mut self) -> Result<(), Error> {
        if self.done {
            return Ok(());
        }

        loop {
            let chunk = match self.resp.chunk().await? {
                Some(v) => v,
                None => return Ok(()),
            };

            self.buf.extend(chunk.iter().copied());

            if !self.prelude_read && !self.read_prelude()? {
                continue;
            }

            if !self.prelude_crc_read {
                if !self.read_prelude_crc()? {
                    continue;
                }

                let got = crc32(&self.prelude);
                let expected = uint32(&self.prelude_crc)?;
                if got != expected {
                    self.done = true;
                    return Err(Error::CrcMismatch(String::from("prelude"), expected, got));
                }

                self.total_length = uint32(&self.prelude[0..4])? as usize;
            }

            if !self.data_read && !self.read_data()? {
                continue;
            }

            if !self.message_crc_read {
                if !self.read_message_crc()? {
                    continue;
                }

                let mut message: Vec<u8> = Vec::new();
                message.extend_from_slice(&self.prelude);
                message.extend_from_slice(&self.prelude_crc);
                message.extend_from_slice(&self.data);

                let got = crc32(&message);
                let expected = uint32(&self.message_crc)?;
                if got != expected {
                    self.done = true;
                    return Err(Error::CrcMismatch(String::from("message"), expected, got));
                }
            }

            let header_length = uint32(&self.prelude[4..])? as usize;
            let headers = self.decode_header(header_length)?;
            let value = match headers.get(":message-type") {
                Some(v) => v.as_str(),
                None => "",
            };
            if value == "error" {
                self.done = true;
                return Err(Error::SelectError(
                    match headers.get(":error-code") {
                        Some(v) => v.clone(),
                        None => String::new(),
                    },
                    match headers.get(":error-message") {
                        Some(v) => v.clone(),
                        None => String::new(),
                    },
                ));
            }

            let event_type = match headers.get(":event-type") {
                Some(v) => v.as_str(),
                None => "",
            };

            if event_type == "End" {
                self.done = true;
                return Ok(());
            }

            let payload_length = self.total_length - header_length - 16;
            if event_type == "Cont" || payload_length < 1 {
                self.reset();
                continue;
            }

            let payload = &self.data[header_length..(header_length + payload_length)];
            if event_type == "Progress" || event_type == "Stats" {
                let root = Element::parse(&mut BufReader::new(payload))?;
                self.reset();
                self.progress = SelectProgress {
                    bytes_scanned: get_text(&root, "BytesScanned")?.parse::<usize>()?,
                    bytes_progressed: get_text(&root, "BytesProcessed")?.parse::<usize>()?,
                    bytes_returned: get_text(&root, "BytesReturned")?.parse::<usize>()?,
                };
                continue;
            }

            if event_type == "Records" {
                self.payload = payload.to_vec();
                self.payload_index = 0;
                self.reset();
                return Ok(());
            }

            self.done = true;
            return Err(Error::UnknownEventType(event_type.to_string()));
        }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.done {
                return Ok(0);
            }

            if self.payload_index < self.payload.len() {
                let n = copy_slice(buf, &self.payload[self.payload_index..]);

                self.payload_index += n;
                if self.payload_index > self.payload.len() {
                    self.payload_index = self.payload.len();
                }

                return Ok(n);
            }

            self.payload.clear();
            self.payload_index = 0;

            match self.do_read().await {
                Err(e) => {
                    self.done = true;
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ));
                }
                Ok(_) => {
                    if self.payload.is_empty() {
                        self.done = true;
                        return Ok(0);
                    }
                }
            }
        }
    }
}

/// Response of [delete_bucket_encryption()](crate::s3::client::Client::delete_bucket_encryption) API
pub type DeleteBucketEncryptionResponse = BucketResponse;

#[derive(Clone, Debug)]
/// Response of [get_bucket_encryption()](crate::s3::client::Client::get_bucket_encryption) API
pub struct GetBucketEncryptionResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub config: SseConfig,
}

/// Response of [set_bucket_encryption()](crate::s3::client::Client::set_bucket_encryption) API
pub type SetBucketEncryptionResponse = BucketResponse;

/// Response of [enable_object_legal_hold()](crate::s3::client::Client::enable_object_legal_hold) API
pub type EnableObjectLegalHoldResponse = ObjectResponse;

/// Response of [disable_object_legal_hold()](crate::s3::client::Client::disable_object_legal_hold) API
pub type DisableObjectLegalHoldResponse = ObjectResponse;

#[derive(Clone, Debug)]
/// Response of [is_object_legal_hold_enabled()](crate::s3::client::Client::is_object_legal_hold_enabled) API
pub struct IsObjectLegalHoldEnabledResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub object_name: String,
    pub version_id: Option<String>,
    pub enabled: bool,
}

/// Response of [delete_bucket_lifecycle()](crate::s3::client::Client::delete_bucket_lifecycle) API
pub type DeleteBucketLifecycleResponse = BucketResponse;

#[derive(Clone, Debug)]
/// Response of [get_bucket_lifecycle()](crate::s3::client::Client::get_bucket_lifecycle) API
pub struct GetBucketLifecycleResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub config: LifecycleConfig,
}

/// Response of [set_bucket_lifecycle()](crate::s3::client::Client::set_bucket_lifecycle) API
pub type SetBucketLifecycleResponse = BucketResponse;

/// Response of [delete_bucket_notification()](crate::s3::client::Client::delete_bucket_notification) API
pub type DeleteBucketNotificationResponse = BucketResponse;

#[derive(Clone, Debug)]
/// Response of [get_bucket_notification()](crate::s3::client::Client::get_bucket_notification) API
pub struct GetBucketNotificationResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub config: NotificationConfig,
}

/// Response of [set_bucket_notification()](crate::s3::client::Client::set_bucket_notification) API
pub type SetBucketNotificationResponse = BucketResponse;

/// Response of [delete_bucket_policy()](crate::s3::client::Client::delete_bucket_policy) API
pub type DeleteBucketPolicyResponse = BucketResponse;

#[derive(Clone, Debug)]
/// Response of [get_bucket_policy()](crate::s3::client::Client::get_bucket_policy) API
pub struct GetBucketPolicyResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub config: String,
}

/// Response of [set_bucket_policy()](crate::s3::client::Client::set_bucket_policy) API
pub type SetBucketPolicyResponse = BucketResponse;

/// Response of [delete_bucket_replication()](crate::s3::client::Client::delete_bucket_replication) API
pub type DeleteBucketReplicationResponse = BucketResponse;

#[derive(Clone, Debug)]
/// Response of [get_bucket_replication()](crate::s3::client::Client::get_bucket_replication) API
pub struct GetBucketReplicationResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub config: ReplicationConfig,
}

/// Response of [set_bucket_replication()](crate::s3::client::Client::set_bucket_replication) API
pub type SetBucketReplicationResponse = BucketResponse;

/// Response of [delete_bucket_tags()](crate::s3::client::Client::delete_bucket_tags) API
pub type DeleteBucketTagsResponse = BucketResponse;

#[derive(Clone, Debug)]
/// Response of [get_bucket_tags()](crate::s3::client::Client::get_bucket_tags) API
pub struct GetBucketTagsResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub tags: std::collections::HashMap<String, String>,
}

/// Response of [set_bucket_tags()](crate::s3::client::Client::set_bucket_tags) API
pub type SetBucketTagsResponse = BucketResponse;

/// Response of [set_bucket_versioning()](crate::s3::client::Client::set_bucket_versioning) API
pub type SetBucketVersioningResponse = BucketResponse;

/// Response of [delete_object_lock_config()](crate::s3::client::Client::delete_object_lock_config) API
pub type DeleteObjectLockConfigResponse = BucketResponse;

#[derive(Clone, Debug)]
/// Response of [get_object_lock_config()](crate::s3::client::Client::get_object_lock_config) API
pub struct GetObjectLockConfigResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub config: ObjectLockConfig,
}

/// Response of [set_object_lock_config()](crate::s3::client::Client::set_object_lock_config) API
pub type SetObjectLockConfigResponse = BucketResponse;

#[derive(Clone, Debug)]
/// Response of [get_object_retention()](crate::s3::client::Client::get_object_retention) API
pub struct GetObjectRetentionResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub object_name: String,
    pub version_id: Option<String>,
    pub retention_mode: Option<RetentionMode>,
    pub retain_until_date: Option<UtcTime>,
}

/// Response of [set_object_retention()](crate::s3::client::Client::set_object_retention) API
pub type SetObjectRetentionResponse = ObjectResponse;

/// Response of [delete_object_tags()](crate::s3::client::Client::delete_object_tags) API
pub type DeleteObjectTagsResponse = ObjectResponse;

#[derive(Clone, Debug)]
/// Response of [get_object_tags()](crate::s3::client::Client::get_object_tags) API
pub struct GetObjectTagsResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub object_name: String,
    pub version_id: Option<String>,
    pub tags: std::collections::HashMap<String, String>,
}

/// Response of [set_object_tags()](crate::s3::client::Client::set_object_tags) API
pub type SetObjectTagsResponse = ObjectResponse;

#[derive(Clone, Debug)]
/// Response of [get_presigned_object_url()](crate::s3::client::Client::get_presigned_object_url) API
pub struct GetPresignedObjectUrlResponse {
    pub region: String,
    pub bucket_name: String,
    pub object_name: String,
    pub version_id: Option<String>,
    pub url: String,
}

#[derive(Clone, Debug)]
/// Response of [download_object()](crate::s3::client::Client::download_object) API
pub struct DownloadObjectResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub object_name: String,
    pub version_id: Option<String>,
}
