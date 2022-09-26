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

use crate::s3::error::Error;
use crate::s3::types::{
    parse_legal_hold, Bucket, Item, LifecycleConfig, RetentionMode, SelectProgress, SseConfig,
};
use crate::s3::utils::{
    copy_slice, crc32, from_http_header_value, from_iso8601utc, get_text, uint32, UtcTime,
};
use reqwest::header::HeaderMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::BufReader;
use xmltree::Element;

#[derive(Debug)]
pub struct ListBucketsResponse {
    pub headers: HeaderMap,
    pub buckets: Vec<Bucket>,
}

#[derive(Debug)]
pub struct BucketResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
}

pub type MakeBucketResponse = BucketResponse;

pub type RemoveBucketResponse = BucketResponse;

#[derive(Debug)]
pub struct ObjectResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub object_name: String,
    pub version_id: Option<String>,
}

pub type RemoveObjectResponse = ObjectResponse;

#[derive(Debug)]
pub struct UploadIdResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub object_name: String,
    pub upload_id: String,
}

pub type AbortMultipartUploadResponse = UploadIdResponse;

pub type CreateMultipartUploadResponse = UploadIdResponse;

#[derive(Debug)]
pub struct PutObjectBaseResponse {
    pub headers: HeaderMap,
    pub bucket_name: String,
    pub object_name: String,
    pub location: String,
    pub etag: String,
    pub version_id: Option<String>,
}

pub type CompleteMultipartUploadResponse = PutObjectBaseResponse;

pub type PutObjectApiResponse = PutObjectBaseResponse;

pub type UploadPartResponse = PutObjectApiResponse;

pub type PutObjectResponse = PutObjectApiResponse;

pub type UploadPartCopyResponse = PutObjectApiResponse;

pub type CopyObjectResponse = PutObjectApiResponse;

pub type ComposeObjectResponse = PutObjectApiResponse;

#[derive(Debug)]
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
            size: size,
            etag: etag.to_string(),
            version_id: version_id,
            last_modified: last_modified,
            retention_mode: retention_mode,
            retention_retain_until_date: retention_retain_until_date,
            legal_hold: legal_hold,
            delete_marker: delete_marker,
            user_metadata: user_metadata,
        })
    }
}

#[derive(Clone, Debug)]
pub struct DeleteError {
    pub code: String,
    pub message: String,
    pub object_name: String,
    pub version_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct DeletedObject {
    pub name: String,
    pub version_id: Option<String>,
    pub delete_marker: bool,
    pub delete_marker_version_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RemoveObjectsApiResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub objects: Vec<DeletedObject>,
    pub errors: Vec<DeleteError>,
}

pub type RemoveObjectsResponse = RemoveObjectsApiResponse;

#[derive(Clone, Debug)]
pub struct ListObjectsV1Response {
    pub headers: HeaderMap,
    pub name: String,
    pub encoding_type: Option<String>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub is_truncated: bool,
    pub max_keys: Option<u16>,
    pub contents: Vec<Item>,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ListObjectsV2Response {
    pub headers: HeaderMap,
    pub name: String,
    pub encoding_type: Option<String>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub is_truncated: bool,
    pub max_keys: Option<u16>,
    pub contents: Vec<Item>,
    pub key_count: Option<u16>,
    pub start_after: Option<String>,
    pub continuation_token: Option<String>,
    pub next_continuation_token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ListObjectVersionsResponse {
    pub headers: HeaderMap,
    pub name: String,
    pub encoding_type: Option<String>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub is_truncated: bool,
    pub max_keys: Option<u16>,
    pub contents: Vec<Item>,
    pub key_marker: Option<String>,
    pub next_key_marker: Option<String>,
    pub version_id_marker: Option<String>,
    pub next_version_id_marker: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ListObjectsResponse {
    pub headers: HeaderMap,
    pub name: String,
    pub encoding_type: Option<String>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub is_truncated: bool,
    pub max_keys: Option<u16>,
    pub contents: Vec<Item>,

    // ListObjectsV1
    pub marker: String,
    pub next_marker: String,

    // ListObjectsV2
    pub key_count: u16,
    pub start_after: String,
    pub continuation_token: String,
    pub next_continuation_token: String,

    // ListObjectVersions
    pub key_marker: String,
    pub next_key_marker: String,
    pub version_id_marker: String,
    pub next_version_id_marker: String,
}

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
            headers: headers,
            region: region.to_string(),
            bucket_name: bucket_name.to_string(),
            object_name: object_name.to_string(),
            progress: SelectProgress {
                bytes_scanned: 0,
                bytes_progressed: 0,
                bytes_returned: 0,
            },
            resp: resp,
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
            self.prelude[i] = self.buf.pop_front().ok_or(Error::InsufficientData(8, i))?;
        }

        return Ok(true);
    }

    fn read_prelude_crc(&mut self) -> Result<bool, Error> {
        if self.buf.len() < 4 {
            return Ok(false);
        }

        self.prelude_crc_read = true;
        for i in 0..4 {
            self.prelude_crc[i] = self.buf.pop_front().ok_or(Error::InsufficientData(4, i))?;
        }

        return Ok(true);
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
                    .ok_or(Error::InsufficientData(data_length, i))?,
            );
        }

        return Ok(true);
    }

    fn read_message_crc(&mut self) -> Result<bool, Error> {
        if self.buf.len() < 4 {
            return Ok(false);
        }

        self.message_crc_read = true;
        for i in 0..4 {
            self.message_crc[i] = self.buf.pop_front().ok_or(Error::InsufficientData(4, i))?;
        }

        return Ok(true);
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

        return Ok(headers);
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
                    if self.payload.len() == 0 {
                        self.done = true;
                        return Ok(0);
                    }
                }
            }
        }
    }
}

pub struct ListenBucketNotificationResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
}

impl ListenBucketNotificationResponse {
    pub fn new(
        headers: HeaderMap,
        region: &str,
        bucket_name: &str,
    ) -> ListenBucketNotificationResponse {
        ListenBucketNotificationResponse {
            headers: headers,
            region: region.to_string(),
            bucket_name: bucket_name.to_string(),
        }
    }
}

pub type DeleteBucketEncryptionResponse = BucketResponse;

pub struct GetBucketEncryptionResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub config: SseConfig,
}

pub type SetBucketEncryptionResponse = BucketResponse;

pub type EnableObjectLegalHoldResponse = ObjectResponse;

pub type DisableObjectLegalHoldResponse = ObjectResponse;

pub struct IsObjectLegalHoldEnabledResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub object_name: String,
    pub version_id: Option<String>,
    pub enabled: bool,
}

pub type DeleteBucketLifecycleResponse = BucketResponse;

pub struct GetBucketLifecycleResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub config: LifecycleConfig,
}

pub type SetBucketLifecycleResponse = BucketResponse;
