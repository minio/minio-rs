// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2025 MinIO, Inc.
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
use crate::s3::types::{FromS3Response, S3Request, SelectProgress};
use crate::s3::utils::{copy_slice, crc32, get_text, uint32};
use async_trait::async_trait;
use http::HeaderMap;
use std::collections::{HashMap, VecDeque};
use std::io::BufReader;
use std::mem;
use xmltree::Element;

/// Response of
/// [select_object_content()](crate::s3::client::Client::select_object_content)
/// API
/// Response of [select_object_content()](crate::s3::client::Client::select_object_content) API
pub struct SelectObjectContentResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub object: String,
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
            length = ((b0 << 8) | b1) as usize;

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
                    return Err(std::io::Error::other(e.to_string()));
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

#[async_trait]
impl FromS3Response for SelectObjectContentResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let bucket = req
            .bucket
            .ok_or_else(|| Error::InvalidBucketName("no bucket specified".into()))?;
        let object = req
            .object
            .ok_or_else(|| Error::InvalidObjectName("no object specified".into()))?;

        let mut resp = resp?;

        Ok(SelectObjectContentResponse {
            headers: mem::take(resp.headers_mut()),
            region: req.inner_region,
            bucket: bucket,
            object: object,
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
        })
    }
}
