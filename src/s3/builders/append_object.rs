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

use crate::s3::Client;
use crate::s3::builders::{
    ContentStream, MAX_MULTIPART_COUNT, ObjectContent, Size, calc_part_info,
};
use crate::s3::error::Error;
use crate::s3::response::{AppendObjectResponse, StatObjectResponse};
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::sse::Sse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{Multimap, check_bucket_name, check_object_name};
use http::Method;
use std::sync::Arc;

// region: append-object
#[derive(Debug, Clone, Default)]
pub struct AppendObject {
    client: Arc<Client>,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    bucket: String,
    object: String,

    region: Option<String>,
    sse: Option<Arc<dyn Sse>>,
    data: SegmentedBytes,

    /// value of x-amz-write-offset-bytes
    offset_bytes: u64,
}

impl AppendObject {
    pub fn new(client: &Arc<Client>, bucket: &str, object: &str, data: SegmentedBytes) -> Self {
        AppendObject {
            client: Arc::clone(client),
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            data,
            ..Default::default()
        }
    }

    pub fn extra_headers(mut self, extra_headers: Option<Multimap>) -> Self {
        self.extra_headers = extra_headers;
        self
    }

    pub fn extra_query_params(mut self, extra_query_params: Option<Multimap>) -> Self {
        self.extra_query_params = extra_query_params;
        self
    }

    pub fn offset_bytes(mut self, offset_bytes: u64) -> Self {
        self.offset_bytes = offset_bytes;
        self
    }
}

impl S3Api for AppendObject {
    type S3Response = AppendObjectResponse;
}

impl ToS3Request for AppendObject {
    fn to_s3request(self) -> Result<S3Request, Error> {
        {
            check_bucket_name(&self.bucket, true)?;
            check_object_name(&self.object)?;

            if let Some(v) = &self.sse {
                if v.tls_required() && !self.client.is_secure() {
                    return Err(Error::SseTlsRequired(None));
                }
            }
        }

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        headers.insert(
            "x-amz-write-offset-bytes".into(),
            self.offset_bytes.to_string(),
        );

        Ok(S3Request::new(self.client, Method::PUT)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(self.extra_query_params.unwrap_or_default())
            .object(Some(self.object))
            .headers(headers)
            .body(Some(self.data)))
    }
}
// endregion: append-object

// region: append-object-content

/// AppendObjectContent takes a `ObjectContent` stream and appends it to MinIO/S3.
///
/// It is a higher level API and handles multipart appends transparently.
pub struct AppendObjectContent {
    client: Arc<Client>,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,
    object: String,
    sse: Option<Arc<dyn Sse>>,
    part_size: Size,

    // source data
    input_content: ObjectContent,

    // Computed.
    content_stream: ContentStream,
    part_count: Option<u16>,

    /// Value of x-amz-write-offset-bytes
    offset_bytes: u64,
}

impl AppendObjectContent {
    pub fn new(
        client: &Arc<Client>,
        bucket: &str,
        object: &str,
        content: impl Into<ObjectContent>,
    ) -> Self {
        AppendObjectContent {
            client: Arc::clone(client),
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            input_content: content.into(),
            extra_headers: None,
            extra_query_params: None,
            region: None,
            sse: None,
            part_size: Size::Unknown,
            content_stream: ContentStream::empty(),
            part_count: None,
            offset_bytes: 0,
        }
    }

    pub fn extra_headers(mut self, extra_headers: Option<Multimap>) -> Self {
        self.extra_headers = extra_headers;
        self
    }

    pub fn extra_query_params(mut self, extra_query_params: Option<Multimap>) -> Self {
        self.extra_query_params = extra_query_params;
        self
    }

    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn part_size(mut self, part_size: impl Into<Size>) -> Self {
        self.part_size = part_size.into();
        self
    }

    pub fn offset_bytes(mut self, offset_bytes: u64) -> Self {
        self.offset_bytes = offset_bytes;
        self
    }

    pub async fn send(mut self) -> Result<AppendObjectResponse, Error> {
        {
            check_bucket_name(&self.bucket, true)?;
            check_object_name(&self.object)?;
            if let Some(v) = &self.sse {
                if v.tls_required() && !self.client.is_secure() {
                    return Err(Error::SseTlsRequired(None));
                }
            }
        }

        {
            let mut headers: Multimap = match self.extra_headers {
                Some(ref headers) => headers.clone(),
                None => Multimap::new(),
            };
            headers.insert(
                String::from("x-amz-write-offset-bytes"),
                self.offset_bytes.to_string(),
            );
            self.extra_query_params = Some(headers);
        }

        self.content_stream = std::mem::take(&mut self.input_content)
            .to_content_stream()
            .await
            .map_err(Error::IOError)?;

        // object_size may be Size::Unknown.
        let object_size = self.content_stream.get_size();

        let (part_size, n_expected_parts) = calc_part_info(object_size, self.part_size)?;
        // Set the chosen part size and part count.
        self.part_size = Size::Known(part_size);
        self.part_count = n_expected_parts;

        // Read the first part.
        let seg_bytes = self.content_stream.read_upto(part_size as usize).await?;

        // get the length (if any) of the current file
        let resp: StatObjectResponse = self
            .client
            .stat_object(&self.bucket, &self.object)
            .send()
            .await?;
        println!("statObjectResponse={:#?}", resp);

        let current_file_size = resp.size;

        // In the first part read, if:
        //
        //   - object_size is unknown AND we got less than the part size, OR
        //   - we are expecting only one part to be uploaded,
        //
        // we upload it as a simple put object.
        if (object_size.is_unknown() && (seg_bytes.len() as u64) < part_size)
            || n_expected_parts == Some(1)
        {
            let ao = AppendObject {
                client: self.client,
                extra_headers: self.extra_headers,
                extra_query_params: self.extra_query_params,
                bucket: self.bucket,
                object: self.object,
                region: self.region,
                offset_bytes: current_file_size,
                sse: self.sse,
                data: seg_bytes,
            };
            ao.send().await
        } else if object_size.is_known() && (seg_bytes.len() as u64) < part_size {
            // Not enough data!
            let expected = object_size.as_u64().unwrap();
            let got = seg_bytes.len() as u64;
            Err(Error::InsufficientData(expected, got))
        } else {
            // Otherwise, we start a multipart append.
            self.send_mpa(part_size, current_file_size, seg_bytes).await
        }
    }

    /// multipart append
    async fn send_mpa(
        &mut self,
        part_size: u64,
        object_size: u64,
        first_part: SegmentedBytes,
    ) -> Result<AppendObjectResponse, Error> {
        let mut done = false;
        let mut part_number = 0;

        let mut last_resp: Option<AppendObjectResponse> = None;
        let mut next_offset_bytes: u64 = object_size;
        println!("initial offset_bytes: {}", next_offset_bytes);

        let mut first_part = Some(first_part);
        while !done {
            let part_content: SegmentedBytes = {
                if let Some(v) = first_part.take() {
                    v
                } else {
                    self.content_stream.read_upto(part_size as usize).await?
                }
            };
            part_number += 1;
            let buffer_size = part_content.len() as u64;

            assert!(
                buffer_size <= part_size,
                "{:?} <= {:?}",
                buffer_size,
                part_size
            );

            if buffer_size == 0 && part_number > 1 {
                // We are done as we appended at least 1 part and we have
                // reached the end of the stream.
                break;
            }

            // Check if we have too many parts to upload.
            if self.part_count.is_none() && part_number > MAX_MULTIPART_COUNT {
                return Err(Error::TooManyParts);
            }

            // Append the part now.
            let append_object = AppendObject {
                client: self.client.clone(),
                extra_headers: self.extra_headers.clone(),
                extra_query_params: self.extra_query_params.clone(),
                bucket: self.bucket.clone(),
                object: self.object.clone(),
                region: self.region.clone(),
                sse: self.sse.clone(),
                data: part_content,
                offset_bytes: next_offset_bytes,
            };
            let resp: AppendObjectResponse = append_object.send().await?;
            println!("AppendObjectResponse: object_size={:?}", resp.object_size);

            next_offset_bytes = resp.object_size;

            // Finally check if we are done.
            if buffer_size < part_size {
                done = true;
                last_resp = Some(resp);
            }
        }
        Ok(last_resp.unwrap())
    }
}
// endregion: append-object-content
