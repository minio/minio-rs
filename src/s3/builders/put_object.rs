// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2023 MinIO, Inc.
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

use super::ObjectContent;
use crate::s3::multimap::{Multimap, MultimapExt};
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::utils::{check_object_name, insert};
use crate::s3::{
    builders::{ContentStream, Size},
    client::Client,
    error::Error,
    response::{
        AbortMultipartUploadResponse, CompleteMultipartUploadResponse,
        CreateMultipartUploadResponse, PutObjectContentResponse, PutObjectResponse,
        UploadPartResponse,
    },
    sse::Sse,
    types::{PartInfo, Retention, S3Api, S3Request, ToS3Request},
    utils::{check_bucket_name, md5sum_hash, to_iso8601utc, urlencode},
};
use bytes::{Bytes, BytesMut};
use http::Method;
use std::{collections::HashMap, sync::Arc};
// region: multipart-upload

/// Argument for
/// [create_multipart_upload()](crate::s3::client::Client::create_multipart_upload)
/// API
#[derive(Clone, Debug, Default)]
pub struct CreateMultipartUpload {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,
    object: String,

    user_metadata: Option<Multimap>,
    sse: Option<Arc<dyn Sse>>,
    tags: Option<HashMap<String, String>>,
    retention: Option<Retention>,
    legal_hold: bool,
    content_type: Option<String>,
}

impl CreateMultipartUpload {
    pub fn new(client: Client, bucket: String, object: String) -> Self {
        CreateMultipartUpload {
            client,
            bucket,
            object,
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

    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn user_metadata(mut self, user_metadata: Option<Multimap>) -> Self {
        self.user_metadata = user_metadata;
        self
    }

    pub fn sse(mut self, sse: Option<Arc<dyn Sse>>) -> Self {
        self.sse = sse;
        self
    }

    pub fn tags(mut self, tags: Option<HashMap<String, String>>) -> Self {
        self.tags = tags;
        self
    }

    pub fn retention(mut self, retention: Option<Retention>) -> Self {
        self.retention = retention;
        self
    }

    pub fn legal_hold(mut self, legal_hold: bool) -> Self {
        self.legal_hold = legal_hold;
        self
    }

    pub fn content_type(mut self, content_type: Option<String>) -> Self {
        self.content_type = content_type;
        self
    }
}

impl S3Api for CreateMultipartUpload {
    type S3Response = CreateMultipartUploadResponse;
}

impl ToS3Request for CreateMultipartUpload {
    fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;
        check_object_name(&self.object)?;

        let headers: Multimap = into_headers_put_object(
            self.extra_headers,
            self.user_metadata,
            self.sse,
            self.tags,
            self.retention,
            self.legal_hold,
            self.content_type,
        )?;

        Ok(S3Request::new(self.client, Method::POST)
            .region(self.region)
            .bucket(Some(self.bucket))
            .object(Some(self.object))
            .query_params(insert(self.extra_query_params, "uploads"))
            .headers(headers))
    }
}

// endregion: multipart-upload

// region: abort-multipart-upload

/// Argument for
/// [abort_multipart_upload()](crate::s3::client::Client::abort_multipart_upload)
/// API
#[derive(Clone, Debug, Default)]
pub struct AbortMultipartUpload {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    object: String,
    upload_id: String,
}

impl AbortMultipartUpload {
    pub fn new(client: Client, bucket: String, object: String, upload_id: String) -> Self {
        Self {
            client,
            bucket,
            object,
            upload_id,
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

    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }
}

impl S3Api for AbortMultipartUpload {
    type S3Response = AbortMultipartUploadResponse;
}

impl ToS3Request for AbortMultipartUpload {
    fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;
        check_object_name(&self.object)?;

        let headers: Multimap = self.extra_headers.unwrap_or_default();
        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        query_params.add("uploadId", urlencode(&self.upload_id).to_string());

        Ok(S3Request::new(self.client, Method::DELETE)
            .region(self.region)
            .bucket(Some(self.bucket))
            .object(Some(self.object))
            .query_params(query_params)
            .headers(headers))
    }
}

// endregion: abort-multipart-upload

// region: complete-multipart-upload

/// Argument for
/// [complete_multipart_upload()](crate::s3::client::Client::complete_multipart_upload)
/// API
#[derive(Clone, Debug, Default)]
pub struct CompleteMultipartUpload {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,
    object: String,
    upload_id: String,
    parts: Vec<PartInfo>,
}

impl S3Api for CompleteMultipartUpload {
    type S3Response = CompleteMultipartUploadResponse;
}

impl CompleteMultipartUpload {
    pub fn new(
        client: Client,
        bucket: String,
        object: String,
        upload_id: String,
        parts: Vec<PartInfo>,
    ) -> Self {
        Self {
            client,
            bucket,
            object,
            upload_id,
            parts,
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

    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }
}

impl ToS3Request for CompleteMultipartUpload {
    fn to_s3request(self) -> Result<S3Request, Error> {
        {
            check_bucket_name(&self.bucket, true)?;
            check_object_name(&self.object)?;
            if self.upload_id.is_empty() {
                return Err(Error::InvalidUploadId("upload ID cannot be empty".into()));
            }
            if self.parts.is_empty() {
                return Err(Error::EmptyParts("parts cannot be empty".into()));
            }
        }

        // Set capacity of the byte-buffer based on the part count - attempting
        // to avoid extra allocations when building the XML payload.
        let data: Bytes = {
            let mut data = BytesMut::with_capacity(100 * self.parts.len() + 100);
            data.extend_from_slice(b"<CompleteMultipartUpload>");
            for part in self.parts.iter() {
                data.extend_from_slice(b"<Part><PartNumber>");
                data.extend_from_slice(part.number.to_string().as_bytes());
                data.extend_from_slice(b"</PartNumber><ETag>");
                data.extend_from_slice(part.etag.as_bytes());
                data.extend_from_slice(b"</ETag></Part>");
            }
            data.extend_from_slice(b"</CompleteMultipartUpload>");
            data.freeze()
        };

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        {
            headers.add("Content-Type", "application/xml");
            headers.add("Content-MD5", md5sum_hash(data.as_ref()));
        }
        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        query_params.add("uploadId", self.upload_id);

        Ok(S3Request::new(self.client, Method::POST)
            .region(self.region)
            .bucket(Some(self.bucket))
            .object(Some(self.object))
            .query_params(query_params)
            .headers(headers)
            .body(Some(data.into())))
    }
}
// endregion: complete-multipart-upload

// region: upload-part

/// Argument for [upload_part()](crate::s3::client::Client::upload_part) S3 API
#[derive(Debug, Clone, Default)]
pub struct UploadPart {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    bucket: String,
    object: String,
    region: Option<String>,
    sse: Option<Arc<dyn Sse>>,
    tags: Option<HashMap<String, String>>,
    retention: Option<Retention>,
    legal_hold: bool,
    data: SegmentedBytes,
    content_type: Option<String>,

    // This is used only when this struct is used for PutObject.
    user_metadata: Option<Multimap>,

    // These are only used for multipart UploadPart but not for PutObject, so
    // they are optional.
    upload_id: Option<String>,
    part_number: Option<u16>,
}

impl UploadPart {
    pub fn new(
        client: Client,
        bucket: String,
        object: String,
        upload_id: String,
        part_number: u16,
        data: SegmentedBytes,
    ) -> Self {
        Self {
            client,
            bucket,
            object,
            upload_id: Some(upload_id),
            part_number: Some(part_number),
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

    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn sse(mut self, sse: Option<Arc<dyn Sse>>) -> Self {
        self.sse = sse;
        self
    }

    pub fn tags(mut self, tags: Option<HashMap<String, String>>) -> Self {
        self.tags = tags;
        self
    }

    pub fn retention(mut self, retention: Option<Retention>) -> Self {
        self.retention = retention;
        self
    }

    pub fn legal_hold(mut self, legal_hold: bool) -> Self {
        self.legal_hold = legal_hold;
        self
    }
}

impl S3Api for UploadPart {
    type S3Response = UploadPartResponse;
}

impl ToS3Request for UploadPart {
    fn to_s3request(self) -> Result<S3Request, Error> {
        {
            check_bucket_name(&self.bucket, true)?;
            check_object_name(&self.object)?;

            if let Some(upload_id) = &self.upload_id {
                if upload_id.is_empty() {
                    return Err(Error::InvalidUploadId("upload ID cannot be empty".into()));
                }
            }
            if let Some(part_number) = self.part_number {
                if !(1..=MAX_MULTIPART_COUNT).contains(&part_number) {
                    return Err(Error::InvalidPartNumber(format!(
                        "part number must be between 1 and {}",
                        MAX_MULTIPART_COUNT
                    )));
                }
            }
        }

        let headers: Multimap = into_headers_put_object(
            self.extra_headers,
            self.user_metadata,
            self.sse,
            self.tags,
            self.retention,
            self.legal_hold,
            self.content_type,
        )?;

        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();

        if let Some(upload_id) = self.upload_id {
            query_params.add("uploadId", upload_id);
        }
        if let Some(part_number) = self.part_number {
            query_params.add("partNumber", part_number.to_string());
        }

        Ok(S3Request::new(self.client, Method::PUT)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(query_params)
            .object(Some(self.object))
            .headers(headers)
            .body(Some(self.data)))
    }
}

// endregion: upload-part

// region: put-object

/// Argument builder for PutObject S3 API. This is a lower-level API.
#[derive(Debug, Clone, Default)]
pub struct PutObject(UploadPart);

impl PutObject {
    pub fn new(client: Client, bucket: String, object: String, data: SegmentedBytes) -> Self {
        PutObject(UploadPart {
            client,
            bucket,
            object,
            data,
            ..Default::default()
        })
    }

    pub fn extra_headers(mut self, extra_headers: Option<Multimap>) -> Self {
        self.0.extra_headers = extra_headers;
        self
    }

    pub fn extra_query_params(mut self, extra_query_params: Option<Multimap>) -> Self {
        self.0.extra_query_params = extra_query_params;
        self
    }

    pub fn region(mut self, region: Option<String>) -> Self {
        self.0.region = region;
        self
    }

    pub fn user_metadata(mut self, user_metadata: Option<Multimap>) -> Self {
        self.0.user_metadata = user_metadata;
        self
    }

    pub fn sse(mut self, sse: Option<Arc<dyn Sse>>) -> Self {
        self.0.sse = sse;
        self
    }

    pub fn tags(mut self, tags: Option<HashMap<String, String>>) -> Self {
        self.0.tags = tags;
        self
    }

    pub fn retention(mut self, retention: Option<Retention>) -> Self {
        self.0.retention = retention;
        self
    }

    pub fn legal_hold(mut self, legal_hold: bool) -> Self {
        self.0.legal_hold = legal_hold;
        self
    }
}

impl S3Api for PutObject {
    type S3Response = PutObjectResponse;
}

impl ToS3Request for PutObject {
    fn to_s3request(self) -> Result<S3Request, Error> {
        self.0.to_s3request()
    }
}

// endregion: put-object

// region: put-object-content

/// PutObjectContent takes a `ObjectContent` stream and uploads it to MinIO/S3.
///
/// It is a higher level API and handles multipart uploads transparently.
#[derive(Default)]
pub struct PutObjectContent {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,
    object: String,
    user_metadata: Option<Multimap>,
    sse: Option<Arc<dyn Sse>>,
    tags: Option<HashMap<String, String>>,
    retention: Option<Retention>,
    legal_hold: bool,
    part_size: Size,
    content_type: Option<String>,

    // source data
    input_content: ObjectContent,

    // Computed.
    // expected_parts: Option<u16>,
    content_stream: ContentStream,
    part_count: Option<u16>,
}

impl PutObjectContent {
    pub fn new(
        client: Client,
        bucket: String,
        object: String,
        content: impl Into<ObjectContent>,
    ) -> Self {
        Self {
            client,
            bucket,
            object,
            input_content: content.into(),
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

    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn user_metadata(mut self, user_metadata: Option<Multimap>) -> Self {
        self.user_metadata = user_metadata;
        self
    }

    pub fn sse(mut self, sse: Option<Arc<dyn Sse>>) -> Self {
        self.sse = sse;
        self
    }

    pub fn tags(mut self, tags: Option<HashMap<String, String>>) -> Self {
        self.tags = tags;
        self
    }

    pub fn retention(mut self, retention: Option<Retention>) -> Self {
        self.retention = retention;
        self
    }

    pub fn legal_hold(mut self, legal_hold: bool) -> Self {
        self.legal_hold = legal_hold;
        self
    }

    pub fn part_size(mut self, part_size: impl Into<Size>) -> Self {
        self.part_size = part_size.into();
        self
    }

    pub fn content_type(mut self, content_type: String) -> Self {
        self.content_type = Some(content_type);
        self
    }

    pub async fn send(mut self) -> Result<PutObjectContentResponse, Error> {
        check_bucket_name(&self.bucket, true)?;
        check_object_name(&self.object)?;

        let input_content = std::mem::take(&mut self.input_content);
        self.content_stream = input_content
            .to_content_stream()
            .await
            .map_err(Error::IOError)?;

        // object_size may be Size::Unknown.
        let object_size = self.content_stream.get_size();

        let (part_size, expected_parts) = calc_part_info(object_size, self.part_size)?;
        // Set the chosen part size and part count.
        self.part_size = Size::Known(part_size);
        self.part_count = expected_parts;

        if let Some(v) = &self.sse {
            if v.tls_required() && !self.client.is_secure() {
                return Err(Error::SseTlsRequired(None));
            }
        }

        // Read the first part.
        let seg_bytes = self.content_stream.read_upto(part_size as usize).await?;

        // In the first part read, if:
        //
        //   - object_size is unknown AND we got less than the part size, OR
        //   - we are expecting only one part to be uploaded,
        //
        // we upload it as a simple put object.
        if (object_size.is_unknown() && (seg_bytes.len() as u64) < part_size)
            || expected_parts == Some(1)
        {
            let size = seg_bytes.len() as u64;

            let res: PutObjectResponse = PutObject(UploadPart {
                client: self.client.clone(),
                extra_headers: self.extra_headers.clone(),
                extra_query_params: self.extra_query_params.clone(),
                bucket: self.bucket.clone(),
                object: self.object.clone(),
                region: self.region.clone(),
                user_metadata: self.user_metadata.clone(),
                sse: self.sse.clone(),
                tags: self.tags.clone(),
                retention: self.retention.clone(),
                legal_hold: self.legal_hold,
                part_number: None,
                upload_id: None,
                data: seg_bytes,
                content_type: self.content_type.clone(),
            })
            .send()
            .await?;

            Ok(PutObjectContentResponse {
                headers: res.headers,
                bucket: res.bucket,
                object: res.object,
                region: res.region,
                object_size: size,
                etag: res.etag,
                version_id: res.version_id,
            })
        } else if object_size.is_known() && (seg_bytes.len() as u64) < part_size {
            // Not enough data!
            let expected: u64 = object_size.as_u64().unwrap();
            let got: u64 = seg_bytes.len() as u64;
            Err(Error::InsufficientData(expected, got))
        } else {
            let bucket: String = self.bucket.clone();
            let object: String = self.object.clone();

            // Otherwise, we start a multipart upload.
            let create_mpu_resp: CreateMultipartUploadResponse = CreateMultipartUpload {
                client: self.client.clone(),
                extra_headers: self.extra_headers.clone(),
                extra_query_params: self.extra_query_params.clone(),
                region: self.region.clone(),
                bucket: self.bucket.clone(),
                object: self.object.clone(),
                user_metadata: self.user_metadata.clone(),
                sse: self.sse.clone(),
                tags: self.tags.clone(),
                retention: self.retention.clone(),
                legal_hold: self.legal_hold,
                content_type: self.content_type.clone(),
            }
            .send()
            .await?;

            let client = self.client.clone();
            let mpu_res = self
                .send_mpu(
                    part_size,
                    create_mpu_resp.upload_id.clone(),
                    object_size,
                    seg_bytes,
                )
                .await;

            if mpu_res.is_err() {
                // If we failed to complete the multipart upload, we should abort it.
                let _ =
                    AbortMultipartUpload::new(client, bucket, object, create_mpu_resp.upload_id)
                        .send()
                        .await;
            }
            mpu_res
        }
    }

    /// send multi-part-upload
    async fn send_mpu(
        mut self,
        part_size: u64,
        upload_id: String,
        object_size: Size,
        first_part: SegmentedBytes,
    ) -> Result<PutObjectContentResponse, Error> {
        let mut done = false;
        let mut part_number = 0;
        let mut parts: Vec<PartInfo> = if let Some(pc) = self.part_count {
            Vec::with_capacity(pc as usize)
        } else {
            Vec::new()
        };

        let mut first_part = Some(first_part);
        let mut total_read = 0;
        while !done {
            let part_content = {
                if let Some(v) = first_part.take() {
                    v
                } else {
                    self.content_stream.read_upto(part_size as usize).await?
                }
            };
            part_number += 1;
            let buffer_size = part_content.len() as u64;
            total_read += buffer_size;

            assert!(
                buffer_size <= part_size,
                "{:?} <= {:?}",
                buffer_size,
                part_size
            );

            if (buffer_size == 0) && (part_number > 1) {
                // We are done as we uploaded at least 1 part and we have reached the end of the stream.
                break;
            }

            // Check if we have too many parts to upload.
            if self.part_count.is_none() && (part_number > MAX_MULTIPART_COUNT) {
                return Err(Error::TooManyParts);
            }

            if object_size.is_known() {
                let exp = object_size.as_u64().unwrap();
                if exp < total_read {
                    return Err(Error::TooMuchData(exp));
                }
            }

            // Upload the part now.
            let resp: UploadPartResponse = UploadPart {
                client: self.client.clone(),
                extra_headers: self.extra_headers.clone(),
                extra_query_params: self.extra_query_params.clone(),
                bucket: self.bucket.clone(),
                object: self.object.clone(),
                region: self.region.clone(),
                // User metadata is not sent with UploadPart.
                user_metadata: None,
                sse: self.sse.clone(),
                tags: self.tags.clone(),
                retention: self.retention.clone(),
                legal_hold: self.legal_hold,
                part_number: Some(part_number),
                upload_id: Some(upload_id.to_string()),
                data: part_content,
                content_type: self.content_type.clone(),
            }
            .send()
            .await?;

            parts.push(PartInfo {
                number: part_number,
                etag: resp.etag,
                size: buffer_size,
            });

            // Finally check if we are done.
            if buffer_size < part_size {
                done = true;
            }
        }

        // Complete the multipart upload.
        let size = parts.iter().map(|p| p.size).sum();

        if object_size.is_known() {
            let expected = object_size.as_u64().unwrap();
            if expected != size {
                return Err(Error::InsufficientData(expected, size));
            }
        }

        let res: CompleteMultipartUploadResponse = CompleteMultipartUpload {
            client: self.client,
            extra_headers: self.extra_headers,
            extra_query_params: self.extra_query_params,
            bucket: self.bucket,
            object: self.object,
            region: self.region,
            parts,
            upload_id,
        }
        .send()
        .await?;

        Ok(PutObjectContentResponse {
            headers: res.headers,
            bucket: res.bucket,
            object: res.object,
            region: res.region,
            object_size: size,
            etag: res.etag,
            version_id: res.version_id,
        })
    }
}

// endregion: put-object-content

fn into_headers_put_object(
    extra_headers: Option<Multimap>,
    user_metadata: Option<Multimap>,
    sse: Option<Arc<dyn Sse>>,
    tags: Option<HashMap<String, String>>,
    retention: Option<Retention>,
    legal_hold: bool,
    content_type: Option<String>,
) -> Result<Multimap, Error> {
    let mut map = Multimap::new();

    if let Some(v) = extra_headers {
        map.add_multimap(v);
    }

    if let Some(v) = user_metadata {
        // Validate it.
        for (k, _) in v.iter() {
            if k.is_empty() {
                return Err(Error::InvalidUserMetadata(
                    "user metadata key cannot be empty".into(),
                ));
            }
            if !k.starts_with("x-amz-meta-") {
                return Err(Error::InvalidUserMetadata(format!(
                    "user metadata key '{}' does not start with 'x-amz-meta-'",
                    k
                )));
            }
        }
        map.add_multimap(v);
    }

    if let Some(v) = sse {
        map.add_multimap(v.headers());
    }

    if let Some(v) = tags {
        let mut tagging = String::new();
        for (key, value) in v.iter() {
            if !tagging.is_empty() {
                tagging.push('&');
            }
            tagging.push_str(&urlencode(key));
            tagging.push('=');
            tagging.push_str(&urlencode(value));
        }

        if !tagging.is_empty() {
            map.insert("x-amz-tagging".into(), tagging);
        }
    }

    if let Some(v) = retention {
        map.insert("x-amz-object-lock-mode".into(), v.mode.to_string());
        map.insert(
            "x-amz-object-lock-retain-until-date".into(),
            to_iso8601utc(v.retain_until_date),
        );
    }

    if legal_hold {
        map.insert("x-amz-object-lock-legal-hold".into(), "ON".into());
    }

    // Set the Content-Type header if not already set.
    if !map.contains_key("Content-Type") {
        map.insert(
            "Content-Type".into(),
            content_type.unwrap_or_else(|| "application/octet-stream".into()),
        );
    }

    Ok(map)
}

pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024; // 5 MiB
pub const MAX_PART_SIZE: u64 = 1024 * MIN_PART_SIZE; // 5 GiB
pub const MAX_OBJECT_SIZE: u64 = 1024 * MAX_PART_SIZE; // 5 TiB
pub const MAX_MULTIPART_COUNT: u16 = 10_000;

/// Returns the size of each part to upload and the total number of parts. The
/// number of parts is `None` when the object size is unknown.
pub fn calc_part_info(object_size: Size, part_size: Size) -> Result<(u64, Option<u16>), Error> {
    // Validate arguments against limits.
    if let Size::Known(v) = part_size {
        if v < MIN_PART_SIZE {
            return Err(Error::InvalidMinPartSize(v));
        }

        if v > MAX_PART_SIZE {
            return Err(Error::InvalidMaxPartSize(v));
        }
    }

    if let Size::Known(v) = object_size {
        if v > MAX_OBJECT_SIZE {
            return Err(Error::InvalidObjectSize(v));
        }
    }

    match (object_size, part_size) {
        // If object size is unknown, part size must be provided.
        (Size::Unknown, Size::Unknown) => Err(Error::MissingPartSize),

        // If object size is unknown, and part size is known, the number of
        // parts will be unknown, so return None for that.
        (Size::Unknown, Size::Known(part_size)) => Ok((part_size, None)),

        // If object size is known, and part size is unknown, calculate part
        // size.
        (Size::Known(object_size), Size::Unknown) => {
            // 1. Calculate the minimum part size (i.e. assuming part count is
            // maximum).
            let mut psize: u64 = (object_size as f64 / MAX_MULTIPART_COUNT as f64).ceil() as u64;

            // 2. Round up to the nearest multiple of MIN_PART_SIZE.
            psize = MIN_PART_SIZE * (psize as f64 / MIN_PART_SIZE as f64).ceil() as u64;

            if psize > object_size {
                psize = object_size;
            }

            let part_count = if psize > 0 {
                (object_size as f64 / psize as f64).ceil() as u16
            } else {
                1
            };

            Ok((psize, Some(part_count)))
        }

        // If both object size and part size are known, validate the resulting
        // part count and return.
        (Size::Known(object_size), Size::Known(part_size)) => {
            let part_count = (object_size as f64 / part_size as f64).ceil() as u16;
            if part_count == 0 || part_count > MAX_MULTIPART_COUNT {
                return Err(Error::InvalidPartCount(
                    object_size,
                    part_size,
                    MAX_MULTIPART_COUNT,
                ));
            }

            Ok((part_size, Some(part_count)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    quickcheck! {
        fn test_calc_part_info(object_size: Size, part_size: Size) -> bool {
            let res = calc_part_info(object_size, part_size);

            // Validate that basic invalid sizes return the expected error.
            if let Size::Known(v) = part_size {
                if v < MIN_PART_SIZE {
                    return match res {
                        Err(Error::InvalidMinPartSize(v_err)) => v == v_err,
                        _ => false,
                    }
                }
                if v > MAX_PART_SIZE {
                    return match res {
                        Err(Error::InvalidMaxPartSize(v_err)) => v == v_err,
                        _ => false,
                    }
                }
            }
            if let Size::Known(v) = object_size {
                if v > MAX_OBJECT_SIZE {
                    return match res {
                        Err(Error::InvalidObjectSize(v_err)) => v == v_err,
                        _ => false,
                    }
                }
            }

            // Validate the calculation of part size and part count.
            match (object_size, part_size, res) {
                (Size::Unknown, Size::Unknown, Err(Error::MissingPartSize)) => true,
                (Size::Unknown, Size::Unknown, _) => false,

                (Size::Unknown, Size::Known(part_size), Ok((psize, None))) => {
                    psize == part_size
                }
                (Size::Unknown, Size::Known(_), _) => false,

                (Size::Known(object_size), Size::Unknown, Ok((psize, Some(part_count)))) => {
                    if object_size < MIN_PART_SIZE  {
                        return psize == object_size && part_count == 1;
                    }
                    if !(MIN_PART_SIZE..=MAX_PART_SIZE).contains(&psize){
                        return false;
                    }
                    if psize > object_size {
                        return false;
                    }
                    (part_count > 0) && (part_count <= MAX_MULTIPART_COUNT)
                }
                (Size::Known(_), Size::Unknown, _) => false,

                (Size::Known(object_size), Size::Known(part_size), res) => {
                    if (part_size > object_size) || ((part_size * (MAX_MULTIPART_COUNT as u64)) < object_size) {
                        return match res {
                            Err(Error::InvalidPartCount(v1, v2, v3)) => {
                                (v1 == object_size) && (v2 == part_size) && (v3 == MAX_MULTIPART_COUNT)
                            }
                            _ => false,
                        }
                    }
                    match res {
                        Ok((psize, part_count)) => {
                            let expected_part_count = (object_size as f64 / part_size as f64).ceil() as u16;
                            (psize == part_size) && (part_count == Some(expected_part_count))
                        }
                        _ => false,
                    }
                }
            }
        }
    }
}
