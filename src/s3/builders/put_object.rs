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

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::BytesMut;
use http::Method;

use crate::s3::{
    builders::ObjectContent,
    client::Client,
    error::Error,
    response::{
        AbortMultipartUploadResponse2, CompleteMultipartUploadResponse2,
        CreateMultipartUploadResponse2, PutObjectResponse2, UploadPartResponse2,
    },
    sse::Sse,
    types::{Part, Retention, S3Api, S3Request, ToS3Request},
    utils::{check_bucket_name, md5sum_hash, merge, to_iso8601utc, urlencode, Multimap},
};

use super::SegmentedBytes;

/// Argument for
/// [create_multipart_upload()](crate::s3::client::Client::create_multipart_upload)
/// API
#[derive(Clone, Debug, Default)]
pub struct CreateMultipartUpload {
    client: Option<Client>,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,
    object: String,
}

impl CreateMultipartUpload {
    pub fn new(bucket: &str, object: &str) -> Self {
        CreateMultipartUpload {
            bucket: bucket.to_string(),
            object: object.to_string(),
            ..Default::default()
        }
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
        self
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

impl ToS3Request for CreateMultipartUpload {
    fn to_s3request(&self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let mut headers = Multimap::new();
        if let Some(v) = &self.extra_headers {
            merge(&mut headers, v);
        }
        if !headers.contains_key("Content-Type") {
            headers.insert(
                String::from("Content-Type"),
                String::from("application/octet-stream"),
            );
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &self.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("uploads"), String::new());

        let req = S3Request::new(
            self.client.as_ref().ok_or(Error::NoClientProvided)?,
            Method::POST,
        )
        .region(self.region.as_deref())
        .bucket(Some(&self.bucket))
        .object(Some(&self.object))
        .query_params(query_params)
        .headers(headers);

        Ok(req)
    }
}

impl S3Api for CreateMultipartUpload {
    type S3Response = CreateMultipartUploadResponse2;
}

/// Argument for
/// [abort_multipart_upload()](crate::s3::client::Client::abort_multipart_upload)
/// API
#[derive(Clone, Debug, Default)]
pub struct AbortMultipartUpload {
    client: Option<Client>,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,
    object: String,
    upload_id: String,
}

impl AbortMultipartUpload {
    pub fn new(bucket: &str, object: &str, upload_id: &str) -> Self {
        AbortMultipartUpload {
            bucket: bucket.to_string(),
            object: object.to_string(),
            upload_id: upload_id.to_string(),
            ..Default::default()
        }
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
        self
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

impl ToS3Request for AbortMultipartUpload {
    fn to_s3request(&self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let mut headers = Multimap::new();
        if let Some(v) = &self.extra_headers {
            merge(&mut headers, v);
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &self.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(
            String::from("uploadId"),
            urlencode(&self.upload_id).to_string(),
        );

        let req = S3Request::new(
            self.client.as_ref().ok_or(Error::NoClientProvided)?,
            Method::DELETE,
        )
        .region(self.region.as_deref())
        .bucket(Some(&self.bucket))
        .object(Some(&self.object))
        .query_params(query_params)
        .headers(headers);

        Ok(req)
    }
}

impl S3Api for AbortMultipartUpload {
    type S3Response = AbortMultipartUploadResponse2;
}

/// Argument for
/// [complete_multipart_upload()](crate::s3::client::Client::complete_multipart_upload)
/// API
#[derive(Clone, Debug, Default)]
pub struct CompleteMultipartUpload {
    client: Option<Client>,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,
    object: String,
    upload_id: String,
    parts: Vec<Part>,
}

impl CompleteMultipartUpload {
    pub fn new(bucket: &str, object: &str, upload_id: &str, parts: Vec<Part>) -> Self {
        CompleteMultipartUpload {
            bucket: bucket.to_string(),
            object: object.to_string(),
            upload_id: upload_id.to_string(),
            parts,
            ..Default::default()
        }
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
        self
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
    fn to_s3request(&self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        if self.object.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        if self.upload_id.is_empty() {
            return Err(Error::InvalidUploadId(String::from(
                "upload ID cannot be empty",
            )));
        }

        if self.parts.is_empty() {
            return Err(Error::EmptyParts(String::from("parts cannot be empty")));
        }

        // Set capacity of the byte-buffer based on the part count - attempting
        // to avoid extra allocations when building the XML payload.
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
        let data = data.freeze();

        let mut headers = Multimap::new();
        if let Some(v) = &self.extra_headers {
            merge(&mut headers, v);
        }
        headers.insert(
            String::from("Content-Type"),
            String::from("application/xml"),
        );
        headers.insert(String::from("Content-MD5"), md5sum_hash(data.as_ref()));

        let mut query_params = Multimap::new();
        if let Some(v) = &self.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("uploadId"), self.upload_id.to_string());

        let req = S3Request::new(
            self.client.as_ref().ok_or(Error::NoClientProvided)?,
            Method::POST,
        )
        .region(self.region.as_deref())
        .bucket(Some(&self.bucket))
        .object(Some(&self.object))
        .query_params(query_params)
        .headers(headers)
        .body(Some(data.into()));

        Ok(req)
    }
}

impl S3Api for CompleteMultipartUpload {
    type S3Response = CompleteMultipartUploadResponse2;
}

/// Argument for [upload_part()](crate::s3::client::Client::upload_part) S3 API
#[derive(Debug, Clone, Default)]
pub struct UploadPart {
    client: Option<Client>,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    bucket: String,
    object: String,
    region: Option<String>,
    user_metadata: Option<Multimap>,
    sse: Option<Arc<dyn Sse>>,
    tags: Option<HashMap<String, String>>,
    retention: Option<Retention>,
    legal_hold: bool,
    data: SegmentedBytes,

    // These are optional as the struct is reused for PutObject.
    upload_id: Option<String>,
    part_number: Option<u16>,
}

impl UploadPart {
    pub fn new(
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number: u16,
        data: SegmentedBytes,
    ) -> Self {
        UploadPart {
            bucket: bucket.to_string(),
            object: object.to_string(),
            upload_id: Some(upload_id.to_string()),
            part_number: Some(part_number),
            data,
            ..Default::default()
        }
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
        self
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

    fn get_headers(&self) -> Multimap {
        object_write_args_headers(
            self.extra_headers.as_ref(),
            None,
            self.user_metadata.as_ref(),
            &self.sse,
            self.tags.as_ref(),
            self.retention.as_ref(),
            self.legal_hold,
        )
    }

    fn validate(&self) -> Result<(), Error> {
        check_bucket_name(&self.bucket, true)?;

        if self.object.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        if let Some(upload_id) = &self.upload_id {
            if upload_id.is_empty() {
                return Err(Error::InvalidUploadId(String::from(
                    "upload ID cannot be empty",
                )));
            }
        }

        if let Some(part_number) = self.part_number {
            if !(1..=10000).contains(&part_number) {
                return Err(Error::InvalidPartNumber(String::from(
                    "part number must be between 1 and 1000",
                )));
            }
        }

        Ok(())
    }
}

impl ToS3Request for UploadPart {
    fn to_s3request(&self) -> Result<S3Request, Error> {
        self.validate()?;

        let headers = self.get_headers();

        let mut query_params = Multimap::new();
        if let Some(v) = &self.extra_query_params {
            merge(&mut query_params, v);
        }
        if let Some(upload_id) = &self.upload_id {
            query_params.insert(String::from("uploadId"), upload_id.to_string());
        }
        if let Some(part_number) = self.part_number {
            query_params.insert(String::from("partNumber"), part_number.to_string());
        }

        let req = S3Request::new(
            self.client.as_ref().ok_or(Error::NoClientProvided)?,
            Method::PUT,
        )
        .region(self.region.as_deref())
        .bucket(Some(&self.bucket))
        .object(Some(&self.object))
        .query_params(query_params)
        .headers(headers)
        .body(Some(self.data.clone()));

        Ok(req)
    }
}

impl S3Api for UploadPart {
    type S3Response = UploadPartResponse2;
}

/// Argument builder for PutObject S3 API. This is a lower-level API.
#[derive(Debug, Clone, Default)]
pub struct PutObject(UploadPart);

impl PutObject {
    pub fn new(bucket: &str, object: &str, data: SegmentedBytes) -> Self {
        PutObject(UploadPart {
            bucket: bucket.to_string(),
            object: object.to_string(),
            data,
            ..Default::default()
        })
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.0.client = Some(client.clone());
        self
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

impl ToS3Request for PutObject {
    fn to_s3request(&self) -> Result<S3Request, Error> {
        self.0.to_s3request()
    }
}

impl S3Api for PutObject {
    type S3Response = PutObjectResponse2;
}

fn object_write_args_headers(
    extra_headers: Option<&Multimap>,
    headers: Option<&Multimap>,
    user_metadata: Option<&Multimap>,
    sse: &Option<Arc<dyn Sse>>,
    tags: Option<&HashMap<String, String>>,
    retention: Option<&Retention>,
    legal_hold: bool,
) -> Multimap {
    let mut map = Multimap::new();

    if let Some(v) = extra_headers {
        merge(&mut map, v);
    }

    if let Some(v) = headers {
        merge(&mut map, v);
    }

    if let Some(v) = user_metadata {
        merge(&mut map, v);
    }

    if let Some(v) = sse {
        merge(&mut map, &v.headers());
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
            map.insert(String::from("x-amz-tagging"), tagging);
        }
    }

    if let Some(v) = retention {
        map.insert(String::from("x-amz-object-lock-mode"), v.mode.to_string());
        map.insert(
            String::from("x-amz-object-lock-retain-until-date"),
            to_iso8601utc(v.retain_until_date),
        );
    }

    if legal_hold {
        map.insert(
            String::from("x-amz-object-lock-legal-hold"),
            String::from("ON"),
        );
    }

    map
}

// PutObjectContent takes a `ObjectContent` stream and uploads it to MinIO/S3.
//
// It is a higher level API and handles multipart uploads transparently.
pub struct PutObjectContent {
    client: Option<Client>,

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
    part_size: Option<u64>,
    content_type: String,

    // source data
    input_reader: Option<ObjectContent>,
    file_path: Option<PathBuf>,

    // Computed.
    // expected_parts: Option<u16>,
    reader: ObjectContent,
    part_count: u16,
}

impl PutObjectContent {
    pub fn new(bucket: &str, object: &str, content: impl Into<ObjectContent>) -> Self {
        PutObjectContent {
            bucket: bucket.to_string(),
            object: object.to_string(),
            input_reader: Some(content.into()),
            file_path: None,
            client: None,
            extra_headers: None,
            extra_query_params: None,
            region: None,
            user_metadata: None,
            sse: None,
            tags: None,
            retention: None,
            legal_hold: false,
            part_size: None,
            content_type: String::from("application/octet-stream"),
            reader: ObjectContent::empty(),
            part_count: 0,
        }
    }

    pub fn from_file(bucket: &str, object: &str, file_path: &Path) -> Self {
        PutObjectContent {
            bucket: bucket.to_string(),
            object: object.to_string(),
            input_reader: None,
            file_path: Some(file_path.to_path_buf()),
            client: None,
            extra_headers: None,
            extra_query_params: None,
            region: None,
            user_metadata: None,
            sse: None,
            tags: None,
            retention: None,
            legal_hold: false,
            part_size: None,
            content_type: String::from("application/octet-stream"),
            reader: ObjectContent::empty(),
            part_count: 0,
        }
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
        self
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

    pub fn part_size(mut self, part_size: Option<u64>) -> Self {
        self.part_size = part_size;
        self
    }

    pub fn content_type(mut self, content_type: String) -> Self {
        self.content_type = content_type;
        self
    }

    pub async fn send(mut self) -> Result<PutObjectResponse2, Error> {
        check_bucket_name(&self.bucket, true)?;

        if self.object.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        if self.input_reader.is_none() {
            // This unwrap is safe as the public API ensures that the file_path
            // or the reader is always set.
            let file_path = self.file_path.as_ref().unwrap();
            let file = tokio::fs::File::open(file_path).await?;
            let size = file.metadata().await?.len();
            self.reader = ObjectContent::from_reader(file, Some(size));
        } else {
            self.reader = self.input_reader.take().unwrap();
        }

        let object_size = self.reader.get_size();
        let (psize, expected_parts) = calc_part_info(object_size, self.part_size)?;
        assert_ne!(expected_parts, Some(0));
        self.part_size = Some(psize);

        let client = self.client.clone().ok_or(Error::NoClientProvided)?;

        if let Some(v) = &self.sse {
            if v.tls_required() && !client.is_secure() {
                return Err(Error::SseTlsRequired(None));
            }
        }

        // Read the first part.
        let seg_bytes = self.reader.read_upto(psize as usize).await?;

        // In the first part read, if:
        //
        //   - we got less than the expected part size, OR
        //   - we are expecting only one part to be uploaded,
        //
        // we upload it as a simple put object.
        if (seg_bytes.len() as u64) < psize || expected_parts == Some(1) {
            let po = self.to_put_object(seg_bytes);
            return po.send().await;
        }

        // Otherwise, we start a multipart upload.
        let create_mpu = CreateMultipartUpload::new(&self.bucket, &self.object)
            .client(&client)
            .extra_headers(self.extra_headers.clone())
            .extra_query_params(self.extra_query_params.clone())
            .region(self.region.clone());

        let create_mpu_resp = create_mpu.send().await?;

        let res = self
            .send_mpu(
                psize,
                expected_parts,
                create_mpu_resp.upload_id.clone(),
                object_size,
                seg_bytes,
            )
            .await;
        if res.is_err() {
            // If we failed to complete the multipart upload, we should abort it.
            let _ =
                AbortMultipartUpload::new(&self.bucket, &self.object, &create_mpu_resp.upload_id)
                    .client(&client)
                    .send()
                    .await;
        }
        res
    }

    async fn send_mpu(
        &mut self,
        psize: u64,
        expected_parts: Option<u16>,
        upload_id: String,
        object_size: Option<u64>,
        seg_bytes: SegmentedBytes,
    ) -> Result<PutObjectResponse2, Error> {
        let mut done = false;
        let mut part_number = 0;
        let mut parts: Vec<Part> = if let Some(pc) = expected_parts {
            Vec::with_capacity(pc as usize)
        } else {
            Vec::new()
        };

        let mut first_part = Some(seg_bytes);
        while !done {
            let part_content = {
                if let Some(v) = first_part.take() {
                    v
                } else {
                    self.reader.read_upto(psize as usize).await?
                }
            };
            part_number += 1;
            let buffer_size = part_content.len() as u64;

            assert!(buffer_size <= psize, "{:?} <= {:?}", buffer_size, psize);

            if buffer_size == 0 && part_number > 1 {
                // We are done as we uploaded at least 1 part and we have
                // reached the end of the stream.
                break;
            }

            // Check if we have too many parts to upload.
            if expected_parts.is_none() && part_number > MAX_MULTIPART_COUNT {
                return Err(Error::InvalidPartCount(
                    object_size.unwrap_or(0),
                    self.part_size.unwrap(),
                    self.part_count,
                ));
            }

            // Upload the part now.
            let upload_part = self.to_upload_part(part_content, &upload_id, part_number);
            let upload_part_resp = upload_part.send().await?;
            parts.push(Part {
                number: part_number,
                etag: upload_part_resp.etag,
            });

            // Finally check if we are done.
            if buffer_size < psize {
                done = true;
            }
        }

        // Complete the multipart upload.
        let complete_mpu = self.to_complete_multipart_upload(&upload_id, parts);
        complete_mpu.send().await
    }
}

impl PutObjectContent {
    fn to_put_object(&self, data: SegmentedBytes) -> PutObject {
        PutObject(UploadPart {
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
            data,
        })
    }

    fn to_upload_part(
        &self,
        data: SegmentedBytes,
        upload_id: &str,
        part_number: u16,
    ) -> UploadPart {
        UploadPart {
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
            part_number: Some(part_number),
            upload_id: Some(upload_id.to_string()),
            data,
        }
    }

    fn to_complete_multipart_upload(
        &self,
        upload_id: &str,
        parts: Vec<Part>,
    ) -> CompleteMultipartUpload {
        CompleteMultipartUpload {
            client: self.client.clone(),
            extra_headers: self.extra_headers.clone(),
            extra_query_params: self.extra_query_params.clone(),
            bucket: self.bucket.clone(),
            object: self.object.clone(),
            region: self.region.clone(),
            parts,
            upload_id: upload_id.to_string(),
        }
    }
}

pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024; // 5 MiB
pub const MAX_PART_SIZE: u64 = 1024 * MIN_PART_SIZE; // 5 GiB
pub const MAX_OBJECT_SIZE: u64 = 1024 * MAX_PART_SIZE; // 5 TiB
pub const MAX_MULTIPART_COUNT: u16 = 10_000;

// Returns the size of each part to upload and the total number of parts. The
// number of parts is `None` when the object size is unknown.
fn calc_part_info(
    object_size: Option<u64>,
    part_size: Option<u64>,
) -> Result<(u64, Option<u16>), Error> {
    // Validate arguments against limits.
    if let Some(v) = part_size {
        if v < MIN_PART_SIZE {
            return Err(Error::InvalidMinPartSize(v));
        }

        if v > MAX_PART_SIZE {
            return Err(Error::InvalidMaxPartSize(v));
        }
    }

    if let Some(v) = object_size {
        if v > MAX_OBJECT_SIZE {
            return Err(Error::InvalidObjectSize(v));
        }
    }

    match (object_size, part_size) {
        (None, None) => Err(Error::MissingPartSize),
        (None, Some(part_size)) => Ok((part_size, None)),
        (Some(object_size), None) => {
            let mut psize: u64 = (object_size as f64 / MAX_MULTIPART_COUNT as f64).ceil() as u64;

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
        (Some(object_size), Some(part_size)) => {
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
