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
use crate::s3::client::{MAX_MULTIPART_COUNT, MAX_PART_SIZE};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::header_constants::*;
use crate::s3::multimap::{Multimap, MultimapExt};
use crate::s3::response::a_response_traits::HasEtagFromBody;
use crate::s3::response::{
    AbortMultipartUploadResponse, CompleteMultipartUploadResponse, ComposeObjectResponse,
    CopyObjectInternalResponse, CopyObjectResponse, CreateMultipartUploadResponse,
    StatObjectResponse, UploadPartCopyResponse,
};
use crate::s3::sse::{Sse, SseCustomerKey};
use crate::s3::types::{Directive, PartInfo, Retention, S3Api, S3Request, ToS3Request};
use crate::s3::utils::{
    UtcTime, check_bucket_name, check_object_name, check_sse, check_ssec, to_http_header_value,
    to_iso8601utc, url_encode,
};
use async_recursion::async_recursion;
use http::Method;
use std::collections::HashMap;
use std::sync::Arc;

/// Argument builder for the [`UploadPartCopy`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::upload_part_copy`](crate::s3::client::Client::upload_part_copy) method.
#[derive(Clone, Debug, Default)]
pub struct UploadPartCopy {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    object: String,
    upload_id: String,
    part_number: u16,
    headers: Multimap,
}

impl UploadPartCopy {
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

    /// Sets the region for the request
    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn part_number(mut self, part_number: u16) -> Self {
        self.part_number = part_number;
        self
    }

    pub fn headers(mut self, headers: Multimap) -> Self {
        self.headers = headers;
        self
    }
}

impl S3Api for UploadPartCopy {
    type S3Response = UploadPartCopyResponse;
}

impl ToS3Request for UploadPartCopy {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        {
            check_bucket_name(&self.bucket, true)?;
            check_object_name(&self.object)?;
            if self.upload_id.is_empty() {
                return Err(ValidationErr::InvalidUploadId(
                    "upload ID cannot be empty".into(),
                ));
            }
            if !(1..=MAX_MULTIPART_COUNT).contains(&self.part_number) {
                return Err(ValidationErr::InvalidPartNumber(format!(
                    "part number must be between 1 and {MAX_MULTIPART_COUNT}"
                )));
            }
        }

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        headers.add_multimap(self.headers);

        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        {
            query_params.add("partNumber", self.part_number.to_string());
            query_params.add("uploadId", self.upload_id);
        }

        Ok(S3Request::new(self.client, Method::PUT)
            .region(self.region)
            .bucket(Some(self.bucket))
            .object(Some(self.object))
            .query_params(query_params)
            .headers(headers))
    }
}

#[derive(Clone, Debug, Default)]
pub struct CopyObjectInternal {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    object: String,
    headers: Multimap,
    user_metadata: Option<Multimap>,
    sse: Option<Arc<dyn Sse>>,
    tags: Option<HashMap<String, String>>,
    retention: Option<Retention>,
    legal_hold: bool,
    source: CopySource,

    metadata_directive: Option<Directive>,
    tagging_directive: Option<Directive>,
}

impl CopyObjectInternal {
    pub fn new(client: Client, bucket: String, object: String) -> Self {
        Self {
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

    /// Sets the region for the request
    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn headers(mut self, headers: Multimap) -> Self {
        self.headers = headers;
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

    pub fn source(mut self, source: CopySource) -> Self {
        self.source = source;
        self
    }

    pub fn metadata_directive(mut self, metadata_directive: Option<Directive>) -> Self {
        self.metadata_directive = metadata_directive;
        self
    }

    pub fn tagging_directive(mut self, tagging_directive: Option<Directive>) -> Self {
        self.tagging_directive = tagging_directive;
        self
    }
}

impl S3Api for CopyObjectInternal {
    type S3Response = CopyObjectInternalResponse;
}

impl ToS3Request for CopyObjectInternal {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_sse(&self.sse, &self.client)?;
        check_ssec(&self.source.ssec, &self.client)?;

        let mut headers = self.headers;
        {
            if let Some(v) = self.extra_headers {
                headers.add_multimap(v);
            }
            if let Some(v) = self.user_metadata {
                headers.add_multimap(v);
            }
            if let Some(v) = self.sse {
                headers.add_multimap(v.headers());
            }
            if let Some(v) = self.tags {
                let mut tagging = String::new();
                for (key, value) in v.iter() {
                    if !tagging.is_empty() {
                        tagging.push('&');
                    }
                    tagging.push_str(&url_encode(key));
                    tagging.push('=');
                    tagging.push_str(&url_encode(value));
                }
                if !tagging.is_empty() {
                    headers.add(X_AMZ_TAGGING, tagging);
                }
            }
            if let Some(v) = self.retention {
                headers.add(X_AMZ_OBJECT_LOCK_MODE, v.mode.to_string());
                headers.add(
                    X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE,
                    to_iso8601utc(v.retain_until_date),
                );
            }
            if self.legal_hold {
                headers.add(X_AMZ_OBJECT_LOCK_LEGAL_HOLD, "ON");
            }
            if let Some(v) = self.metadata_directive {
                headers.add(X_AMZ_METADATA_DIRECTIVE, v.to_string());
            }
            if let Some(v) = self.tagging_directive {
                headers.add(X_AMZ_TAGGING_DIRECTIVE, v.to_string());
            }

            let mut copy_source = String::from("/");
            copy_source.push_str(&self.source.bucket);
            copy_source.push('/');
            copy_source.push_str(&self.source.object);
            if let Some(v) = &self.source.version_id {
                copy_source.push_str("?versionId=");
                copy_source.push_str(&url_encode(v));
            }
            headers.add(X_AMZ_COPY_SOURCE, copy_source);

            let range = self.source.get_range_value();
            if !range.is_empty() {
                headers.add(X_AMZ_COPY_SOURCE_RANGE, range);
            }

            if let Some(v) = self.source.match_etag {
                headers.add(X_AMZ_COPY_SOURCE_IF_MATCH, v);
            }

            if let Some(v) = self.source.not_match_etag {
                headers.add(X_AMZ_COPY_SOURCE_IF_NONE_MATCH, v);
            }

            if let Some(v) = self.source.modified_since {
                headers.add(X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE, to_http_header_value(v));
            }

            if let Some(v) = self.source.unmodified_since {
                headers.add(
                    X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE,
                    to_http_header_value(v),
                );
            }

            if let Some(v) = self.source.ssec {
                headers.add_multimap(v.copy_headers());
            }
        };

        Ok(S3Request::new(self.client, Method::PUT)
            .region(self.region)
            .bucket(Some(self.bucket))
            .object(Some(self.object))
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(headers))
    }
}

/// Argument builder for [`CopyObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::copy_object`](crate::s3::client::Client::copy_object) method.
#[derive(Clone, Debug, Default)]
pub struct CopyObject {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    object: String,
    headers: Option<Multimap>,
    user_metadata: Option<Multimap>,
    sse: Option<Arc<dyn Sse>>,
    tags: Option<HashMap<String, String>>,
    retention: Option<Retention>,
    legal_hold: bool,
    source: CopySource,
    metadata_directive: Option<Directive>,
    tagging_directive: Option<Directive>,
}

impl CopyObject {
    pub fn new(client: Client, bucket: String, object: String) -> Self {
        Self {
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

    /// Sets the region for the request
    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn headers(mut self, headers: Option<Multimap>) -> Self {
        self.headers = headers;
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

    /// Sets the source for the copy operation.
    pub fn source(mut self, source: CopySource) -> Self {
        self.source = source;
        self
    }
    pub fn metadata_directive(mut self, metadata_directive: Option<Directive>) -> Self {
        self.metadata_directive = metadata_directive;
        self
    }
    pub fn tagging_directive(mut self, tagging_directive: Option<Directive>) -> Self {
        self.tagging_directive = tagging_directive;
        self
    }

    /// Sends the copy object request.
    ///
    /// Functionally related to the [S3Api::send()](crate::s3::types::S3Api::send) method, but
    /// specifically tailored for the `CopyObject` operation.
    pub async fn send(self) -> Result<CopyObjectResponse, Error> {
        check_sse(&self.sse, &self.client)?;
        check_ssec(&self.source.ssec, &self.client)?;

        let source = self.source.clone();

        let stat_resp: StatObjectResponse = self
            .client
            .stat_object(&source.bucket, &source.object)
            .extra_headers(source.extra_headers)
            .extra_query_params(source.extra_query_params)
            .region(source.region)
            .version_id(source.version_id)
            .match_etag(source.match_etag)
            .not_match_etag(source.not_match_etag)
            .modified_since(source.modified_since)
            .unmodified_since(source.unmodified_since)
            .send()
            .await?;

        if self.source.offset.is_some()
            || self.source.length.is_some()
            || stat_resp.size()? > MAX_PART_SIZE
        {
            if let Some(v) = &self.metadata_directive {
                match v {
                    Directive::Copy => {
                        return Err(ValidationErr::InvalidCopyDirective(
                            "COPY metadata directive is not applicable to source object size greater than 5 GiB".into()
                        ).into());
                    }
                    _ => todo!(), // Nothing to do.
                }
            }
            if let Some(v) = &self.tagging_directive {
                match v {
                    Directive::Copy => {
                        return Err(ValidationErr::InvalidCopyDirective(
                            "COPY tagging directive is not applicable to source object size greater than 5 GiB".into()
                        ).into());
                    }
                    _ => todo!(), // Nothing to do.
                }
            }

            let src: ComposeSource = {
                let mut src = ComposeSource::new(&self.source.bucket, &self.source.object)?;
                src.extra_headers = self.source.extra_headers;
                src.extra_query_params = self.source.extra_query_params;
                src.region = self.source.region;
                src.ssec = self.source.ssec;
                src.offset = self.source.offset;
                src.length = self.source.length;
                src.match_etag = self.source.match_etag;
                src.not_match_etag = self.source.not_match_etag;
                src.modified_since = self.source.modified_since;
                src.unmodified_since = self.source.unmodified_since;
                src
            };
            let sources: Vec<ComposeSource> = vec![src];

            let resp: ComposeObjectResponse = self
                .client
                .compose_object(&self.source.bucket, &self.source.object, sources)
                .extra_headers(self.extra_headers)
                .extra_query_params(self.extra_query_params)
                .region(self.region)
                .headers(self.headers)
                .user_metadata(self.user_metadata)
                .sse(self.sse)
                .tags(self.tags)
                .retention(self.retention)
                .legal_hold(self.legal_hold)
                .send()
                .await?;

            let resp: CopyObjectResponse = resp; // retype to CopyObjectResponse
            Ok(resp)
        } else {
            let resp: CopyObjectInternalResponse = self
                .client
                .copy_object_internal(&self.bucket, &self.object)
                .extra_headers(self.extra_headers)
                .extra_query_params(self.extra_query_params)
                .region(self.region)
                .headers(self.headers.unwrap_or_default())
                .user_metadata(self.user_metadata)
                .sse(self.sse)
                .tags(self.tags)
                .retention(self.retention)
                .legal_hold(self.legal_hold)
                .source(self.source)
                .metadata_directive(self.metadata_directive)
                .tagging_directive(self.tagging_directive)
                .send()
                .await?;

            let resp: CopyObjectResponse = resp; // retype to CopyObjectResponse
            Ok(resp)
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ComposeObjectInternal {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    object: String,
    headers: Option<Multimap>,
    user_metadata: Option<Multimap>,
    sse: Option<Arc<dyn Sse>>,
    tags: Option<HashMap<String, String>>,
    retention: Option<Retention>,
    legal_hold: bool,
    sources: Vec<ComposeSource>,
}

impl ComposeObjectInternal {
    pub fn new(client: Client, bucket: String, object: String) -> Self {
        Self {
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

    /// Sets the region for the request
    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn headers(mut self, headers: Option<Multimap>) -> Self {
        self.headers = headers;
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

    pub fn sources(mut self, sources: Vec<ComposeSource>) -> Self {
        self.sources = sources;
        self
    }

    #[async_recursion]
    pub async fn send(self) -> (Result<ComposeObjectResponse, Error>, String) {
        let mut upload_id = String::new();

        let mut sources = self.sources;
        let part_count: u16 = match self.client.calculate_part_count(&mut sources).await {
            Ok(v) => v,
            Err(e) => return (Err(e), upload_id),
        };
        let sources = sources; // Note: make sources readonly

        if (part_count == 1) && sources[0].offset.is_none() && sources[0].length.is_none() {
            // the provided data contains one part: no need to use multipart upload,
            // use copy_object instead

            let resp: CopyObjectResponse = match self
                .client
                .copy_object(&self.bucket, &self.object)
                .extra_headers(self.extra_headers)
                .extra_query_params(self.extra_query_params)
                .region(self.region)
                .headers(self.headers)
                .user_metadata(self.user_metadata)
                .sse(self.sse)
                .tags(self.tags)
                .retention(self.retention)
                .legal_hold(self.legal_hold)
                .send()
                .await
            {
                Ok(v) => v,
                Err(e) => return (Err(e), upload_id),
            };

            let resp: ComposeObjectResponse = resp; // retype to ComposeObjectResponse

            (Ok(resp), upload_id)
        } else {
            let headers: Multimap = into_headers_copy_object(
                self.extra_headers,
                self.headers,
                self.user_metadata,
                self.sse.clone(),
                self.tags,
                self.retention,
                self.legal_hold,
            );
            let cmu: CreateMultipartUploadResponse = match self
                .client
                .create_multipart_upload(&self.bucket, &self.object)
                .extra_query_params(self.extra_query_params.clone())
                .region(self.region.clone())
                .extra_headers(Some(headers))
                .send()
                .await
            {
                Ok(v) => v,
                Err(e) => return (Err(e), upload_id),
            };

            // the multipart upload was successful: update the upload_id
            let upload_id_cmu: String = match cmu.upload_id().await {
                Ok(v) => v,
                Err(e) => return (Err(e.into()), upload_id),
            };
            upload_id.push_str(&upload_id_cmu);

            let mut part_number = 0_u16;
            let ssec_headers: Multimap = match self.sse {
                Some(v) => match v.as_any().downcast_ref::<SseCustomerKey>() {
                    Some(_) => v.headers(),
                    _ => Multimap::new(),
                },
                _ => Multimap::new(),
            };

            let mut parts: Vec<PartInfo> = Vec::new();
            for source in sources.iter() {
                let mut size = source.get_object_size();
                if let Some(l) = source.length {
                    size = l;
                } else if let Some(o) = source.offset {
                    size -= o;
                }

                let mut offset = source.offset.unwrap_or_default();

                let mut headers = source.get_headers();
                headers.add_multimap(ssec_headers.clone());

                if size <= MAX_PART_SIZE {
                    part_number += 1;
                    if let Some(l) = source.length {
                        headers.add(
                            X_AMZ_COPY_SOURCE_RANGE,
                            format!("bytes={}-{}", offset, offset + l - 1),
                        );
                    } else if source.offset.is_some() {
                        headers.add(
                            X_AMZ_COPY_SOURCE_RANGE,
                            format!("bytes={}-{}", offset, offset + size - 1),
                        );
                    }

                    let resp: UploadPartCopyResponse = match self
                        .client
                        .upload_part_copy(&self.bucket, &self.object, &upload_id)
                        .region(self.region.clone())
                        .part_number(part_number)
                        .headers(headers)
                        .send()
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return (Err(e), upload_id),
                    };

                    let etag = match resp.etag() {
                        Ok(v) => v,
                        Err(e) => return (Err(e.into()), upload_id),
                    };

                    parts.push(PartInfo {
                        number: part_number,
                        etag,
                        size,
                    });
                } else {
                    while size > 0 {
                        part_number += 1;

                        let mut length = size;
                        if length > MAX_PART_SIZE {
                            length = MAX_PART_SIZE;
                        }
                        let end_bytes = offset + length - 1;

                        let mut headers_copy = headers.clone();
                        headers_copy.add(
                            X_AMZ_COPY_SOURCE_RANGE,
                            format!("bytes={offset}-{end_bytes}"),
                        );

                        let resp: UploadPartCopyResponse = match self
                            .client
                            .upload_part_copy(&self.bucket, &self.object, &upload_id)
                            .region(self.region.clone())
                            .part_number(part_number)
                            .headers(headers_copy)
                            .send()
                            .await
                        {
                            Ok(v) => v,
                            Err(e) => return (Err(e), upload_id),
                        };

                        let etag = match resp.etag() {
                            Ok(v) => v,
                            Err(e) => return (Err(e.into()), upload_id),
                        };

                        parts.push(PartInfo {
                            number: part_number,
                            etag,
                            size,
                        });

                        offset += length;
                        size -= length;
                    }
                }
            }

            let resp: Result<CompleteMultipartUploadResponse, Error> = self
                .client
                .complete_multipart_upload(&self.bucket, &self.object, &upload_id, parts)
                .region(self.region)
                .send()
                .await;

            match resp {
                Ok(v) => {
                    let resp = ComposeObjectResponse {
                        request: v.request,
                        headers: v.headers,
                        body: v.body,
                    };
                    (Ok(resp), upload_id)
                }
                Err(e) => (Err(e), upload_id),
            }
        }
    }
}

/// Argument builder for [`CopyObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html) S3 API operation.
///
/// See [Amazon S3 Multipart Upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
/// This struct constructs the parameters required for the [`Client::copy_object`](crate::s3::client::Client::copy_object) method.
#[derive(Clone, Debug, Default)]
pub struct ComposeObject {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    object: String,
    headers: Option<Multimap>,
    user_metadata: Option<Multimap>,
    sse: Option<Arc<dyn Sse>>,
    tags: Option<HashMap<String, String>>,
    retention: Option<Retention>,
    legal_hold: bool,
    sources: Vec<ComposeSource>,
}

impl ComposeObject {
    pub fn new(client: Client, bucket: String, object: String) -> Self {
        Self {
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

    /// Sets the region for the request
    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn headers(mut self, headers: Option<Multimap>) -> Self {
        self.headers = headers;
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

    pub fn sources(mut self, sources: Vec<ComposeSource>) -> Self {
        self.sources = sources;
        self
    }

    pub async fn send(self) -> Result<ComposeObjectResponse, Error> {
        check_sse(&self.sse, &self.client)?;

        let object: String = self.object.clone();
        let bucket: String = self.bucket.clone();

        let (res, upload_id): (Result<ComposeObjectResponse, Error>, String) = self
            .client
            .compose_object_internal(&self.bucket, &self.object)
            .extra_headers(self.extra_headers)
            .extra_query_params(self.extra_query_params)
            .region(self.region)
            .headers(self.headers)
            .user_metadata(self.user_metadata)
            .sse(self.sse)
            .tags(self.tags)
            .retention(self.retention)
            .legal_hold(self.legal_hold)
            .sources(self.sources)
            .send()
            .await;

        match res {
            Ok(v) => Ok(v),
            Err(e) => {
                if !upload_id.is_empty() {
                    let _resp: AbortMultipartUploadResponse = self
                        .client
                        .abort_multipart_upload(&bucket, &object, &upload_id)
                        .send()
                        .await?;
                }
                Err(e)
            }
        }
    }
}

// region: misc

#[derive(Clone, Debug, Default)]
/// Source object information for [compose_object](Client::compose_object)
pub struct ComposeSource {
    pub extra_headers: Option<Multimap>,
    pub extra_query_params: Option<Multimap>,
    pub region: Option<String>,
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
    pub ssec: Option<SseCustomerKey>,
    pub offset: Option<u64>,
    pub length: Option<u64>,
    pub match_etag: Option<String>,
    pub not_match_etag: Option<String>,
    pub modified_since: Option<UtcTime>,
    pub unmodified_since: Option<UtcTime>,

    object_size: Option<u64>,  // populated by build_headers()
    headers: Option<Multimap>, // populated by build_headers()
}

impl ComposeSource {
    /// Returns a compose source with given bucket name and object name
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::builders::ComposeSource;
    /// let src = ComposeSource::new("my-src-bucket", "my-src-object").unwrap();
    /// ```
    pub fn new(bucket_name: &str, object_name: &str) -> Result<Self, ValidationErr> {
        check_bucket_name(bucket_name, true)?;
        check_object_name(object_name)?;

        Ok(Self {
            bucket: bucket_name.to_owned(),
            object: object_name.to_owned(),
            ..Default::default()
        })
    }

    pub fn get_object_size(&self) -> u64 {
        self.object_size.expect("A: ABORT: ComposeSource::build_headers() must be called prior to this method invocation. This should not happen.")
    }

    pub fn get_headers(&self) -> Multimap {
        self.headers.as_ref().expect("B: ABORT: ComposeSource::build_headers() must be called prior to this method invocation. This should not happen.").clone()
    }

    pub fn build_headers(&mut self, object_size: u64, etag: String) -> Result<(), ValidationErr> {
        if let Some(v) = self.offset
            && v >= object_size
        {
            return Err(ValidationErr::InvalidComposeSourceOffset {
                bucket: self.bucket.to_string(),
                object: self.object.to_string(),
                version: self.version_id.clone(),
                offset: v,
                object_size,
            });
        }

        if let Some(v) = self.length {
            if v > object_size {
                return Err(ValidationErr::InvalidComposeSourceLength {
                    bucket: self.bucket.to_string(),
                    object: self.object.to_string(),
                    version: self.version_id.clone(),
                    length: v,
                    object_size,
                });
            }

            if (self.offset.unwrap_or_default() + v) > object_size {
                return Err(ValidationErr::InvalidComposeSourceSize {
                    bucket: self.bucket.to_string(),
                    object: self.object.to_string(),
                    version: self.version_id.clone(),
                    compose_size: self.offset.unwrap_or_default() + v,
                    object_size,
                });
            }
        }

        self.object_size = Some(object_size);

        let mut headers = Multimap::new();

        let mut copy_source = String::from("/");
        copy_source.push_str(&self.bucket);
        copy_source.push('/');
        copy_source.push_str(&self.object);
        if let Some(v) = &self.version_id {
            copy_source.push_str("?versionId=");
            copy_source.push_str(&url_encode(v));
        }
        headers.add(X_AMZ_COPY_SOURCE, copy_source);

        if let Some(v) = &self.match_etag {
            headers.add(X_AMZ_COPY_SOURCE_IF_MATCH, v);
        }

        if let Some(v) = &self.not_match_etag {
            headers.add(X_AMZ_COPY_SOURCE_IF_NONE_MATCH, v);
        }

        if let Some(v) = self.modified_since {
            headers.add(X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE, to_http_header_value(v));
        }

        if let Some(v) = self.unmodified_since {
            headers.add(
                X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE,
                to_http_header_value(v),
            );
        }

        if let Some(v) = &self.ssec {
            headers.add_multimap(v.copy_headers());
        }

        if !headers.contains_key(X_AMZ_COPY_SOURCE_IF_MATCH) {
            headers.add(X_AMZ_COPY_SOURCE_IF_MATCH, etag);
        }

        self.headers = Some(headers);

        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
/// Base argument for object conditional read APIs
pub struct CopySource {
    pub extra_headers: Option<Multimap>,
    pub extra_query_params: Option<Multimap>,
    pub region: Option<String>,
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
    pub ssec: Option<SseCustomerKey>,
    pub offset: Option<u64>,
    pub length: Option<u64>,
    pub match_etag: Option<String>,
    pub not_match_etag: Option<String>,
    pub modified_since: Option<UtcTime>,
    pub unmodified_since: Option<UtcTime>,
}

impl CopySource {
    pub fn new(bucket_name: &str, object_name: &str) -> Result<Self, ValidationErr> {
        check_bucket_name(bucket_name, true)?;
        check_object_name(object_name)?;

        Ok(Self {
            bucket: bucket_name.to_owned(),
            object: object_name.to_owned(),
            ..Default::default()
        })
    }

    fn get_range_value(&self) -> String {
        let (offset, length) = match self.length {
            Some(_) => (Some(self.offset.unwrap_or(0_u64)), self.length),
            None => (self.offset, None),
        };

        let mut range = String::new();
        if let Some(o) = offset {
            range.push_str("bytes=");
            range.push_str(&o.to_string());
            range.push('-');
            if let Some(l) = length {
                range.push_str(&(o + l - 1).to_string());
            }
        }

        range
    }
}

fn into_headers_copy_object(
    extra_headers: Option<Multimap>,
    headers: Option<Multimap>,
    user_metadata: Option<Multimap>,
    sse: Option<Arc<dyn Sse>>,
    tags: Option<HashMap<String, String>>,
    retention: Option<Retention>,
    legal_hold: bool,
) -> Multimap {
    let mut map = Multimap::new();

    if let Some(v) = extra_headers {
        map.add_multimap(v);
    }

    if let Some(v) = headers {
        map.add_multimap(v);
    }

    if let Some(v) = user_metadata {
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
            tagging.push_str(&url_encode(key));
            tagging.push('=');
            tagging.push_str(&url_encode(value));
        }

        if !tagging.is_empty() {
            map.add(X_AMZ_TAGGING, tagging);
        }
    }

    if let Some(v) = retention {
        map.add(X_AMZ_OBJECT_LOCK_MODE, v.mode.to_string());
        map.add(
            X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE,
            to_iso8601utc(v.retain_until_date),
        );
    }

    if legal_hold {
        map.add(X_AMZ_OBJECT_LOCK_LEGAL_HOLD, "ON");
    }

    map
}
// endregion: misc
