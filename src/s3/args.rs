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
use crate::s3::signer::post_presign_v4;
use crate::s3::sse::{Sse, SseCustomerKey};
use crate::s3::types::{
    DeleteObject, Directive, Item, LifecycleConfig, NotificationConfig, NotificationRecords,
    ObjectLockConfig, Part, Quota, ReplicationConfig, Retention, RetentionMode, SelectRequest,
    SseConfig,
};
use crate::s3::utils::{
    b64encode, check_bucket_name, merge, to_amz_date, to_http_header_value, to_iso8601utc,
    to_signer_date, urlencode, utc_now, Multimap, UtcTime,
};
use derivative::Derivative;
use hyper::http::Method;
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;

pub const MIN_PART_SIZE: usize = 5_242_880; // 5 MiB
pub const MAX_PART_SIZE: usize = 5_368_709_120; // 5 GiB
pub const MAX_OBJECT_SIZE: usize = 5_497_558_138_880; // 5 TiB
pub const MAX_MULTIPART_COUNT: u16 = 10_000;
pub const DEFAULT_EXPIRY_SECONDS: u32 = 604_800; // 7 days

fn object_write_args_headers(
    extra_headers: Option<&Multimap>,
    headers: Option<&Multimap>,
    user_metadata: Option<&Multimap>,
    sse: Option<&dyn Sse>,
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

fn calc_part_info(
    object_size: Option<usize>,
    part_size: Option<usize>,
) -> Result<(usize, i16), Error> {
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
    } else {
        if part_size.is_none() {
            return Err(Error::MissingPartSize);
        }

        return Ok((part_size.unwrap(), -1));
    }

    let mut psize = 0_usize;
    if part_size.is_none() {
        psize = (object_size.unwrap() as f64 / MAX_MULTIPART_COUNT as f64).ceil() as usize;
        psize = MIN_PART_SIZE * (psize as f64 / MIN_PART_SIZE as f64).ceil() as usize;
    }

    if psize > object_size.unwrap() {
        psize = object_size.unwrap();
    }

    let mut part_count = 1_i16;
    if psize > 0 {
        part_count = (object_size.unwrap() as f64 / psize as f64).ceil() as i16;
    }

    if part_count as u16 > MAX_MULTIPART_COUNT {
        return Err(Error::InvalidPartCount(
            object_size.unwrap(),
            psize,
            MAX_MULTIPART_COUNT,
        ));
    }

    Ok((psize, part_count))
}

#[derive(Clone, Debug, Default)]
pub struct BucketArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
}

impl<'a> BucketArgs<'a> {
    pub fn new(bucket_name: &'a str) -> Result<BucketArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(BucketArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
        })
    }
}

pub type BucketExistsArgs<'a> = BucketArgs<'a>;

pub type RemoveBucketArgs<'a> = BucketArgs<'a>;

#[derive(Clone, Debug, Default)]
pub struct ObjectArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
}

impl<'a> ObjectArgs<'a> {
    pub fn new(bucket_name: &'a str, object_name: &'a str) -> Result<ObjectArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        Ok(ObjectArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct ObjectVersionArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub version_id: Option<&'a str>,
}

impl<'a> ObjectVersionArgs<'a> {
    pub fn new(bucket_name: &'a str, object_name: &'a str) -> Result<ObjectVersionArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        Ok(ObjectVersionArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            version_id: None,
        })
    }
}

pub type RemoveObjectArgs<'a> = ObjectVersionArgs<'a>;

#[derive(Clone, Debug, Default)]
pub struct MakeBucketArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object_lock: bool,
}

impl<'a> MakeBucketArgs<'a> {
    pub fn new(bucket_name: &'a str) -> Result<MakeBucketArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(MakeBucketArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object_lock: false,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct ListBucketsArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
}

impl<'a> ListBucketsArgs<'a> {
    pub fn new() -> ListBucketsArgs<'a> {
        ListBucketsArgs::default()
    }
}

#[derive(Clone, Debug, Default)]
pub struct GetBucketQuotaArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub bucket_name: &'a str,
}

impl<'a> GetBucketQuotaArgs<'a> {
    pub fn new() -> GetBucketQuotaArgs<'a> {
        GetBucketQuotaArgs::default()
    }
}

#[derive(Clone, Debug)]
pub struct SetBucketQuotaArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub bucket_name: &'a str,
    pub quota: &'a Quota,
}

#[derive(Clone, Debug, Default)]
pub struct AbortMultipartUploadArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub upload_id: &'a str,
}

impl<'a> AbortMultipartUploadArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
        upload_id: &'a str,
    ) -> Result<AbortMultipartUploadArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        if upload_id.is_empty() {
            return Err(Error::InvalidUploadId(String::from(
                "upload ID cannot be empty",
            )));
        }

        Ok(AbortMultipartUploadArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            upload_id,
        })
    }
}

#[derive(Clone, Debug)]
pub struct CompleteMultipartUploadArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub upload_id: &'a str,
    pub parts: &'a Vec<Part>,
}

impl<'a> CompleteMultipartUploadArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
        upload_id: &'a str,
        parts: &'a Vec<Part>,
    ) -> Result<CompleteMultipartUploadArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        if upload_id.is_empty() {
            return Err(Error::InvalidUploadId(String::from(
                "upload ID cannot be empty",
            )));
        }

        if parts.is_empty() {
            return Err(Error::EmptyParts(String::from("parts cannot be empty")));
        }

        Ok(CompleteMultipartUploadArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            upload_id,
            parts,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct CreateMultipartUploadArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub headers: Option<&'a Multimap>,
}

impl<'a> CreateMultipartUploadArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
    ) -> Result<CreateMultipartUploadArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        Ok(CreateMultipartUploadArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            headers: None,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct PutObjectApiArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub headers: Option<&'a Multimap>,
    pub user_metadata: Option<&'a Multimap>,
    pub sse: Option<&'a dyn Sse>,
    pub tags: Option<&'a HashMap<String, String>>,
    pub retention: Option<&'a Retention>,
    pub legal_hold: bool,
    pub data: &'a [u8],
    pub query_params: Option<&'a Multimap>,
}

impl<'a> PutObjectApiArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
        data: &'a [u8],
    ) -> Result<PutObjectApiArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        Ok(PutObjectApiArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            headers: None,
            user_metadata: None,
            sse: None,
            tags: None,
            retention: None,
            legal_hold: false,
            data,
            query_params: None,
        })
    }

    pub fn get_headers(&self) -> Multimap {
        object_write_args_headers(
            self.extra_headers,
            self.headers,
            self.user_metadata,
            self.sse,
            self.tags,
            self.retention,
            self.legal_hold,
        )
    }
}

#[derive(Clone, Debug, Default)]
pub struct UploadPartArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub headers: Option<&'a Multimap>,
    pub user_metadata: Option<&'a Multimap>,
    pub sse: Option<&'a dyn Sse>,
    pub tags: Option<&'a HashMap<String, String>>,
    pub retention: Option<&'a Retention>,
    pub legal_hold: bool,
    pub upload_id: &'a str,
    pub part_number: u16,
    pub data: &'a [u8],
}

impl<'a> UploadPartArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
        upload_id: &'a str,
        part_number: u16,
        data: &'a [u8],
    ) -> Result<UploadPartArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        if upload_id.is_empty() {
            return Err(Error::InvalidUploadId(String::from(
                "upload ID cannot be empty",
            )));
        }

        if !(1..=10000).contains(&part_number) {
            return Err(Error::InvalidPartNumber(String::from(
                "part number must be between 1 and 1000",
            )));
        }

        Ok(UploadPartArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            headers: None,
            user_metadata: None,
            sse: None,
            tags: None,
            retention: None,
            legal_hold: false,
            upload_id,
            part_number,
            data,
        })
    }

    pub fn get_headers(&self) -> Multimap {
        object_write_args_headers(
            self.extra_headers,
            self.headers,
            self.user_metadata,
            self.sse,
            self.tags,
            self.retention,
            self.legal_hold,
        )
    }
}

pub struct PutObjectArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub headers: Option<&'a Multimap>,
    pub user_metadata: Option<&'a Multimap>,
    pub sse: Option<&'a dyn Sse>,
    pub tags: Option<&'a HashMap<String, String>>,
    pub retention: Option<&'a Retention>,
    pub legal_hold: bool,
    pub object_size: Option<usize>,
    pub part_size: usize,
    pub part_count: i16,
    pub content_type: &'a str,
    pub stream: &'a mut dyn std::io::Read,
}

impl<'a> PutObjectArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
        stream: &'a mut dyn std::io::Read,
        object_size: Option<usize>,
        part_size: Option<usize>,
    ) -> Result<PutObjectArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        let (psize, part_count) = calc_part_info(object_size, part_size)?;

        Ok(PutObjectArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            headers: None,
            user_metadata: None,
            sse: None,
            tags: None,
            retention: None,
            legal_hold: false,
            object_size,
            part_size: psize,
            part_count,
            content_type: "application/octet-stream",
            stream,
        })
    }

    pub fn get_headers(&self) -> Multimap {
        object_write_args_headers(
            self.extra_headers,
            self.headers,
            self.user_metadata,
            self.sse,
            self.tags,
            self.retention,
            self.legal_hold,
        )
    }
}

#[derive(Clone, Debug, Default)]
pub struct ObjectConditionalReadArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub version_id: Option<&'a str>,
    pub ssec: Option<&'a SseCustomerKey>,
    pub offset: Option<usize>,
    pub length: Option<usize>,
    pub match_etag: Option<&'a str>,
    pub not_match_etag: Option<&'a str>,
    pub modified_since: Option<UtcTime>,
    pub unmodified_since: Option<UtcTime>,
}

impl<'a> ObjectConditionalReadArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
    ) -> Result<ObjectConditionalReadArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        Ok(ObjectConditionalReadArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            version_id: None,
            ssec: None,
            offset: None,
            length: None,
            match_etag: None,
            not_match_etag: None,
            modified_since: None,
            unmodified_since: None,
        })
    }

    fn get_range_value(&self) -> String {
        let (offset, length) = match self.length {
            Some(_) => (Some(self.offset.unwrap_or(0_usize)), self.length),
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

    pub fn get_headers(&self) -> Multimap {
        let mut headers = Multimap::new();

        let range = self.get_range_value();
        if !range.is_empty() {
            headers.insert(String::from("Range"), range);
        }

        if let Some(v) = self.match_etag {
            headers.insert(String::from("if-match"), v.to_string());
        }

        if let Some(v) = self.not_match_etag {
            headers.insert(String::from("if-none-match"), v.to_string());
        }

        if let Some(v) = self.modified_since {
            headers.insert(String::from("if-modified-since"), to_http_header_value(v));
        }

        if let Some(v) = self.unmodified_since {
            headers.insert(String::from("if-unmodified-since"), to_http_header_value(v));
        }

        if let Some(v) = self.ssec {
            merge(&mut headers, &v.headers());
        }

        headers
    }

    pub fn get_copy_headers(&self) -> Multimap {
        let mut headers = Multimap::new();

        let mut copy_source = String::from("/");
        copy_source.push_str(self.bucket);
        copy_source.push('/');
        copy_source.push_str(self.object);
        if let Some(v) = self.version_id {
            copy_source.push_str("?versionId=");
            copy_source.push_str(&urlencode(v));
        }
        headers.insert(String::from("x-amz-copy-source"), copy_source.to_string());

        let range = self.get_range_value();
        if !range.is_empty() {
            headers.insert(String::from("x-amz-copy-source-range"), range);
        }

        if let Some(v) = self.match_etag {
            headers.insert(String::from("x-amz-copy-source-if-match"), v.to_string());
        }

        if let Some(v) = self.not_match_etag {
            headers.insert(
                String::from("x-amz-copy-source-if-none-match"),
                v.to_string(),
            );
        }

        if let Some(v) = self.modified_since {
            headers.insert(
                String::from("x-amz-copy-source-if-modified-since"),
                to_http_header_value(v),
            );
        }

        if let Some(v) = self.unmodified_since {
            headers.insert(
                String::from("x-amz-copy-source-if-unmodified-since"),
                to_http_header_value(v),
            );
        }

        if let Some(v) = self.ssec {
            merge(&mut headers, &v.copy_headers());
        }

        headers
    }
}

pub type GetObjectArgs<'a> = ObjectConditionalReadArgs<'a>;

pub type StatObjectArgs<'a> = ObjectConditionalReadArgs<'a>;

pub type CopySource<'a> = ObjectConditionalReadArgs<'a>;

#[derive(Derivative, Clone, Debug, Default)]
pub struct RemoveObjectsApiArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub bypass_governance_mode: bool,
    #[derivative(Default(value = "true"))]
    pub quiet: bool,
    pub objects: &'a [DeleteObject<'a>],
}

impl<'a> RemoveObjectsApiArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        objects: &'a [DeleteObject],
    ) -> Result<RemoveObjectsApiArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(RemoveObjectsApiArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            bypass_governance_mode: false,
            quiet: true,
            objects,
        })
    }
}

pub struct RemoveObjectsArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub bypass_governance_mode: bool,
    pub objects: &'a mut core::slice::Iter<'a, DeleteObject<'a>>,
}

impl<'a> RemoveObjectsArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        objects: &'a mut core::slice::Iter<'a, DeleteObject<'a>>,
    ) -> Result<RemoveObjectsArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(RemoveObjectsArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            bypass_governance_mode: false,
            objects,
        })
    }
}

pub struct ListObjectsV1Args<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub delimiter: Option<&'a str>,
    pub encoding_type: Option<&'a str>,
    pub max_keys: Option<u16>,
    pub prefix: Option<&'a str>,
    pub marker: Option<String>,
}

impl<'a> ListObjectsV1Args<'a> {
    pub fn new(bucket_name: &'a str) -> Result<ListObjectsV1Args<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(ListObjectsV1Args {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            delimiter: None,
            encoding_type: None,
            max_keys: None,
            prefix: None,
            marker: None,
        })
    }
}

pub struct ListObjectsV2Args<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub delimiter: Option<&'a str>,
    pub encoding_type: Option<&'a str>,
    pub max_keys: Option<u16>,
    pub prefix: Option<&'a str>,
    pub start_after: Option<String>,
    pub continuation_token: Option<String>,
    pub fetch_owner: bool,
    pub include_user_metadata: bool,
}

impl<'a> ListObjectsV2Args<'a> {
    pub fn new(bucket_name: &'a str) -> Result<ListObjectsV2Args<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(ListObjectsV2Args {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            delimiter: None,
            encoding_type: None,
            max_keys: None,
            prefix: None,
            start_after: None,
            continuation_token: None,
            fetch_owner: false,
            include_user_metadata: false,
        })
    }
}

pub struct ListObjectVersionsArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub delimiter: Option<&'a str>,
    pub encoding_type: Option<&'a str>,
    pub max_keys: Option<u16>,
    pub prefix: Option<&'a str>,
    pub key_marker: Option<String>,
    pub version_id_marker: Option<String>,
}

impl<'a> ListObjectVersionsArgs<'a> {
    pub fn new(bucket_name: &'a str) -> Result<ListObjectVersionsArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(ListObjectVersionsArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            delimiter: None,
            encoding_type: None,
            max_keys: None,
            prefix: None,
            key_marker: None,
            version_id_marker: None,
        })
    }
}

pub struct ListObjectsArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub delimiter: Option<&'a str>,
    pub use_url_encoding_type: bool,
    pub marker: Option<&'a str>,      // only for ListObjectsV1.
    pub start_after: Option<&'a str>, // only for ListObjectsV2.
    pub key_marker: Option<&'a str>,  // only for GetObjectVersions.
    pub max_keys: Option<u16>,
    pub prefix: Option<&'a str>,
    pub continuation_token: Option<&'a str>, // only for ListObjectsV2.
    pub fetch_owner: bool,                   // only for ListObjectsV2.
    pub version_id_marker: Option<&'a str>,  // only for GetObjectVersions.
    pub include_user_metadata: bool,         // MinIO extension for ListObjectsV2.
    pub recursive: bool,
    pub use_api_v1: bool,
    pub include_versions: bool,
    pub result_fn: &'a dyn Fn(Vec<Item>) -> bool,
}

impl<'a> ListObjectsArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        result_fn: &'a dyn Fn(Vec<Item>) -> bool,
    ) -> Result<ListObjectsArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(ListObjectsArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            delimiter: None,
            use_url_encoding_type: true,
            marker: None,
            start_after: None,
            key_marker: None,
            max_keys: None,
            prefix: None,
            continuation_token: None,
            fetch_owner: false,
            version_id_marker: None,
            include_user_metadata: false,
            recursive: false,
            use_api_v1: false,
            include_versions: false,
            result_fn,
        })
    }
}

pub struct SelectObjectContentArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub version_id: Option<&'a str>,
    pub ssec: Option<&'a SseCustomerKey>,
    pub request: &'a SelectRequest<'a>,
}

impl<'a> SelectObjectContentArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
        request: &'a SelectRequest,
    ) -> Result<SelectObjectContentArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        Ok(SelectObjectContentArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            version_id: None,
            ssec: None,
            request,
        })
    }
}

pub struct ListenBucketNotificationArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub prefix: Option<&'a str>,
    pub suffix: Option<&'a str>,
    pub events: Option<Vec<&'a str>>,
    pub event_fn: &'a (dyn Fn(NotificationRecords) -> bool + Send + Sync),
}

impl<'a> ListenBucketNotificationArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        event_fn: &'a (dyn Fn(NotificationRecords) -> bool + Send + Sync),
    ) -> Result<ListenBucketNotificationArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(ListenBucketNotificationArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            prefix: None,
            suffix: None,
            events: None,
            event_fn,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct UploadPartCopyArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub upload_id: &'a str,
    pub part_number: u16,
    pub headers: Multimap,
}

impl<'a> UploadPartCopyArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
        upload_id: &'a str,
        part_number: u16,
        headers: Multimap,
    ) -> Result<UploadPartCopyArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        if upload_id.is_empty() {
            return Err(Error::InvalidUploadId(String::from(
                "upload ID cannot be empty",
            )));
        }

        if !(1..=10000).contains(&part_number) {
            return Err(Error::InvalidPartNumber(String::from(
                "part number must be between 1 and 1000",
            )));
        }

        Ok(UploadPartCopyArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            upload_id,
            part_number,
            headers,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct CopyObjectArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub headers: Option<&'a Multimap>,
    pub user_metadata: Option<&'a Multimap>,
    pub sse: Option<&'a dyn Sse>,
    pub tags: Option<&'a HashMap<String, String>>,
    pub retention: Option<&'a Retention>,
    pub legal_hold: bool,
    pub source: CopySource<'a>,
    pub metadata_directive: Option<Directive>,
    pub tagging_directive: Option<Directive>,
}

impl<'a> CopyObjectArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
        source: CopySource<'a>,
    ) -> Result<CopyObjectArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        Ok(CopyObjectArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            headers: None,
            user_metadata: None,
            sse: None,
            tags: None,
            retention: None,
            legal_hold: false,
            source,
            metadata_directive: None,
            tagging_directive: None,
        })
    }

    pub fn get_headers(&self) -> Multimap {
        object_write_args_headers(
            self.extra_headers,
            self.headers,
            self.user_metadata,
            self.sse,
            self.tags,
            self.retention,
            self.legal_hold,
        )
    }
}

#[derive(Clone, Debug, Default)]
pub struct ComposeSource<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub version_id: Option<&'a str>,
    pub ssec: Option<&'a SseCustomerKey>,
    pub offset: Option<usize>,
    pub length: Option<usize>,
    pub match_etag: Option<&'a str>,
    pub not_match_etag: Option<&'a str>,
    pub modified_since: Option<UtcTime>,
    pub unmodified_since: Option<UtcTime>,

    object_size: Option<usize>, // populated by build_headers()
    headers: Option<Multimap>,  // populated by build_headers()
}

impl<'a> ComposeSource<'a> {
    pub fn new(bucket_name: &'a str, object_name: &'a str) -> Result<ComposeSource<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        Ok(ComposeSource {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            version_id: None,
            ssec: None,
            offset: None,
            length: None,
            match_etag: None,
            not_match_etag: None,
            modified_since: None,
            unmodified_since: None,
            object_size: None,
            headers: None,
        })
    }

    pub fn get_object_size(&self) -> usize {
        self.object_size.expect("ABORT: ComposeSource::build_headers() must be called prior to this method invocation. This shoud not happen.")
    }

    pub fn get_headers(&self) -> Multimap {
        return self.headers.as_ref().expect("ABORT: ComposeSource::build_headers() must be called prior to this method invocation. This shoud not happen.").clone();
    }

    pub fn build_headers(&mut self, object_size: usize, etag: String) -> Result<(), Error> {
        if let Some(v) = self.offset {
            if v >= object_size {
                return Err(Error::InvalidComposeSourceOffset(
                    self.bucket.to_string(),
                    self.object.to_string(),
                    self.version_id.map(|v| v.to_string()),
                    v,
                    object_size,
                ));
            }
        }

        if let Some(v) = self.length {
            if v > object_size {
                return Err(Error::InvalidComposeSourceLength(
                    self.bucket.to_string(),
                    self.object.to_string(),
                    self.version_id.map(|v| v.to_string()),
                    v,
                    object_size,
                ));
            }

            if (self.offset.unwrap_or_default() + v) > object_size {
                return Err(Error::InvalidComposeSourceSize(
                    self.bucket.to_string(),
                    self.object.to_string(),
                    self.version_id.map(|v| v.to_string()),
                    self.offset.unwrap_or_default() + v,
                    object_size,
                ));
            }
        }

        self.object_size = Some(object_size);

        let mut headers = Multimap::new();

        let mut copy_source = String::from("/");
        copy_source.push_str(self.bucket);
        copy_source.push('/');
        copy_source.push_str(self.object);
        if let Some(v) = self.version_id {
            copy_source.push_str("?versionId=");
            copy_source.push_str(&urlencode(v));
        }
        headers.insert(String::from("x-amz-copy-source"), copy_source.to_string());

        if let Some(v) = self.match_etag {
            headers.insert(String::from("x-amz-copy-source-if-match"), v.to_string());
        }

        if let Some(v) = self.not_match_etag {
            headers.insert(
                String::from("x-amz-copy-source-if-none-match"),
                v.to_string(),
            );
        }

        if let Some(v) = self.modified_since {
            headers.insert(
                String::from("x-amz-copy-source-if-modified-since"),
                to_http_header_value(v),
            );
        }

        if let Some(v) = self.unmodified_since {
            headers.insert(
                String::from("x-amz-copy-source-if-unmodified-since"),
                to_http_header_value(v),
            );
        }

        if let Some(v) = self.ssec {
            merge(&mut headers, &v.copy_headers());
        }

        if !headers.contains_key("x-amz-copy-source-if-match") {
            headers.insert(String::from("x-amz-copy-source-if-match"), etag);
        }

        self.headers = Some(headers);

        Ok(())
    }
}

pub struct ComposeObjectArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub headers: Option<&'a Multimap>,
    pub user_metadata: Option<&'a Multimap>,
    pub sse: Option<&'a dyn Sse>,
    pub tags: Option<&'a HashMap<String, String>>,
    pub retention: Option<&'a Retention>,
    pub legal_hold: bool,
    pub sources: &'a mut Vec<ComposeSource<'a>>,
}

impl<'a> ComposeObjectArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
        sources: &'a mut Vec<ComposeSource<'a>>,
    ) -> Result<ComposeObjectArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        Ok(ComposeObjectArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            headers: None,
            user_metadata: None,
            sse: None,
            tags: None,
            retention: None,
            legal_hold: false,
            sources,
        })
    }

    pub fn get_headers(&self) -> Multimap {
        object_write_args_headers(
            self.extra_headers,
            self.headers,
            self.user_metadata,
            self.sse,
            self.tags,
            self.retention,
            self.legal_hold,
        )
    }
}

pub type DeleteBucketEncryptionArgs<'a> = BucketArgs<'a>;

pub type GetBucketEncryptionArgs<'a> = BucketArgs<'a>;

#[derive(Clone, Debug)]
pub struct SetBucketEncryptionArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub config: &'a SseConfig,
}

impl<'a> SetBucketEncryptionArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        config: &'a SseConfig,
    ) -> Result<SetBucketEncryptionArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(SetBucketEncryptionArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            config,
        })
    }
}

pub type EnableObjectLegalHoldArgs<'a> = ObjectVersionArgs<'a>;

pub type DisableObjectLegalHoldArgs<'a> = ObjectVersionArgs<'a>;

pub type IsObjectLegalHoldEnabledArgs<'a> = ObjectVersionArgs<'a>;

pub type DeleteBucketLifecycleArgs<'a> = BucketArgs<'a>;

pub type GetBucketLifecycleArgs<'a> = BucketArgs<'a>;

pub struct SetBucketLifecycleArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub config: &'a LifecycleConfig,
}

pub type DeleteBucketNotificationArgs<'a> = BucketArgs<'a>;

pub type GetBucketNotificationArgs<'a> = BucketArgs<'a>;

pub struct SetBucketNotificationArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub config: &'a NotificationConfig,
}

impl<'a> SetBucketNotificationArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        config: &'a NotificationConfig,
    ) -> Result<SetBucketNotificationArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(SetBucketNotificationArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            config,
        })
    }
}

pub type DeleteBucketPolicyArgs<'a> = BucketArgs<'a>;

pub type GetBucketPolicyArgs<'a> = BucketArgs<'a>;

pub struct SetBucketPolicyArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub config: &'a str,
}

impl<'a> SetBucketPolicyArgs<'a> {
    pub fn new(bucket_name: &'a str, config: &'a str) -> Result<SetBucketPolicyArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(SetBucketPolicyArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            config,
        })
    }
}

pub type DeleteBucketReplicationArgs<'a> = BucketArgs<'a>;

pub type GetBucketReplicationArgs<'a> = BucketArgs<'a>;

pub struct SetBucketReplicationArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub config: &'a ReplicationConfig,
}

pub type DeleteBucketTagsArgs<'a> = BucketArgs<'a>;

pub type GetBucketTagsArgs<'a> = BucketArgs<'a>;

pub struct SetBucketTagsArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub tags: &'a HashMap<String, String>,
}

impl<'a> SetBucketTagsArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        tags: &'a HashMap<String, String>,
    ) -> Result<SetBucketTagsArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(SetBucketTagsArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            tags,
        })
    }
}

pub type GetBucketVersioningArgs<'a> = BucketArgs<'a>;

pub struct SetBucketVersioningArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub status: bool,
    pub mfa_delete: Option<bool>,
}

impl<'a> SetBucketVersioningArgs<'a> {
    pub fn new(bucket_name: &'a str, status: bool) -> Result<SetBucketVersioningArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(SetBucketVersioningArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            status,
            mfa_delete: None,
        })
    }
}

pub type DeleteObjectLockConfigArgs<'a> = BucketArgs<'a>;

pub type GetObjectLockConfigArgs<'a> = BucketArgs<'a>;

pub struct SetObjectLockConfigArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub config: &'a ObjectLockConfig,
}

impl<'a> SetObjectLockConfigArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        config: &'a ObjectLockConfig,
    ) -> Result<SetObjectLockConfigArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(SetObjectLockConfigArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            config,
        })
    }
}

pub type GetObjectRetentionArgs<'a> = ObjectVersionArgs<'a>;

pub struct SetObjectRetentionArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub version_id: Option<&'a str>,
    pub bypass_governance_mode: bool,
    pub retention_mode: Option<RetentionMode>,
    pub retain_until_date: Option<UtcTime>,
}

impl<'a> SetObjectRetentionArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
    ) -> Result<SetObjectRetentionArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        Ok(SetObjectRetentionArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            version_id: None,
            bypass_governance_mode: false,
            retention_mode: None,
            retain_until_date: None,
        })
    }
}

pub type DeleteObjectTagsArgs<'a> = ObjectVersionArgs<'a>;

pub type GetObjectTagsArgs<'a> = ObjectVersionArgs<'a>;

pub struct SetObjectTagsArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub version_id: Option<&'a str>,
    pub tags: &'a HashMap<String, String>,
}

impl<'a> SetObjectTagsArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
        tags: &'a HashMap<String, String>,
    ) -> Result<SetObjectTagsArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        Ok(SetObjectTagsArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            version_id: None,
            tags,
        })
    }
}

pub struct GetPresignedObjectUrlArgs<'a> {
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub version_id: Option<&'a str>,
    pub method: Method,
    pub expiry_seconds: Option<u32>,
    pub request_time: Option<UtcTime>,
}

impl<'a> GetPresignedObjectUrlArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
        method: Method,
    ) -> Result<GetPresignedObjectUrlArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        Ok(GetPresignedObjectUrlArgs {
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            version_id: None,
            method,
            expiry_seconds: Some(DEFAULT_EXPIRY_SECONDS),
            request_time: None,
        })
    }
}

pub struct PostPolicy<'a> {
    pub region: Option<&'a str>,
    pub bucket: &'a str,

    expiration: &'a UtcTime,
    eq_conditions: HashMap<String, String>,
    starts_with_conditions: HashMap<String, String>,
    lower_limit: Option<usize>,
    upper_limit: Option<usize>,
}

impl<'a> PostPolicy<'a> {
    const EQ: &str = "eq";
    const STARTS_WITH: &str = "starts-with";
    const ALGORITHM: &str = "AWS4-HMAC-SHA256";

    pub fn new(bucket_name: &'a str, expiration: &'a UtcTime) -> Result<PostPolicy<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(PostPolicy {
            region: None,
            bucket: bucket_name,
            expiration,
            eq_conditions: HashMap::new(),
            starts_with_conditions: HashMap::new(),
            lower_limit: None,
            upper_limit: None,
        })
    }

    fn trim_dollar(value: &str) -> String {
        let mut s = value.to_string();
        if s.starts_with('$') {
            s.remove(0);
        }
        s
    }

    fn is_reserved_element(element: &str) -> bool {
        element == "bucket"
            || element == "x-amz-algorithm"
            || element == "x-amz-credential"
            || element == "x-amz-date"
            || element == "policy"
            || element == "x-amz-signature"
    }

    fn get_credential_string(access_key: &String, date: &UtcTime, region: &String) -> String {
        format!(
            "{}/{}/{}/s3/aws4_request",
            access_key,
            to_signer_date(*date),
            region
        )
    }

    pub fn add_equals_condition(&mut self, element: &str, value: &str) -> Result<(), Error> {
        if element.is_empty() {
            return Err(Error::PostPolicyError(
                "condition element cannot be empty".to_string(),
            ));
        }

        let v = PostPolicy::trim_dollar(element);
        if v == "success_action_redirect" || v == "redirect" || v == "content-length-range" {
            return Err(Error::PostPolicyError(format!(
                "{} is unsupported for equals condition",
                element
            )));
        }

        if PostPolicy::is_reserved_element(v.as_str()) {
            return Err(Error::PostPolicyError(format!("{} cannot set", element)));
        }

        self.eq_conditions.insert(v, value.to_string());
        Ok(())
    }

    pub fn remove_equals_condition(&mut self, element: &str) {
        self.eq_conditions.remove(element);
    }

    pub fn add_starts_with_condition(&mut self, element: &str, value: &str) -> Result<(), Error> {
        if element.is_empty() {
            return Err(Error::PostPolicyError(
                "condition element cannot be empty".to_string(),
            ));
        }

        let v = PostPolicy::trim_dollar(element);
        if v == "success_action_status"
            || v == "content-length-range"
            || (v.starts_with("x-amz-") && v.starts_with("x-amz-meta-"))
        {
            return Err(Error::PostPolicyError(format!(
                "{} is unsupported for starts-with condition",
                element
            )));
        }

        if PostPolicy::is_reserved_element(v.as_str()) {
            return Err(Error::PostPolicyError(format!("{} cannot set", element)));
        }

        self.starts_with_conditions.insert(v, value.to_string());
        Ok(())
    }

    pub fn remove_starts_with_condition(&mut self, element: &str) {
        self.starts_with_conditions.remove(element);
    }

    pub fn add_content_length_range_condition(
        &mut self,
        lower_limit: usize,
        upper_limit: usize,
    ) -> Result<(), Error> {
        if lower_limit > upper_limit {
            return Err(Error::PostPolicyError(
                "lower limit cannot be greater than upper limit".to_string(),
            ));
        }

        self.lower_limit = Some(lower_limit);
        self.upper_limit = Some(upper_limit);
        Ok(())
    }

    pub fn remove_content_length_range_condition(&mut self) {
        self.lower_limit = None;
        self.upper_limit = None;
    }

    pub fn form_data(
        &self,
        access_key: String,
        secret_key: String,
        session_token: Option<String>,
        region: String,
    ) -> Result<HashMap<String, String>, Error> {
        if region.is_empty() {
            return Err(Error::PostPolicyError("region cannot be empty".to_string()));
        }

        if !self.eq_conditions.contains_key("key")
            && !self.starts_with_conditions.contains_key("key")
        {
            return Err(Error::PostPolicyError(
                "key condition must be set".to_string(),
            ));
        }

        let mut conditions: Vec<Value> = Vec::new();
        conditions.push(json!([PostPolicy::EQ, "$bucket", self.bucket]));
        for (key, value) in &self.eq_conditions {
            conditions.push(json!([PostPolicy::EQ, String::from("$") + key, value]));
        }
        for (key, value) in &self.starts_with_conditions {
            conditions.push(json!([
                PostPolicy::STARTS_WITH,
                String::from("$") + key,
                value
            ]));
        }
        if self.lower_limit.is_some() && self.upper_limit.is_some() {
            conditions.push(json!([
                "content-length-range",
                self.lower_limit.unwrap(),
                self.upper_limit.unwrap()
            ]));
        }

        let date = utc_now();
        let credential = PostPolicy::get_credential_string(&access_key, &date, &region);
        let amz_date = to_amz_date(date);
        conditions.push(json!([
            PostPolicy::EQ,
            "$x-amz-algorithm",
            PostPolicy::ALGORITHM
        ]));
        conditions.push(json!([PostPolicy::EQ, "$x-amz-credential", credential]));
        if let Some(v) = &session_token {
            conditions.push(json!([PostPolicy::EQ, "$x-amz-security-token", v]));
        }
        conditions.push(json!([PostPolicy::EQ, "$x-amz-date", amz_date]));

        let policy = json!({
            "expiration": to_iso8601utc(*self.expiration),
            "conditions": conditions,
        });

        let encoded_policy = b64encode(policy.to_string());
        let signature = post_presign_v4(&encoded_policy, &secret_key, date, &region);

        let mut data: HashMap<String, String> = HashMap::new();
        data.insert(
            String::from("x-amz-algorithm"),
            String::from(PostPolicy::ALGORITHM),
        );
        data.insert(String::from("x-amz-credential"), credential);
        data.insert(String::from("x-amz-date"), amz_date);
        data.insert(String::from("policy"), encoded_policy);
        data.insert(String::from("x-amz-signature"), signature);
        if let Some(v) = session_token {
            data.insert(String::from("x-amz-security-token"), v);
        }

        Ok(data)
    }
}

pub struct DownloadObjectArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub version_id: Option<&'a str>,
    pub ssec: Option<&'a SseCustomerKey>,
    pub filename: &'a str,
    pub overwrite: bool,
}

impl<'a> DownloadObjectArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
        filename: &'a str,
    ) -> Result<DownloadObjectArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        Ok(DownloadObjectArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            version_id: None,
            ssec: None,
            filename,
            overwrite: false,
        })
    }
}

pub struct UploadObjectArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub headers: Option<&'a Multimap>,
    pub user_metadata: Option<&'a Multimap>,
    pub sse: Option<&'a dyn Sse>,
    pub tags: Option<&'a HashMap<String, String>>,
    pub retention: Option<&'a Retention>,
    pub legal_hold: bool,
    pub object_size: Option<usize>,
    pub part_size: usize,
    pub part_count: i16,
    pub content_type: &'a str,
    pub filename: &'a str,
}

impl<'a> UploadObjectArgs<'a> {
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
        filename: &'a str,
    ) -> Result<UploadObjectArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        let meta = std::fs::metadata(filename)?;
        if !meta.is_file() {
            return Err(Error::IOError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "not a file",
            )));
        }

        let object_size = Some(meta.len() as usize);
        let (psize, part_count) = calc_part_info(object_size, None)?;

        Ok(UploadObjectArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            headers: None,
            user_metadata: None,
            sse: None,
            tags: None,
            retention: None,
            legal_hold: false,
            object_size,
            part_size: psize,
            part_count,
            content_type: "application/octet-stream",
            filename,
        })
    }
}
