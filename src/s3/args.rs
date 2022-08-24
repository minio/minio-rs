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
use crate::s3::sse::{Sse, SseCustomerKey};
use crate::s3::types::{DeleteObject, Item, Part, Retention, SelectRequest};
use crate::s3::utils::{
    check_bucket_name, merge, to_http_header_value, to_iso8601utc, urlencode, Multimap, UtcTime,
};
use derivative::Derivative;

const MIN_PART_SIZE: usize = 5_242_880; // 5 MiB
const MAX_PART_SIZE: usize = 5_368_709_120; // 5 GiB
const MAX_OBJECT_SIZE: usize = 5_497_558_138_880; // 5 TiB
const MAX_MULTIPART_COUNT: u16 = 10_000;

fn object_write_args_headers(
    extra_headers: Option<&Multimap>,
    headers: Option<&Multimap>,
    user_metadata: Option<&Multimap>,
    sse: Option<&dyn Sse>,
    tags: Option<&std::collections::HashMap<String, String>>,
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
                tagging.push_str("&");
            }
            tagging.push_str(&urlencode(key));
            tagging.push_str("=");
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

    return map;
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

    return Ok((psize, part_count));
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
            upload_id: upload_id,
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

        if parts.len() == 0 {
            return Err(Error::EmptyParts(String::from("parts cannot be empty")));
        }

        Ok(CompleteMultipartUploadArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            object: object_name,
            upload_id: upload_id,
            parts: parts,
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
    pub tags: Option<&'a std::collections::HashMap<String, String>>,
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
            data: data,
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
    pub tags: Option<&'a std::collections::HashMap<String, String>>,
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

        if part_number < 1 || part_number > 10000 {
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
            upload_id: upload_id,
            part_number: part_number,
            data: data,
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
    pub tags: Option<&'a std::collections::HashMap<String, String>>,
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
            object_size: object_size,
            part_size: psize,
            part_count: part_count,
            content_type: "application/octet-stream",
            stream: stream,
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

    pub fn get_headers(&self) -> Multimap {
        let (offset, length) = match self.length {
            Some(_) => (Some(self.offset.unwrap_or(0_usize)), self.length),
            None => (self.offset, None),
        };

        let mut range = String::new();
        if let Some(o) = offset {
            range.push_str("bytes=");
            range.push_str(&o.to_string());
            range.push_str("-");
            if let Some(l) = length {
                range.push_str(&(o + l - 1).to_string());
            }
        }

        let mut headers = Multimap::new();
        if !range.is_empty() {
            headers.insert(String::from("Range"), range.clone());
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

        return headers;
    }

    pub fn get_copy_headers(&self) -> Multimap {
        let mut headers = Multimap::new();

        let mut copy_source = String::from("/");
        copy_source.push_str(self.bucket);
        copy_source.push_str("/");
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

        return headers;
    }
}

pub type GetObjectArgs<'a> = ObjectConditionalReadArgs<'a>;

pub type StatObjectArgs<'a> = ObjectConditionalReadArgs<'a>;

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
            objects: objects,
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
            objects: objects,
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
            result_fn: result_fn,
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
            request: request,
        })
    }
}
