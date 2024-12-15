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

//! Arguments for [minio::s3::client::Client](crate::s3::client::Client) APIs

use crate::s3::error::Error;
use crate::s3::signer::post_presign_v4;
use crate::s3::sse::{Sse, SseCustomerKey};
use crate::s3::types::{
    Directive, LifecycleConfig, NotificationConfig, ObjectLockConfig, Part, ReplicationConfig,
    Retention, RetentionMode, SelectRequest, SseConfig,
};
use crate::s3::utils::{
    b64encode, check_bucket_name, merge, to_amz_date, to_http_header_value, to_iso8601utc,
    to_signer_date, urlencode, utc_now, Multimap, UtcTime,
};

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
    sse: Option<&(dyn Sse + Send + Sync)>,
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
            return Err(Error::InvalidMinPartSize(v as u64));
        }

        if v > MAX_PART_SIZE {
            return Err(Error::InvalidMaxPartSize(v as u64));
        }
    }

    if let Some(v) = object_size {
        if v > MAX_OBJECT_SIZE {
            return Err(Error::InvalidObjectSize(v as u64));
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
            object_size.unwrap() as u64,
            psize as u64,
            MAX_MULTIPART_COUNT,
        ));
    }

    Ok((psize, part_count))
}

#[derive(Clone, Debug, Default)]
/// Base bucket argument
pub struct BucketArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
}

impl<'a> BucketArgs<'a> {
    /// Returns a bucket argument with given bucket name
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let args = BucketArgs::new("my-bucket").unwrap();
    /// ```
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

/// Argument for [bucket_exists()](crate::s3::client::Client::bucket_exists) API
pub type BucketExistsArgs<'a> = BucketArgs<'a>;

/// Argument for [remove_bucket()](crate::s3::client::Client::remove_bucket) API
pub type RemoveBucketArgs<'a> = BucketArgs<'a>;

#[derive(Clone, Debug, Default)]
/// Base object argument
pub struct ObjectArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
}

impl<'a> ObjectArgs<'a> {
    /// Returns a object argument with given bucket name and object name
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let args = ObjectArgs::new("my-bucket", "my-object").unwrap();
    /// ```
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
/// Base object argument with optional version ID
pub struct ObjectVersionArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub version_id: Option<&'a str>,
}

impl<'a> ObjectVersionArgs<'a> {
    /// Returns a object argument with given bucket name and object name
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let mut args = ObjectVersionArgs::new("my-bucket", "my-object").unwrap();
    /// args.version_id = Some("ef090b89-cfbe-4a04-aa90-03c09110ba23");
    /// ```
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

/// Argument for [remove_object()](crate::s3::client::Client::remove_object) API
pub type RemoveObjectArgs<'a> = ObjectVersionArgs<'a>;

#[derive(Clone, Debug, Default)]
/// Argument for [make_bucket()](crate::s3::client::Client::make_bucket) API
pub struct MakeBucketArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object_lock: bool,
}

impl<'a> MakeBucketArgs<'a> {
    /// Returns argument for [make_bucket()](crate::s3::client::Client::make_bucket) API with given bucket name
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let args = MakeBucketArgs::new("my-bucket").unwrap();
    /// ```
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
/// Argument for [abort_multipart_upload()](crate::s3::client::Client::abort_multipart_upload) API
pub struct AbortMultipartUploadArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub upload_id: &'a str,
}

impl<'a> AbortMultipartUploadArgs<'a> {
    /// Returns argument for [abort_multipart_upload()](crate::s3::client::Client::abort_multipart_upload) API with given bucket name, object name and upload ID
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let args = AbortMultipartUploadArgs::new(
    ///     "my-bucket",
    ///     "my-object",
    ///     "c53a2b73-f5e6-484a-9bc0-09cce13e8fd0",
    /// ).unwrap();
    /// ```
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
/// Argument for [complete_multipart_upload()](crate::s3::client::Client::complete_multipart_upload) API
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
    /// Returns argument for [complete_multipart_upload()](crate::s3::client::Client::complete_multipart_upload) API with given bucket name, object name, upload ID and parts information
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use minio::s3::types::Part;
    /// let mut parts: Vec<Part> = Vec::new();
    /// parts.push(Part {number: 1, etag: String::from("0b2daaba1d0b52a15a98c7ab6927347a")});
    /// parts.push(Part {number: 2, etag: String::from("acc0485d88ec53f47b599e4e8998706d")});
    /// let args = CompleteMultipartUploadArgs::new(
    ///     "my-bucket",
    ///     "my-object",
    ///     "c53a2b73-f5e6-484a-9bc0-09cce13e8fd0",
    ///     &parts,
    /// ).unwrap();
    /// ```
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
/// Argument for [create_multipart_upload()](crate::s3::client::Client::create_multipart_upload) API
pub struct CreateMultipartUploadArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub headers: Option<&'a Multimap>,
}

impl<'a> CreateMultipartUploadArgs<'a> {
    /// Returns argument for [create_multipart_upload()](crate::s3::client::Client::create_multipart_upload) API with given bucket name and object name
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let args = CreateMultipartUploadArgs::new("my-bucket", "my-object").unwrap();
    /// ```
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
/// Argument for [put_object_api()](crate::s3::client::Client::put_object_api) S3 API
pub struct PutObjectApiArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub headers: Option<&'a Multimap>,
    pub user_metadata: Option<&'a Multimap>,
    pub sse: Option<&'a (dyn Sse + Send + Sync)>,
    pub tags: Option<&'a HashMap<String, String>>,
    pub retention: Option<&'a Retention>,
    pub legal_hold: bool,
    pub data: &'a [u8],
    pub query_params: Option<&'a Multimap>,
}

impl<'a> PutObjectApiArgs<'a> {
    /// Returns argument for [put_object_api()](crate::s3::client::Client::put_object_api) S3 API with given bucket name, object name and data
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let data: &[u8] = &[65, 67, 69];
    /// let args = PutObjectApiArgs::new("my-bucket", "my-object", data).unwrap();
    /// ```
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
/// Argument for [upload_part()](crate::s3::client::Client::upload_part) S3 API
pub struct UploadPartArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub headers: Option<&'a Multimap>,
    pub user_metadata: Option<&'a Multimap>,
    pub sse: Option<&'a (dyn Sse + Send + Sync)>,
    pub tags: Option<&'a HashMap<String, String>>,
    pub retention: Option<&'a Retention>,
    pub legal_hold: bool,
    pub upload_id: &'a str,
    pub part_number: u16,
    pub data: &'a [u8],
}

impl<'a> UploadPartArgs<'a> {
    /// Returns argument for [upload_part()](crate::s3::client::Client::upload_part) API with given bucket name, object name, part number and data
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let data: &[u8] = &[65, 67, 69];
    /// let args = UploadPartArgs::new(
    ///     "my-bucket",
    ///     "my-object",
    ///     "c53a2b73-f5e6-484a-9bc0-09cce13e8fd0",
    ///     3,
    ///     data,
    /// ).unwrap();
    /// ```
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

/// Argument for [put_object()](crate::s3::client::Client::put_object) API
pub struct PutObjectArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub headers: Option<&'a Multimap>,
    pub user_metadata: Option<&'a Multimap>,
    pub sse: Option<&'a (dyn Sse + Send + Sync)>,
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
    /// Returns argument for [put_object()](crate::s3::client::Client::put_object) API with given bucket name, object name, stream, optional object size and optional part size
    ///
    /// * If stream size is known and wanted to create object with entire stream data, pass stream size as object size.
    /// * If part size is omitted, this API calculates optimal part size for given object size.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use minio::s3::args::*;
    /// use std::fs::File;
    /// let filename = "asiaphotos-2015.zip";
    /// let meta = std::fs::metadata(filename).unwrap();
    /// let object_size = Some(meta.len() as usize);
    /// let mut file = File::open(filename).unwrap();
    /// let args = PutObjectArgs::new("my-bucket", "my-object", &mut file, object_size, None).unwrap();
    /// ```
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
/// Base argument for object conditional read APIs
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
    /// Returns a object conditional read argument with given bucket name and object name
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let args = ObjectConditionalReadArgs::new("my-bucket", "my-object").unwrap();
    /// ```
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

/// Argument for [get_object()](crate::s3::client::Client::get_object) API
pub type GetObjectArgs<'a> = ObjectConditionalReadArgs<'a>;

/// Argument for [stat_object()](crate::s3::client::Client::stat_object) API
pub type StatObjectArgs<'a> = ObjectConditionalReadArgs<'a>;

/// Source object information for [copy object argument](CopyObjectArgs)
pub type CopySource<'a> = ObjectConditionalReadArgs<'a>;

/// Argument for [select_object_content()](crate::s3::client::Client::select_object_content) API
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
    /// Returns argument for [select_object_content()](crate::s3::client::Client::select_object_content) API with given bucket name, object name and callback function for results.
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use minio::s3::types::*;
    /// let request = SelectRequest::new_csv_input_output(
    ///     "select * from S3Object",
    ///     CsvInputSerialization {
    ///         compression_type: None,
    ///         allow_quoted_record_delimiter: false,
    ///         comments: None,
    ///         field_delimiter: None,
    ///         file_header_info: Some(FileHeaderInfo::USE),
    ///         quote_character: None,
    ///         quote_escape_character: None,
    ///         record_delimiter: None,
    ///     },
    ///     CsvOutputSerialization {
    ///         field_delimiter: None,
    ///         quote_character: None,
    ///         quote_escape_character: None,
    ///         quote_fields: Some(QuoteFields::ASNEEDED),
    ///         record_delimiter: None,
    ///     },
    /// ).unwrap();
    /// let args = SelectObjectContentArgs::new("my-bucket", "my-object", &request).unwrap();
    /// ```
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

#[derive(Clone, Debug, Default)]
/// Argument for [upload_part_copy()](crate::s3::client::Client::upload_part_copy) S3 API
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
    /// Returns argument for [upload_part_copy()](crate::s3::client::Client::upload_part_copy) S3 API with given bucket name, object name, upload ID, part number and headers
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let src = CopySource::new("my-src-bucket", "my-src-object").unwrap();
    /// let args = UploadPartCopyArgs::new(
    ///     "my-bucket",
    ///     "my-object",
    ///     "c53a2b73-f5e6-484a-9bc0-09cce13e8fd0",
    ///     3,
    ///     src.get_copy_headers(),
    /// ).unwrap();
    /// ```
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
/// Argument for [copy_object()](crate::s3::client::Client::copy_object) API
pub struct CopyObjectArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub headers: Option<&'a Multimap>,
    pub user_metadata: Option<&'a Multimap>,
    pub sse: Option<&'a (dyn Sse + Send + Sync)>,
    pub tags: Option<&'a HashMap<String, String>>,
    pub retention: Option<&'a Retention>,
    pub legal_hold: bool,
    pub source: CopySource<'a>,
    pub metadata_directive: Option<Directive>,
    pub tagging_directive: Option<Directive>,
}

impl<'a> CopyObjectArgs<'a> {
    /// Returns argument for [copy_object()](crate::s3::client::Client::copy_object) API with given bucket name, object name and copy source.
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let src = CopySource::new("my-src-bucket", "my-src-object").unwrap();
    /// let args = CopyObjectArgs::new("my-bucket", "my-object", src).unwrap();
    /// ```
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
/// Source object information for [compose object argument](ComposeObjectArgs)
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
    /// Returns a compose source with given bucket name and object name
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let src = ComposeSource::new("my-src-bucket", "my-src-object").unwrap();
    /// ```
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
        self.headers.as_ref().expect("ABORT: ComposeSource::build_headers() must be called prior to this method invocation. This shoud not happen.").clone()
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

/// Argument for [compose_object()](crate::s3::client::Client::compose_object) API
pub struct ComposeObjectArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub headers: Option<&'a Multimap>,
    pub user_metadata: Option<&'a Multimap>,
    pub sse: Option<&'a (dyn Sse + Send + Sync)>,
    pub tags: Option<&'a HashMap<String, String>>,
    pub retention: Option<&'a Retention>,
    pub legal_hold: bool,
    pub sources: &'a mut Vec<ComposeSource<'a>>,
}

impl<'a> ComposeObjectArgs<'a> {
    /// Returns argument for [compose_object()](crate::s3::client::Client::compose_object) API with given bucket name, object name and list of compose sources.
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let mut sources: Vec<ComposeSource> = Vec::new();
    /// sources.push(ComposeSource::new("my-src-bucket", "my-src-object-1").unwrap());
    /// sources.push(ComposeSource::new("my-src-bucket", "my-src-object-2").unwrap());
    /// let args = ComposeObjectArgs::new("my-bucket", "my-object", &mut sources).unwrap();
    /// ```
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

/// Argument for [delete_bucket_encryption()](crate::s3::client::Client::delete_bucket_encryption) API
pub type DeleteBucketEncryptionArgs<'a> = BucketArgs<'a>;

/// Argument for [get_bucket_encryption()](crate::s3::client::Client::get_bucket_encryption) API
pub type GetBucketEncryptionArgs<'a> = BucketArgs<'a>;

#[derive(Clone, Debug)]
/// Argument for [set_bucket_encryption()](crate::s3::client::Client::set_bucket_encryption) API
pub struct SetBucketEncryptionArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub config: &'a SseConfig,
}

impl<'a> SetBucketEncryptionArgs<'a> {
    /// Returns argument for [set_bucket_encryption()](crate::s3::client::Client::set_bucket_encryption) API with given bucket name and configuration
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use minio::s3::types::SseConfig;
    /// let args = SetBucketEncryptionArgs::new("my-bucket", &SseConfig::s3()).unwrap();
    /// ```
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

/// Argument for [enable_object_legal_hold()](crate::s3::client::Client::enable_object_legal_hold) API
pub type EnableObjectLegalHoldArgs<'a> = ObjectVersionArgs<'a>;

/// Argument for [disable_object_legal_hold()](crate::s3::client::Client::disable_object_legal_hold) API
pub type DisableObjectLegalHoldArgs<'a> = ObjectVersionArgs<'a>;

/// Argument for [is_object_legal_hold_enabled()](crate::s3::client::Client::is_object_legal_hold_enabled) API
pub type IsObjectLegalHoldEnabledArgs<'a> = ObjectVersionArgs<'a>;

/// Argument for [delete_bucket_lifecycle()](crate::s3::client::Client::delete_bucket_lifecycle) API
pub type DeleteBucketLifecycleArgs<'a> = BucketArgs<'a>;

/// Argument for [get_bucket_lifecycle()](crate::s3::client::Client::get_bucket_lifecycle) API
pub type GetBucketLifecycleArgs<'a> = BucketArgs<'a>;

/// Argument for [set_bucket_lifecycle()](crate::s3::client::Client::set_bucket_lifecycle) API
pub struct SetBucketLifecycleArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub config: &'a LifecycleConfig,
}

impl<'a> SetBucketLifecycleArgs<'a> {
    /// Returns argument for [set_bucket_lifecycle()](crate::s3::client::Client::set_bucket_lifecycle) API with given bucket name and configuration
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use minio::s3::types::*;
    /// let mut rules: Vec<LifecycleRule> = Vec::new();
    /// rules.push(LifecycleRule {
    ///     abort_incomplete_multipart_upload_days_after_initiation: None,
    ///     expiration_date: None,
    ///     expiration_days: Some(365),
    ///     expiration_expired_object_delete_marker: None,
    ///     filter: Filter {and_operator: None, prefix: Some(String::from("logs/")), tag: None},
    ///     id: String::from("rule1"),
    ///     noncurrent_version_expiration_noncurrent_days: None,
    ///     noncurrent_version_transition_noncurrent_days: None,
    ///     noncurrent_version_transition_storage_class: None,
    ///     status: true,
    ///     transition_date: None,
    ///     transition_days: None,
    ///     transition_storage_class: None,
    /// });
    /// let mut config = LifecycleConfig {rules};
    /// let args = SetBucketLifecycleArgs::new("my-bucket", &config).unwrap();
    /// ```
    pub fn new(
        bucket_name: &'a str,
        config: &'a LifecycleConfig,
    ) -> Result<SetBucketLifecycleArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(SetBucketLifecycleArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            config,
        })
    }
}

/// Argument for [delete_bucket_notification()](crate::s3::client::Client::delete_bucket_notification) API
pub type DeleteBucketNotificationArgs<'a> = BucketArgs<'a>;

/// Argument for [delete_bucket_notification()](crate::s3::client::Client::delete_bucket_notification) API
pub type GetBucketNotificationArgs<'a> = BucketArgs<'a>;

/// Argument for [set_bucket_notification()](crate::s3::client::Client::set_bucket_notification) API
pub struct SetBucketNotificationArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub config: &'a NotificationConfig,
}

impl<'a> SetBucketNotificationArgs<'a> {
    /// Returns argument for [set_bucket_notification()](crate::s3::client::Client::set_bucket_notification) API with given bucket name and configuration
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use minio::s3::types::*;
    /// let config = NotificationConfig {
    ///     cloud_func_config_list: None,
    ///     queue_config_list: Some(vec![QueueConfig {
    ///         events: vec![
    ///             String::from("s3:ObjectCreated:Put"),
    ///             String::from("s3:ObjectCreated:Copy"),
    ///         ],
    ///         id: None,
    ///         prefix_filter_rule: Some(PrefixFilterRule {
    ///             value: String::from("images"),
    ///         }),
    ///         suffix_filter_rule: Some(SuffixFilterRule {
    ///             value: String::from("pg"),
    ///         }),
    ///         queue: String::from("arn:minio:sqs::miniojavatest:webhook"),
    ///     }]),
    ///     topic_config_list: None,
    /// };
    /// let args = SetBucketNotificationArgs::new("my-bucket", &config).unwrap();
    /// ```
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

/// Argument for [delete_bucket_policy()](crate::s3::client::Client::delete_bucket_policy) API
pub type DeleteBucketPolicyArgs<'a> = BucketArgs<'a>;

/// Argument for [get_bucket_policy()](crate::s3::client::Client::get_bucket_policy) API
pub type GetBucketPolicyArgs<'a> = BucketArgs<'a>;

/// Argument for [set_bucket_policy()](crate::s3::client::Client::set_bucket_policy) API
pub struct SetBucketPolicyArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub config: &'a str,
}

impl<'a> SetBucketPolicyArgs<'a> {
    /// Returns argument for [set_bucket_policy()](crate::s3::client::Client::set_bucket_policy) API with given bucket name and configuration
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let config = r#"{
    ///   "Version": "2012-10-17",
    ///   "Statement": [
    ///     {
    ///       "Effect": "Allow",
    ///       "Principal": {
    ///         "AWS": "*"
    ///       },
    ///       "Action": [
    ///         "s3:GetBucketLocation",
    ///         "s3:ListBucket"
    ///       ],
    ///       "Resource": "arn:aws:s3:::my-bucket"
    ///     },
    ///     {
    ///       "Effect": "Allow",
    ///       "Principal": {
    ///         "AWS": "*"
    ///       },
    ///       "Action": "s3:GetObject",
    ///       "Resource": "arn:aws:s3:::my-bucket/*"
    ///     }
    ///   ]
    /// }"#;
    /// let args = SetBucketPolicyArgs::new("my-bucket", config).unwrap();
    /// ```
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

/// Argument for [delete_bucket_replication()](crate::s3::client::Client::delete_bucket_replication) API
pub type DeleteBucketReplicationArgs<'a> = BucketArgs<'a>;

/// Argument for [get_bucket_replication()](crate::s3::client::Client::get_bucket_replication) API
pub type GetBucketReplicationArgs<'a> = BucketArgs<'a>;

/// Argument for [set_bucket_replication()](crate::s3::client::Client::set_bucket_replication) API
pub struct SetBucketReplicationArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub config: &'a ReplicationConfig,
}

impl<'a> SetBucketReplicationArgs<'a> {
    /// Returns argument for [set_bucket_replication()](crate::s3::client::Client::set_bucket_replication) API with given bucket name and configuration
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use minio::s3::types::*;
    /// use std::collections::HashMap;
    /// let mut tags: HashMap<String, String> = HashMap::new();
    /// tags.insert(String::from("key1"), String::from("value1"));
    /// tags.insert(String::from("key2"), String::from("value2"));
    /// let mut rules: Vec<ReplicationRule> = Vec::new();
    /// rules.push(ReplicationRule {
    ///     destination: Destination {
    ///         bucket_arn: String::from("REPLACE-WITH-ACTUAL-DESTINATION-BUCKET-ARN"),
    ///         access_control_translation: None,
    ///         account: None,
    ///         encryption_config: None,
    ///         metrics: None,
    ///         replication_time: None,
    ///         storage_class: None,
    ///     },
    ///     delete_marker_replication_status: None,
    ///     existing_object_replication_status: None,
    ///     filter: Some(Filter {
    ///         and_operator: Some(AndOperator {
    ///     	    prefix: Some(String::from("TaxDocs")),
    ///     	    tags: Some(tags),
    ///         }),
    ///         prefix: None,
    ///         tag: None,
    ///     }),
    ///     id: Some(String::from("rule1")),
    ///     prefix: None,
    ///     priority: Some(1),
    ///     source_selection_criteria: None,
    ///     delete_replication_status: Some(false),
    ///     status: true,
    /// });
    /// let config = ReplicationConfig {role: None, rules: rules};
    /// let args = SetBucketReplicationArgs::new("my-bucket", &config).unwrap();
    /// ```
    pub fn new(
        bucket_name: &'a str,
        config: &'a ReplicationConfig,
    ) -> Result<SetBucketReplicationArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        Ok(SetBucketReplicationArgs {
            extra_headers: None,
            extra_query_params: None,
            region: None,
            bucket: bucket_name,
            config,
        })
    }
}

/// Argument for [delete_bucket_tags()](crate::s3::client::Client::delete_bucket_tags) API
pub type DeleteBucketTagsArgs<'a> = BucketArgs<'a>;

/// Argument for [get_bucket_tags()](crate::s3::client::Client::get_bucket_tags) API
pub type GetBucketTagsArgs<'a> = BucketArgs<'a>;

/// Argument for [set_bucket_tags()](crate::s3::client::Client::set_bucket_tags) API
pub struct SetBucketTagsArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub tags: &'a HashMap<String, String>,
}

impl<'a> SetBucketTagsArgs<'a> {
    /// Returns argument for [set_bucket_tags()](crate::s3::client::Client::set_bucket_tags) API with given bucket name and tags
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use std::collections::HashMap;
    /// let mut tags: HashMap<String, String> = HashMap::new();
    /// tags.insert(String::from("Project"), String::from("Project One"));
    /// tags.insert(String::from("User"), String::from("jsmith"));
    /// let args = SetBucketTagsArgs::new("my-bucket", &tags).unwrap();
    /// ```
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

/// Argument for [set_bucket_versioning()](crate::s3::client::Client::set_bucket_versioning) API
pub struct SetBucketVersioningArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub status: bool,
    pub mfa_delete: Option<bool>,
}

impl<'a> SetBucketVersioningArgs<'a> {
    /// Returns argument for [set_bucket_versioning()](crate::s3::client::Client::set_bucket_versioning) API with given bucket name and status
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let args = SetBucketVersioningArgs::new("my-bucket", true).unwrap();
    /// ```
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

/// Argument for [delete_object_lock_config()](crate::s3::client::Client::delete_object_lock_config) API
pub type DeleteObjectLockConfigArgs<'a> = BucketArgs<'a>;

/// Argument for [get_object_lock_config()](crate::s3::client::Client::get_object_lock_config) API
pub type GetObjectLockConfigArgs<'a> = BucketArgs<'a>;

/// Argument for [set_object_lock_config()](crate::s3::client::Client::set_object_lock_config) API
pub struct SetObjectLockConfigArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub config: &'a ObjectLockConfig,
}

impl<'a> SetObjectLockConfigArgs<'a> {
    /// Returns argument for [set_object_lock_config()](crate::s3::client::Client::set_object_lock_config) API with given bucket name and configuration
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use minio::s3::types::*;
    /// let config = ObjectLockConfig::new(RetentionMode::GOVERNANCE, Some(100), None).unwrap();
    /// let args = SetObjectLockConfigArgs::new("my-bucket", &config).unwrap();
    /// ```
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

/// Argument for [get_object_retention()](crate::s3::client::Client::get_object_retention) API
pub type GetObjectRetentionArgs<'a> = ObjectVersionArgs<'a>;

/// Argument for [set_object_retention()](crate::s3::client::Client::set_object_retention) API
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
    /// Returns argument for [set_object_retention()](crate::s3::client::Client::set_object_retention) API with given bucket name, object name, retention mode and retain-until date.
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use minio::s3::types::RetentionMode;
    /// use minio::s3::utils::*;
    /// use chrono::Timelike;
    /// let args = SetObjectRetentionArgs::new(
    ///     "my-bucket",
    ///     "my-object",
    ///     Some(RetentionMode::COMPLIANCE),
    ///     Some(utc_now().with_nanosecond(0).unwrap()),
    /// ).unwrap();
    /// ```
    pub fn new(
        bucket_name: &'a str,
        object_name: &'a str,
        retention_mode: Option<RetentionMode>,
        retain_until_date: Option<UtcTime>,
    ) -> Result<SetObjectRetentionArgs<'a>, Error> {
        check_bucket_name(bucket_name, true)?;

        if object_name.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        if retention_mode.is_some() ^ retain_until_date.is_some() {
            return Err(Error::InvalidRetentionConfig(String::from(
                "both mode and retain_until_date must be set or unset",
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
            retention_mode,
            retain_until_date,
        })
    }
}

/// Argument for [delete_object_tags()](crate::s3::client::Client::delete_object_tags) API
pub type DeleteObjectTagsArgs<'a> = ObjectVersionArgs<'a>;

/// Argument for [get_object_tags()](crate::s3::client::Client::get_object_tags) API
pub type GetObjectTagsArgs<'a> = ObjectVersionArgs<'a>;

/// Argument for [set_object_tags()](crate::s3::client::Client::set_object_tags) API
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
    /// Returns argument for [set_object_tags()](crate::s3::client::Client::set_object_tags) API with given bucket name, object name and tags
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use std::collections::HashMap;
    /// let mut tags: HashMap<String, String> = HashMap::new();
    /// tags.insert(String::from("Project"), String::from("Project One"));
    /// tags.insert(String::from("User"), String::from("jsmith"));
    /// let args = SetObjectTagsArgs::new("my-bucket", "my-object", &tags).unwrap();
    /// ```
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

/// Argument for [get_presigned_object_url()](crate::s3::client::Client::get_presigned_object_url) API
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
    /// Returns argument for [get_presigned_object_url()](crate::s3::client::Client::get_presigned_object_url) API with given bucket name, object name and method
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use hyper::http::Method;
    /// let args = GetPresignedObjectUrlArgs::new("my-bucket", "my-object", Method::GET).unwrap();
    /// ```
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

/// Post policy information for presigned post policy form-data
///
/// Condition elements and respective condition for Post policy is available <a
/// href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html#sigv4-PolicyConditions">here</a>.
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
    const EQ: &'static str = "eq";
    const STARTS_WITH: &'static str = "starts-with";
    const ALGORITHM: &'static str = "AWS4-HMAC-SHA256";

    /// Returns post policy with given bucket name and expiration
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use minio::s3::utils::*;
    /// use chrono::Duration;
    /// let expiration = utc_now() + Duration::days(7);
    /// let policy = PostPolicy::new("my-bucket", &expiration).unwrap();
    /// ```
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

    /// Adds equals condition for given element and value
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use minio::s3::utils::*;
    /// use chrono::Duration;
    /// let expiration = utc_now() + Duration::days(7);
    /// let mut policy = PostPolicy::new("my-bucket", &expiration).unwrap();
    ///
    /// // Add condition that 'key' (object name) equals to 'my-objectname'
    /// policy.add_equals_condition("key", "my-object");
    /// ```
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

    /// Removes equals condition for given element
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use minio::s3::utils::*;
    /// use chrono::Duration;
    /// let expiration = utc_now() + Duration::days(7);
    /// let mut policy = PostPolicy::new("my-bucket", &expiration).unwrap();
    /// policy.add_equals_condition("key", "my-object");
    ///
    /// policy.remove_equals_condition("key");
    /// ```
    pub fn remove_equals_condition(&mut self, element: &str) {
        self.eq_conditions.remove(element);
    }

    /// Adds starts-with condition for given element and value
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use minio::s3::utils::*;
    /// use chrono::Duration;
    /// let expiration = utc_now() + Duration::days(7);
    /// let mut policy = PostPolicy::new("my-bucket", &expiration).unwrap();
    ///
    /// // Add condition that 'Content-Type' starts with 'image/'
    /// policy.add_starts_with_condition("Content-Type", "image/");
    /// ```
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

    /// Removes starts-with condition for given element
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use minio::s3::utils::*;
    /// use chrono::Duration;
    /// let expiration = utc_now() + Duration::days(7);
    /// let mut policy = PostPolicy::new("my-bucket", &expiration).unwrap();
    /// policy.add_starts_with_condition("Content-Type", "image/");
    ///
    /// policy.remove_starts_with_condition("Content-Type");
    /// ```
    pub fn remove_starts_with_condition(&mut self, element: &str) {
        self.starts_with_conditions.remove(element);
    }

    /// Adds content-length range condition with given lower and upper limits
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// use minio::s3::utils::*;
    /// use chrono::Duration;
    /// let expiration = utc_now() + Duration::days(7);
    /// let mut policy = PostPolicy::new("my-bucket", &expiration).unwrap();
    ///
    /// // Add condition that 'content-length-range' is between 64kiB to 10MiB
    /// policy.add_content_length_range_condition(64 * 1024, 10 * 1024 * 1024);
    /// ```
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

    /// Removes content-length range condition
    pub fn remove_content_length_range_condition(&mut self) {
        self.lower_limit = None;
        self.upper_limit = None;
    }

    /// Generates form data for given access/secret keys, optional session token and region.
    /// The returned map contains `x-amz-algorithm`, `x-amz-credential`, `x-amz-security-token`, `x-amz-date`, `policy` and `x-amz-signature` keys and values.
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

/// Argument for [download_object()](crate::s3::client::Client::download_object) API
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
    /// Returns argument for [download_object()](crate::s3::client::Client::download_object) API with given bucket name, object name and filename
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::args::*;
    /// let args = DownloadObjectArgs::new("my-bucket", "my-object", "/path/to/my/object/download").unwrap();
    /// ```
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

/// Argument for [upload_object()](crate::s3::client::Client::upload_object) API
pub struct UploadObjectArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub extra_query_params: Option<&'a Multimap>,
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub object: &'a str,
    pub headers: Option<&'a Multimap>,
    pub user_metadata: Option<&'a Multimap>,
    pub sse: Option<&'a (dyn Sse + Send + Sync)>,
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
    /// Returns argument for [upload_object()](crate::s3::client::Client::upload_object) API with given bucket name, object name and filename
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use minio::s3::args::*;
    /// let args = UploadObjectArgs::new("my-bucket", "my-object", "asiaphotos-2015.zip").unwrap();
    /// ```
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
