/*
 * MinIO Rust Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use bytes::Bytes;
use futures::stream::Stream;
use hyper::header::{
    HeaderMap, HeaderValue, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE,
    CONTENT_LENGTH, CONTENT_TYPE, ETAG, EXPIRES,
};
use hyper::{body::Body, Response};
use roxmltree;
use std::collections::HashMap;
use std::string;
use time::{strptime, Tm};

#[derive(Clone)]
pub struct Region(String);

impl Region {
    pub fn new(s: &str) -> Region {
        Region(s.to_string())
    }

    pub fn empty() -> Region {
        Region::new("")
    }

    pub fn to_string(&self) -> String {
        self.0.clone()
    }
}

#[derive(Debug)]
pub enum Err {
    InvalidUrl(String),
    InvalidEnv(String),
    InvalidTmFmt(String),
    HttpErr(http::Error),
    HyperErr(hyper::Error),
    FailStatusCodeErr(hyper::StatusCode, Bytes),
    Utf8DecodingErr(string::FromUtf8Error),
    XmlDocParseErr(roxmltree::Error),
    XmlElemMissing(String),
    XmlElemParseErr(String),
    InvalidXmlResponseErr(String),
    MissingRequiredParams,
    RawSvcErr(hyper::StatusCode, Response<Body>),
    XmlWriteErr(String),
}

pub struct GetObjectResp {
    pub user_metadata: Vec<(String, String)>,
    pub object_size: u64,
    pub etag: String,

    // standard headers
    pub content_type: Option<String>,
    pub content_language: Option<String>,
    pub expires: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,

    resp: Response<Body>,
}

impl GetObjectResp {
    pub fn new(r: Response<Body>) -> Result<GetObjectResp, Err> {
        let h = r.headers();

        let cl_opt = hv2s(h.get(CONTENT_LENGTH)).and_then(|l| l.parse::<u64>().ok());
        let etag_opt = hv2s(h.get(ETAG));
        match (cl_opt, etag_opt) {
            (Some(cl), Some(etag)) => Ok(GetObjectResp {
                user_metadata: extract_user_meta(h),
                object_size: cl,
                etag: etag,

                content_type: hv2s(h.get(CONTENT_TYPE)),
                content_language: hv2s(h.get(CONTENT_LANGUAGE)),
                expires: hv2s(h.get(EXPIRES)),
                cache_control: hv2s(h.get(CACHE_CONTROL)),
                content_disposition: hv2s(h.get(CONTENT_DISPOSITION)),
                content_encoding: hv2s(h.get(CONTENT_ENCODING)),

                resp: r,
            }),
            _ => Err(Err::MissingRequiredParams),
        }
    }

    // Consumes GetObjectResp
    pub fn get_object_stream(self) -> impl Stream<Item = hyper::Chunk, Error = Err> {
        self.resp.into_body().map_err(|err| Err::HyperErr(err))
    }
}

fn hv2s(o: Option<&HeaderValue>) -> Option<String> {
    o.and_then(|v| v.to_str().ok()).map(|x| x.to_string())
}

fn extract_user_meta(h: &HeaderMap) -> Vec<(String, String)> {
    h.iter()
        .map(|(k, v)| (k.as_str(), v.to_str()))
        .filter(|(k, v)| k.to_lowercase().starts_with("x-amz-meta-") && v.is_ok())
        .map(|(k, v)| (k.to_string(), v.unwrap_or("").to_string()))
        .collect()
}

fn parse_aws_time(time_str: &str) -> Result<Tm, Err> {
    strptime(time_str, "%Y-%m-%dT%H:%M:%S.%Z")
        .map_err(|err| Err::InvalidTmFmt(format!("{:?}", err)))
}

#[derive(Debug)]
pub struct BucketInfo {
    pub name: String,
    pub created_time: Tm,
}

impl BucketInfo {
    pub fn new(name: &str, time_str: &str) -> Result<BucketInfo, Err> {
        parse_aws_time(time_str).and_then(|ctime| {
            Ok(BucketInfo {
                name: name.to_string(),
                created_time: ctime,
            })
        })
    }
}

#[derive(Debug)]
pub struct ObjectInfo {
    pub name: String,
    pub modified_time: Tm,
    pub etag: String,
    pub size: i64,
    pub storage_class: String,
    pub metadata: HashMap<String, String>,
}

impl ObjectInfo {
    pub fn new(
        name: &str,
        mtime_str: &str,
        etag: &str,
        size: i64,
        storage_class: &str,
        metadata: HashMap<String, String>,
    ) -> Result<ObjectInfo, Err> {
        parse_aws_time(mtime_str).and_then(|mtime| {
            Ok(ObjectInfo {
                name: name.to_string(),
                modified_time: mtime,
                etag: etag.to_string(),
                size: size,
                storage_class: storage_class.to_string(),
                metadata: metadata,
            })
        })
    }
}

#[derive(Debug)]
pub struct ListObjectsResp {
    pub bucket_name: String,
    pub prefix: String,
    pub max_keys: i32,
    pub key_count: i32,
    pub is_truncated: bool,
    pub object_infos: Vec<ObjectInfo>,
    pub common_prefixes: Vec<String>,
    pub next_continuation_token: String,
}
