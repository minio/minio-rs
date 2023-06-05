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
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::engine::Engine as _;
use byteorder::{BigEndian, ReadBytesExt};
use chrono::{DateTime, Datelike, NaiveDateTime, ParseError, Utc};
use crc::{Crc, CRC_32_ISO_HDLC};
use lazy_static::lazy_static;
use md5::compute as md5compute;
use multimap::MultiMap;
use regex::Regex;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
pub use urlencoding::decode as urldecode;
pub use urlencoding::encode as urlencode;
use xmltree::Element;

pub type UtcTime = DateTime<Utc>;

pub type Multimap = MultiMap<String, String>;

pub fn b64encode<T: AsRef<[u8]>>(input: T) -> String {
    BASE64.encode(input)
}

pub fn merge(m1: &mut Multimap, m2: &Multimap) {
    for (key, values) in m2.iter_all() {
        for value in values {
            m1.insert(key.to_string(), value.to_string());
        }
    }
}

pub fn crc32(data: &[u8]) -> u32 {
    Crc::<u32>::new(&CRC_32_ISO_HDLC).checksum(data)
}

pub fn uint32(mut data: &[u8]) -> Result<u32, std::io::Error> {
    data.read_u32::<BigEndian>()
}

pub fn sha256_hash(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

pub fn md5sum_hash(data: &[u8]) -> String {
    b64encode(md5compute(data).as_slice())
}

pub fn utc_now() -> UtcTime {
    chrono::offset::Utc::now()
}

pub fn to_signer_date(time: UtcTime) -> String {
    time.format("%Y%m%d").to_string()
}

pub fn to_amz_date(time: UtcTime) -> String {
    time.format("%Y%m%dT%H%M%SZ").to_string()
}

pub fn to_http_header_value(time: UtcTime) -> String {
    format!(
        "{}, {} {} {} GMT",
        time.weekday(),
        time.day(),
        match time.month() {
            1 => "Jan",
            2 => "Feb",
            3 => "Mar",
            4 => "Apr",
            5 => "May",
            6 => "Jun",
            7 => "Jul",
            8 => "Aug",
            9 => "Sep",
            10 => "Oct",
            11 => "Nov",
            12 => "Dec",
            _ => "",
        },
        time.format("%Y %H:%M:%S")
    )
}

pub fn to_iso8601utc(time: UtcTime) -> String {
    time.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string()
}

pub fn from_iso8601utc(s: &str) -> Result<UtcTime, ParseError> {
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(
        match NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S.%3fZ") {
            Ok(d) => d,
            _ => NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ")?,
        },
        Utc,
    ))
}

pub fn from_http_header_value(s: &str) -> Result<UtcTime, ParseError> {
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(
        NaiveDateTime::parse_from_str(s, "%a, %d %b %Y %H:%M:%S GMT")?,
        Utc,
    ))
}

pub fn to_http_headers(map: &Multimap) -> Vec<String> {
    let mut headers: Vec<String> = Vec::new();
    for (key, values) in map.iter_all() {
        for value in values {
            let mut s = String::new();
            s.push_str(key);
            s.push_str(": ");
            s.push_str(value);
            headers.push(s);
        }
    }
    headers
}

pub fn to_query_string(map: &Multimap) -> String {
    let mut query = String::new();
    for (key, values) in map.iter_all() {
        for value in values {
            if !query.is_empty() {
                query.push('&');
            }
            query.push_str(&urlencode(key));
            query.push('=');
            query.push_str(&urlencode(value));
        }
    }
    query
}

pub fn get_canonical_query_string(map: &Multimap) -> String {
    let mut keys: Vec<String> = Vec::new();
    for (key, _) in map.iter() {
        keys.push(key.to_string());
    }
    keys.sort();

    let mut query = String::new();
    for key in keys {
        match map.get_vec(key.as_str()) {
            Some(values) => {
                for value in values {
                    if !query.is_empty() {
                        query.push('&');
                    }
                    query.push_str(&urlencode(key.as_str()));
                    query.push('=');
                    query.push_str(&urlencode(value));
                }
            }
            None => todo!(), // This never happens.
        };
    }

    query
}

pub fn get_canonical_headers(map: &Multimap) -> (String, String) {
    lazy_static! {
        static ref MULTI_SPACE_REGEX: Regex = Regex::new("( +)").unwrap();
    }
    let mut btmap: BTreeMap<String, String> = BTreeMap::new();

    for (k, values) in map.iter_all() {
        let key = k.to_lowercase();
        if "authorization" == key || "user-agent" == key {
            continue;
        }

        let mut vs = values.clone();
        vs.sort();

        let mut value = String::new();
        for v in vs {
            if !value.is_empty() {
                value.push(',');
            }
            let s: String = MULTI_SPACE_REGEX.replace_all(&v, " ").to_string();
            value.push_str(&s);
        }
        btmap.insert(key.clone(), value.clone());
    }

    let mut signed_headers = String::new();
    let mut canonical_headers = String::new();
    let mut add_delim = false;
    for (key, value) in &btmap {
        if add_delim {
            signed_headers.push(';');
            canonical_headers.push('\n');
        }

        signed_headers.push_str(key);

        canonical_headers.push_str(key);
        canonical_headers.push(':');
        canonical_headers.push_str(value);

        add_delim = true;
    }

    (signed_headers, canonical_headers)
}

pub fn check_bucket_name(bucket_name: &str, strict: bool) -> Result<(), Error> {
    if bucket_name.trim().is_empty() {
        return Err(Error::InvalidBucketName(String::from(
            "bucket name cannot be empty",
        )));
    }

    if bucket_name.len() < 3 {
        return Err(Error::InvalidBucketName(String::from(
            "bucket name cannot be less than 3 characters",
        )));
    }

    if bucket_name.len() > 63 {
        return Err(Error::InvalidBucketName(String::from(
            "Bucket name cannot be greater than 63 characters",
        )));
    }

    lazy_static! {
        static ref VALID_IP_ADDR_REGEX: Regex = Regex::new("^(\\d+\\.){3}\\d+$").unwrap();
        static ref VALID_BUCKET_NAME_REGEX: Regex =
            Regex::new("^[A-Za-z0-9][A-Za-z0-9\\.\\-_:]{1,61}[A-Za-z0-9]$").unwrap();
        static ref VALID_BUCKET_NAME_STRICT_REGEX: Regex =
            Regex::new("^[a-z0-9][a-z0-9\\.\\-]{1,61}[a-z0-9]$").unwrap();
    }

    if VALID_IP_ADDR_REGEX.is_match(bucket_name) {
        return Err(Error::InvalidBucketName(String::from(
            "bucket name cannot be an IP address",
        )));
    }

    if bucket_name.contains("..") || bucket_name.contains(".-") || bucket_name.contains("-.") {
        return Err(Error::InvalidBucketName(String::from(
            "bucket name contains invalid successive characters '..', '.-' or '-.'",
        )));
    }

    if strict {
        if !VALID_BUCKET_NAME_STRICT_REGEX.is_match(bucket_name) {
            return Err(Error::InvalidBucketName(String::from(
                "bucket name does not follow S3 standards strictly",
            )));
        }
    } else if !VALID_BUCKET_NAME_REGEX.is_match(bucket_name) {
        return Err(Error::InvalidBucketName(String::from(
            "bucket name does not follow S3 standards",
        )));
    }

    Ok(())
}

pub fn get_text(element: &Element, tag: &str) -> Result<String, Error> {
    Ok(element
        .get_child(tag)
        .ok_or(Error::XmlError(format!("<{}> tag not found", tag)))?
        .get_text()
        .ok_or(Error::XmlError(format!("text of <{}> tag not found", tag)))?
        .to_string())
}

pub fn get_option_text(element: &Element, tag: &str) -> Option<String> {
    if let Some(v) = element.get_child(tag) {
        return Some(v.get_text().unwrap_or_default().to_string());
    }

    None
}

pub fn get_default_text(element: &Element, tag: &str) -> String {
    element.get_child(tag).map_or(String::new(), |v| {
        v.get_text().unwrap_or_default().to_string()
    })
}

pub fn copy_slice(dst: &mut [u8], src: &[u8]) -> usize {
    let mut c = 0;
    for (d, s) in dst.iter_mut().zip(src.iter()) {
        *d = *s;
        c += 1;
    }
    c
}
