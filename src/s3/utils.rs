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

use crate::s3::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::sse::{Sse, SseCustomerKey};
use base64::engine::Engine as _;
use chrono::{DateTime, Datelike, NaiveDateTime, Utc};
use crc::{CRC_32_ISO_HDLC, Crc};
use lazy_static::lazy_static;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use regex::Regex;
#[cfg(feature = "ring")]
use ring::digest::{Context, SHA256};
#[cfg(not(feature = "ring"))]
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use xmltree::Element;

/// Date and time with UTC timezone
pub type UtcTime = DateTime<Utc>;

// Great stuff to get confused about.
// String "a b+c" in Percent-Encoding (RFC 3986) becomes "a%20b%2Bc".
// S3 sometimes returns Form-Encoding (application/x-www-form-urlencoded) rendering string "a%20b%2Bc" into "a+b%2Bc"
// If you were to do Percent-Decoding on "a+b%2Bc" you would get "a+b+c", which is wrong.
// If you use Form-Decoding on "a+b%2Bc" you would get "a b+c", which is correct.

/// Decodes a URL-encoded string in the application/x-www-form-urlencoded syntax into a string.
/// Note that "+" is decoded to a space character, and "%2B" is decoded to a plus sign.
pub fn url_decode(s: &str) -> String {
    url::form_urlencoded::parse(s.as_bytes())
        .map(|(k, _)| k)
        .collect()
}

/// Encodes a string using URL encoding. Note that a whitespace is encoded as "%20" and plus
/// sign is encoded as "%2B".
pub fn url_encode(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

/// Encodes data using base64 algorithm
pub fn b64_encode(input: impl AsRef<[u8]>) -> String {
    base64::engine::general_purpose::STANDARD.encode(input)
}

/// Computes CRC32 of given data.
pub fn crc32(data: &[u8]) -> u32 {
    //TODO creating a new Crc object is expensive, we should cache it
    Crc::<u32>::new(&CRC_32_ISO_HDLC).checksum(data)
}

/// Converts data array into 32 bit BigEndian unsigned int
pub fn uint32(data: &[u8]) -> Result<u32, ValidationErr> {
    if data.len() < 4 {
        return Err(ValidationErr::InvalidIntegerValue {
            message: "data is not a valid 32-bit BigEndian unsigned integer".into(),
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "not enough bytes",
            )),
        });
    }
    Ok(u32::from_be_bytes(data[..4].try_into().unwrap()))
}

/// sha256 hash of empty data
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Gets hex encoded SHA256 hash of given data
pub fn sha256_hash(data: &[u8]) -> String {
    #[cfg(feature = "ring")]
    {
        hex_encode(ring::digest::digest(&SHA256, data).as_ref())
    }
    #[cfg(not(feature = "ring"))]
    {
        hex_encode(Sha256::new_with_prefix(data).finalize().as_slice())
    }
}

/// Hex-encode a byte slice into a lowercase ASCII string.
///
/// # Safety
/// This implementation uses `unsafe` code for performance reasons:
/// - We call [`String::as_mut_vec`] to get direct access to the
///   underlying `Vec<u8>` backing the `String`.
/// - We then use [`set_len`] to pre-allocate the final length without
///   initializing the contents first.
/// - Finally, we use [`get_unchecked`] and [`get_unchecked_mut`] to
///   avoid bounds checking inside the tight encoding loop.
///
/// # Why unsafe is needed
/// Normally, writing this function with safe Rust requires:
/// - Pushing each hex digit one-by-one into the string (extra bounds checks).
/// - Or allocating and copying temporary buffers.
///
/// Using `unsafe` avoids redundant checks and makes this implementation
///   significantly faster, especially for large inputs.
///
/// # Why this is correct
/// - `s` is allocated with exactly `len * 2` capacity, and we immediately
///   set its length to that value. Every byte in the string buffer will be
///   initialized before being read or used.
/// - The loop index `i` is always in `0..len`, so `bytes.get_unchecked(i)`
///   is safe.
/// - Each write goes to positions `j` and `j + 1`, where `j = i * 2`.
///   Since `i < len`, the maximum write index is `2*len - 1`, which is
///   within the allocated range.
/// - All written bytes come from the `LUT` table, which has exactly 16
///   elements, and indices are masked into the 0–15 range.
///
/// Therefore, although `unsafe` is used to skip bounds checking,
/// the logic ensures all memory accesses remain in-bounds and initialized.
pub fn hex_encode(bytes: &[u8]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let len = bytes.len();
    let mut s = String::with_capacity(len * 2);

    unsafe {
        let v = s.as_mut_vec();
        v.set_len(len * 2);
        for i in 0..len {
            let b = bytes.get_unchecked(i);
            let hi = LUT.get_unchecked((b >> 4) as usize);
            let lo = LUT.get_unchecked((b & 0xF) as usize);
            let j = i * 2;
            *v.get_unchecked_mut(j) = *hi;
            *v.get_unchecked_mut(j + 1) = *lo;
        }
    }

    s
}

pub fn sha256_hash_sb(sb: Arc<SegmentedBytes>) -> String {
    #[cfg(feature = "ring")]
    {
        let mut context = Context::new(&SHA256);
        for data in sb.iter() {
            context.update(data.as_ref());
        }
        hex_encode(context.finish().as_ref())
    }
    #[cfg(not(feature = "ring"))]
    {
        let mut hasher = Sha256::new();
        for data in sb.iter() {
            hasher.update(data);
        }
        hex_encode(hasher.finalize().as_slice())
    }
}

#[cfg(test)]
mod tests {
    use crate::s3::utils::SegmentedBytes;
    use crate::s3::utils::sha256_hash_sb;
    use std::sync::Arc;

    #[test]
    fn test_empty_sha256_segmented_bytes() {
        assert_eq!(
            super::EMPTY_SHA256,
            sha256_hash_sb(Arc::new(SegmentedBytes::new()))
        );
    }
}

/// Gets bas64 encoded MD5 hash of given data
pub fn md5sum_hash(data: &[u8]) -> String {
    b64_encode(md5::compute(data).as_slice())
}

/// Gets current UTC time
pub fn utc_now() -> UtcTime {
    chrono::offset::Utc::now()
}

/// Gets signer date value of given time
pub fn to_signer_date(time: UtcTime) -> String {
    time.format("%Y%m%d").to_string()
}

/// Gets AMZ date value of given time
pub fn to_amz_date(time: UtcTime) -> String {
    time.format("%Y%m%dT%H%M%SZ").to_string()
}

/// Gets HTTP header value of given time
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

/// Gets ISO8601 UTC formatted value of given time
pub fn to_iso8601utc(time: UtcTime) -> String {
    time.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string()
}

/// Parses ISO8601 UTC formatted value to time
pub fn from_iso8601utc(s: &str) -> Result<UtcTime, ValidationErr> {
    let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S.%3fZ")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ"))?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
}

const OBJECT_KEY_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'/');

pub fn urlencode_object_key(key: &str) -> String {
    utf8_percent_encode(key, OBJECT_KEY_ENCODE_SET).collect()
}

pub mod aws_date_format {
    use super::{UtcTime, from_iso8601utc, to_iso8601utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &UtcTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&to_iso8601utc(*date))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<UtcTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        from_iso8601utc(&s).map_err(serde::de::Error::custom)
    }
}

pub fn parse_bool(value: &str) -> Result<bool, ValidationErr> {
    if value.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(ValidationErr::InvalidBooleanValue(value.to_string()))
    }
}

/// Parses HTTP header value to time
pub fn from_http_header_value(s: &str) -> Result<UtcTime, ValidationErr> {
    let dt = NaiveDateTime::parse_from_str(s, "%a, %d %b %Y %H:%M:%S GMT")?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
}

/// Checks if given hostname is valid or not
pub fn match_hostname(value: &str) -> bool {
    lazy_static! {
        static ref HOSTNAME_REGEX: Regex =
            Regex::new(r"^([a-z_\d-]{1,63}\.)*([a-z_\d-]{1,63})$").unwrap();
    }

    if !HOSTNAME_REGEX.is_match(value.to_lowercase().as_str()) {
        return false;
    }

    for token in value.split('.') {
        if token.starts_with('-')
            || token.starts_with('_')
            || token.ends_with('-')
            || token.ends_with('_')
        {
            return false;
        }
    }

    true
}

/// Checks if given region is valid or not
pub fn match_region(value: &str) -> bool {
    lazy_static! {
        static ref REGION_REGEX: Regex = Regex::new(r"^([a-z_\d-]{1,63})$").unwrap();
    }

    !REGION_REGEX.is_match(value.to_lowercase().as_str())
        || value.starts_with('-')
        || value.starts_with('_')
        || value.ends_with('-')
        || value.ends_with('_')
}

/// Validates given bucket name. TODO S3Express has slightly different rules for bucket names
pub fn check_bucket_name(bucket_name: impl AsRef<str>, strict: bool) -> Result<(), ValidationErr> {
    let bucket_name: &str = bucket_name.as_ref().trim();
    let bucket_name_len = bucket_name.len();
    if bucket_name_len == 0 {
        return Err(ValidationErr::InvalidBucketName {
            name: "".into(),
            reason: "bucket name cannot be empty".into(),
        });
    }
    if bucket_name_len < 3 {
        return Err(ValidationErr::InvalidBucketName {
            name: bucket_name.into(),
            reason: "bucket name  cannot be less than 3 characters".into(),
        });
    }
    if bucket_name_len > 63 {
        return Err(ValidationErr::InvalidBucketName {
            name: bucket_name.into(),
            reason: "bucket name cannot be greater than 63 characters".into(),
        });
    }

    lazy_static! {
    static ref IPV4_REGEX: Regex = Regex::new(r"^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])$").unwrap();
        static ref VALID_BUCKET_NAME_REGEX: Regex =
            Regex::new("^[A-Za-z0-9][A-Za-z0-9\\.\\-_:]{1,61}[A-Za-z0-9]$").unwrap();
        static ref VALID_BUCKET_NAME_STRICT_REGEX: Regex =
            Regex::new("^[a-z0-9][a-z0-9\\.\\-]{1,61}[a-z0-9]$").unwrap();
    }

    if IPV4_REGEX.is_match(bucket_name) {
        return Err(ValidationErr::InvalidBucketName {
            name: bucket_name.into(),
            reason: "bucket name cannot be an IP address".into(),
        });
    }

    if bucket_name.contains("..") || bucket_name.contains(".-") || bucket_name.contains("-.") {
        return Err(ValidationErr::InvalidBucketName {
            name: bucket_name.into(),
            reason: "bucket name contains invalid successive characters '..', '.-' or '-.'".into(),
        });
    }

    if strict {
        if !VALID_BUCKET_NAME_STRICT_REGEX.is_match(bucket_name) {
            return Err(ValidationErr::InvalidBucketName {
                name: bucket_name.into(),
                reason: format!(
                    "bucket name does not follow S3 standards strictly, according to {}",
                    *VALID_BUCKET_NAME_STRICT_REGEX
                ),
            });
        }
    } else if !VALID_BUCKET_NAME_REGEX.is_match(bucket_name) {
        return Err(ValidationErr::InvalidBucketName {
            name: bucket_name.into(),
            reason: format!(
                "bucket name does not follow S3 standards, according to {}",
                *VALID_BUCKET_NAME_REGEX
            ),
        });
    }

    Ok(())
}

/// Validates given object name. TODO S3Express has slightly different rules for object names
pub fn check_object_name(object_name: impl AsRef<str>) -> Result<(), ValidationErr> {
    let name = object_name.as_ref();
    match name.len() {
        0 => Err(ValidationErr::InvalidObjectName(
            "object name cannot be empty".into(),
        )),
        n if n > 1024 => Err(ValidationErr::InvalidObjectName(format!(
            "Object name ('{name}') cannot be greater than 1024 bytes"
        ))),
        _ => Ok(()),
    }
}

/// Validates SSE (Server-Side Encryption) settings.
pub fn check_sse(sse: &Option<Arc<dyn Sse>>, client: &MinioClient) -> Result<(), ValidationErr> {
    if let Some(v) = &sse
        && v.tls_required()
        && !client.is_secure()
    {
        return Err(ValidationErr::SseTlsRequired(None));
    }
    Ok(())
}

/// Validates SSE-C (Server-Side Encryption with Customer-Provided Keys) settings.
pub fn check_ssec(
    ssec: &Option<SseCustomerKey>,
    client: &MinioClient,
) -> Result<(), ValidationErr> {
    if ssec.is_some() && !client.is_secure() {
        return Err(ValidationErr::SseTlsRequired(None));
    }
    Ok(())
}

/// Validates SSE-C (Server-Side Encryption with Customer-Provided Keys) settings and logs an error
pub fn check_ssec_with_log(
    ssec: &Option<SseCustomerKey>,
    client: &MinioClient,
    bucket: &str,
    object: &str,
    version: &Option<String>,
) -> Result<(), ValidationErr> {
    if ssec.is_some() && !client.is_secure() {
        return Err(ValidationErr::SseTlsRequired(Some(format!(
            "source {bucket}/{object}{}: ",
            version
                .as_ref()
                .map_or(String::new(), |v| String::from("?versionId=") + v)
        ))));
    }
    Ok(())
}

/// Gets default text value of given XML element for given tag.
pub fn get_text_default(element: &Element, tag: &str) -> String {
    element.get_child(tag).map_or(String::new(), |v| {
        v.get_text().unwrap_or_default().to_string()
    })
}

/// Gets text value of given XML element for given tag.
pub fn get_text_result(element: &Element, tag: &str) -> Result<String, ValidationErr> {
    Ok(element
        .get_child(tag)
        .ok_or(ValidationErr::xml_error(format!("<{tag}> tag not found")))?
        .get_text()
        .ok_or(ValidationErr::xml_error(format!(
            "text of <{tag}> tag not found"
        )))?
        .to_string())
}

/// Gets optional text value of given XML element for given tag.
pub fn get_text_option(element: &Element, tag: &str) -> Option<String> {
    element
        .get_child(tag)
        .and_then(|v| v.get_text().map(|s| s.to_string()))
}

/// Trim leading and trailing quotes from a string. It consumes the
pub fn trim_quotes(mut s: String) -> String {
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        s.drain(0..1); // remove the leading quote
        s.pop(); // remove the trailing quote
    }
    s
}

/// Copies source byte slice into destination byte slice
pub fn copy_slice(dst: &mut [u8], src: &[u8]) -> usize {
    let mut c = 0;
    for (d, s) in dst.iter_mut().zip(src.iter()) {
        *d = *s;
        c += 1;
    }
    c
}

// Characters to escape in query strings. Based on RFC 3986 and the golang
// net/url implementation used in the MinIO server.
//
// https://tools.ietf.org/html/rfc3986
//
// 1. All non-ascii characters are escaped always.
// 2. All reserved characters are escaped.
// 3. Any other characters are not escaped.
//
// Unreserved characters in addition to alphanumeric characters are: '-', '_',
// '.', '~' (§2.3 Unreserved characters (mark))
//
// Reserved characters for query strings: '$', '&', '+', ',', '/', ':', ';',
// '=', '?', '@' (§3.4)
//
// NON_ALPHANUMERIC already escapes everything non-alphanumeric (it includes all
// the reserved characters). So we only remove the unreserved characters from
// this set.
const QUERY_ESCAPE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

fn unescape(s: &str) -> Result<String, ValidationErr> {
    percent_decode_str(s)
        .decode_utf8()
        .map_err(|e| ValidationErr::TagDecodingError {
            input: s.to_string(),
            error_message: e.to_string(),
        })
        .map(|s| s.to_string())
}

fn escape(s: &str) -> String {
    utf8_percent_encode(s, QUERY_ESCAPE).collect()
}

// TODO: use this while adding API to set tags.
//
// Handles escaping same as MinIO server - needed for ensuring compatibility.
pub fn encode_tags(h: &HashMap<String, String>) -> String {
    let mut tags = Vec::with_capacity(h.len());
    for (k, v) in h {
        tags.push(format!("{}={}", escape(k), escape(v)));
    }
    tags.join("&")
}

pub fn parse_tags(s: &str) -> Result<HashMap<String, String>, ValidationErr> {
    let mut tags = HashMap::new();
    for tag in s.split('&') {
        let mut kv = tag.split('=');
        let k = match kv.next() {
            Some(v) => unescape(v)?,
            None => {
                return Err(ValidationErr::TagDecodingError {
                    input: s.into(),
                    error_message: "tag key was empty".into(),
                });
            }
        };
        let v = match kv.next() {
            Some(v) => unescape(v)?,
            None => "".to_owned(),
        };
        if kv.next().is_some() {
            return Err(ValidationErr::TagDecodingError {
                input: s.into(),
                error_message: "tag had too many values for a key".into(),
            });
        }
        tags.insert(k, v);
    }
    Ok(tags)
}

#[must_use]
/// Returns the consumed data and inserts a key into it with an empty value.
pub fn insert(data: Option<Multimap>, key: impl Into<String>) -> Multimap {
    let mut result: Multimap = data.unwrap_or_default();
    result.insert(key.into(), String::new());
    result
}

pub mod xml {
    use crate::s3::error::ValidationErr;
    use std::collections::HashMap;

    #[derive(Debug, Clone)]
    struct XmlElementIndex {
        children: HashMap<String, Vec<usize>>,
    }

    impl XmlElementIndex {
        fn get_first(&self, tag: &str) -> Option<usize> {
            let tag: String = tag.to_string();
            let is = self.children.get(&tag)?;
            is.first().copied()
        }

        fn get(&self, tag: &str) -> Option<&Vec<usize>> {
            let tag: String = tag.to_string();
            self.children.get(&tag)
        }
    }

    impl From<&xmltree::Element> for XmlElementIndex {
        fn from(value: &xmltree::Element) -> Self {
            let mut children = HashMap::new();
            for (i, e) in value
                .children
                .iter()
                .enumerate()
                .filter_map(|(i, v)| v.as_element().map(|e| (i, e)))
            {
                children
                    .entry(e.name.clone())
                    .or_insert_with(Vec::new)
                    .push(i);
            }
            Self { children }
        }
    }

    #[derive(Debug, Clone)]
    pub struct Element<'a> {
        inner: &'a xmltree::Element,
        child_element_index: XmlElementIndex,
    }

    impl<'a> From<&'a xmltree::Element> for Element<'a> {
        fn from(value: &'a xmltree::Element) -> Self {
            let element_index = XmlElementIndex::from(value);
            Self {
                inner: value,
                child_element_index: element_index,
            }
        }
    }

    impl Element<'_> {
        pub fn name(&self) -> &str {
            &self.inner.name
        }

        pub fn get_child_text(&self, tag: &str) -> Option<String> {
            let index = self.child_element_index.get_first(tag)?;
            self.inner.children[index]
                .as_element()?
                .get_text()
                .map(|v| v.to_string())
        }

        pub fn get_child_text_or_error(&self, tag: &str) -> Result<String, ValidationErr> {
            let i = self
                .child_element_index
                .get_first(tag)
                .ok_or(ValidationErr::xml_error(format!("<{tag}> tag not found")))?;
            self.inner.children[i]
                .as_element()
                .unwrap()
                .get_text()
                .map(|x| x.to_string())
                .ok_or(ValidationErr::xml_error(format!(
                    "text of <{tag}> tag not found"
                )))
        }

        // Returns all children with given tag along with their index.
        pub fn get_matching_children(&self, tag: &str) -> Vec<(usize, Element<'_>)> {
            self.child_element_index
                .get(tag)
                .unwrap_or(&vec![])
                .iter()
                .map(|i| (*i, self.inner.children[*i].as_element().unwrap().into()))
                .collect()
        }

        pub fn get_child(&self, tag: &str) -> Option<Element<'_>> {
            let index = self.child_element_index.get_first(tag)?;
            Some(self.inner.children[index].as_element()?.into())
        }

        pub fn get_xmltree_children(&self) -> Vec<&xmltree::Element> {
            self.inner
                .children
                .iter()
                .filter_map(|v| v.as_element())
                .collect()
        }
    }

    // Helper type that implements merge sort in the iterator.
    pub struct MergeXmlElements<'a> {
        v1: &'a Vec<(usize, Element<'a>)>,
        v2: &'a Vec<(usize, Element<'a>)>,
        i1: usize,
        i2: usize,
    }

    impl<'a> MergeXmlElements<'a> {
        pub fn new(v1: &'a Vec<(usize, Element<'a>)>, v2: &'a Vec<(usize, Element<'a>)>) -> Self {
            Self {
                v1,
                v2,
                i1: 0,
                i2: 0,
            }
        }
    }

    impl<'a> Iterator for MergeXmlElements<'a> {
        type Item = &'a Element<'a>;

        fn next(&mut self) -> Option<Self::Item> {
            let c1 = self.v1.get(self.i1);
            let c2 = self.v2.get(self.i2);
            match (c1, c2) {
                (Some(val1), Some(val2)) => {
                    if val1.0 < val2.0 {
                        self.i1 += 1;
                        Some(&val1.1)
                    } else {
                        self.i2 += 1;
                        Some(&val2.1)
                    }
                }
                (Some(val1), None) => {
                    self.i1 += 1;
                    Some(&val1.1)
                }
                (None, Some(val2)) => {
                    self.i2 += 1;
                    Some(&val2.1)
                }
                (None, None) => None,
            }
        }
    }
}
