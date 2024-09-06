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

//! Signature V4 for S3 API

use crate::s3::utils::{
    get_canonical_headers, get_canonical_query_string, sha256_hash, to_amz_date, to_signer_date,
    Multimap, UtcTime,
};
use hex::encode as hexencode;
use hmac::{Hmac, Mac};
use hyper::http::Method;
use sha2::Sha256;

/// Returns HMAC hash for given key and data
pub fn hmac_hash(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut hasher = Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
    hasher.update(data);
    hasher.finalize().into_bytes().to_vec()
}

/// Returns hex encoded HMAC hash for given key and data
pub fn hmac_hash_hex(key: &[u8], data: &[u8]) -> String {
    hexencode(hmac_hash(key, data))
}

/// Returns scope value of given date, region and service name
pub fn get_scope(date: UtcTime, region: &str, service_name: &str) -> String {
    format!(
        "{}/{}/{}/aws4_request",
        to_signer_date(date),
        region,
        service_name
    )
}

/// Returns hex encoded SHA256 hash of canonical request
pub fn get_canonical_request_hash(
    method: &Method,
    uri: &str,
    query_string: &str,
    headers: &str,
    signed_headers: &str,
    content_sha256: &str,
) -> String {
    // CanonicalRequest =
    //   HTTPRequestMethod + '\n' +
    //   CanonicalURI + '\n' +
    //   CanonicalQueryString + '\n' +
    //   CanonicalHeaders + '\n\n' +
    //   SignedHeaders + '\n' +
    //   HexEncode(Hash(RequestPayload))
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n\n{}\n{}",
        method, uri, query_string, headers, signed_headers, content_sha256
    );
    sha256_hash(canonical_request.as_bytes())
}

/// Returns string-to-sign value of given date, scope and canonical request hash
pub fn get_string_to_sign(date: UtcTime, scope: &str, canonical_request_hash: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        to_amz_date(date),
        scope,
        canonical_request_hash
    )
}

/// Returns signing key of given secret key, date, region and service name
pub fn get_signing_key(
    secret_key: &str,
    date: UtcTime,
    region: &str,
    service_name: &str,
) -> Vec<u8> {
    let mut key: Vec<u8> = b"AWS4".to_vec();
    key.extend(secret_key.as_bytes());

    let date_key = hmac_hash(key.as_slice(), to_signer_date(date).as_bytes());
    let date_region_key = hmac_hash(date_key.as_slice(), region.as_bytes());
    let date_region_service_key = hmac_hash(date_region_key.as_slice(), service_name.as_bytes());
    hmac_hash(date_region_service_key.as_slice(), b"aws4_request")
}

/// Returns signature value for given signing key and string-to-sign
pub fn get_signature(signing_key: &[u8], string_to_sign: &[u8]) -> String {
    hmac_hash_hex(signing_key, string_to_sign)
}

/// Returns authorization value for given access key, scope, signed headers and signature
pub fn get_authorization(
    access_key: &str,
    scope: &str,
    signed_headers: &str,
    signature: &str,
) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        access_key, scope, signed_headers, signature
    )
}

/// Signs and updates headers for given parameters
pub fn sign_v4(
    service_name: &str,
    method: &Method,
    uri: &str,
    region: &str,
    headers: &mut Multimap,
    query_params: &Multimap,
    access_key: &str,
    secret_key: &str,
    content_sha256: &str,
    date: UtcTime,
) {
    let scope = get_scope(date, region, service_name);
    let (signed_headers, canonical_headers) = get_canonical_headers(headers);
    let canonical_query_string = get_canonical_query_string(query_params);
    let canonical_request_hash = get_canonical_request_hash(
        method,
        uri,
        &canonical_query_string,
        &canonical_headers,
        &signed_headers,
        content_sha256,
    );
    let string_to_sign = get_string_to_sign(date, &scope, &canonical_request_hash);
    let signing_key = get_signing_key(secret_key, date, region, service_name);
    let signature = get_signature(signing_key.as_slice(), string_to_sign.as_bytes());
    let authorization = get_authorization(access_key, &scope, &signed_headers, &signature);

    headers.insert("Authorization".to_string(), authorization);
}

/// Signs and updates headers for given parameters for S3 request
pub fn sign_v4_s3(
    method: &Method,
    uri: &str,
    region: &str,
    headers: &mut Multimap,
    query_params: &Multimap,
    access_key: &str,
    secret_key: &str,
    content_sha256: &str,
    date: UtcTime,
) {
    sign_v4(
        "s3",
        method,
        uri,
        region,
        headers,
        query_params,
        access_key,
        secret_key,
        content_sha256,
        date,
    )
}

/// Signs and updates headers for given parameters for STS request
pub fn sign_v4_sts(
    method: &Method,
    uri: &str,
    region: &str,
    headers: &mut Multimap,
    query_params: &Multimap,
    access_key: &str,
    secret_key: &str,
    content_sha256: &str,
    date: UtcTime,
) {
    sign_v4(
        "sts",
        method,
        uri,
        region,
        headers,
        query_params,
        access_key,
        secret_key,
        content_sha256,
        date,
    )
}

/// Signs and updates headers for given parameters for pre-sign request
pub fn presign_v4(
    method: &Method,
    host: &str,
    uri: &str,
    region: &str,
    query_params: &mut Multimap,
    access_key: &str,
    secret_key: &str,
    date: UtcTime,
    expires: u32,
) {
    let scope = get_scope(date, region, "s3");
    let canonical_headers = "host:".to_string() + host;
    let signed_headers = "host";

    query_params.insert(
        "X-Amz-Algorithm".to_string(),
        "AWS4-HMAC-SHA256".to_string(),
    );
    query_params.insert(
        "X-Amz-Credential".to_string(),
        access_key.to_string() + "/" + &scope,
    );
    query_params.insert("X-Amz-Date".to_string(), to_amz_date(date));
    query_params.insert("X-Amz-Expires".to_string(), expires.to_string());
    query_params.insert(
        "X-Amz-SignedHeaders".to_string(),
        signed_headers.to_string(),
    );

    let canonical_query_string = get_canonical_query_string(query_params);
    let canonical_request_hash = get_canonical_request_hash(
        method,
        uri,
        &canonical_query_string,
        &canonical_headers,
        signed_headers,
        "UNSIGNED-PAYLOAD",
    );
    let string_to_sign = get_string_to_sign(date, &scope, &canonical_request_hash);
    let signing_key = get_signing_key(secret_key, date, region, "s3");
    let signature = get_signature(signing_key.as_slice(), string_to_sign.as_bytes());

    query_params.insert("X-Amz-Signature".to_string(), signature);
}

/// Signs and updates headers for given parameters for pre-sign POST request
pub fn post_presign_v4(
    string_to_sign: &str,
    secret_key: &str,
    date: UtcTime,
    region: &str,
) -> String {
    let signing_key = get_signing_key(secret_key, date, region, "s3");
    get_signature(signing_key.as_slice(), string_to_sign.as_bytes())
}
