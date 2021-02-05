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

use std::collections::{HashMap, HashSet};

use hyper::header::{
    AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT,
};
use log::debug;
use ring::{digest, hmac};
use time::Tm;

use crate::minio;
use crate::minio::types::Region;

fn aws_format_time(t: &Tm) -> String {
    t.strftime("%Y%m%dT%H%M%SZ").unwrap().to_string()
}

fn aws_format_date(t: &Tm) -> String {
    t.strftime("%Y%m%d").unwrap().to_string()
}

fn mk_scope(t: &Tm, r: &minio::Region) -> String {
    let scope_time = t.strftime("%Y%m%d").unwrap().to_string();
    format!("{}/{}/s3/aws4_request", scope_time, r.to_string())
}

// Returns list of SORTED headers that will be signed.
// TODO: verify that input headermap contains only ASCII valued headers
fn get_headers_to_sign(h: HeaderMap) -> Vec<(String, String)> {
    let ignored_hdrs: HashSet<HeaderName> = vec![
        AUTHORIZATION,
        CONTENT_LENGTH,
        CONTENT_TYPE,
        USER_AGENT].into_iter().collect();

    let mut res: Vec<(String, String)> = h
        .iter()
        .filter(|(x, _)| !ignored_hdrs.contains(*x))
        .map(|(x, y)| {
            (
                x.as_str().to_string(),
                y.to_str()
                    .expect("Unexpected non-ASCII header value!")
                    .to_string(),
            )
        }).collect();
    res.sort();
    res
}

fn uri_encode(c: char, encode_slash: bool) -> String {
    if c == '/' {
        if encode_slash {
            "%2F".to_string()
        } else {
            "/".to_string()
        }
    } else if c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' || c == '~' {
        c.to_string()
    } else {
        let mut b = [0; 8];
        let cs = c.encode_utf8(&mut b).as_bytes();
        cs.iter().map(|x| format!("%{:02X}", x)).collect()
    }
}

fn uri_encode_str(s: &str, encode_slash: bool) -> String {
    s.chars().map(|x| uri_encode(x, encode_slash)).collect()
}

fn get_canonical_querystr(q: &HashMap<String, Vec<Option<String>>>) -> String {
    let mut hs: Vec<(String, Vec<Option<String>>)> = q.clone().drain().collect();
    // sort keys
    hs.sort();
    // Build canonical query string
    hs.iter()
        .map(|(key, values)| {
            values.iter().map(move |value| match value {
                Some(v) => format!("{}={}", &key, uri_encode_str(&v, true)),
                None => format!("{}=", &key),
            })
        })
        .flatten()
        .collect::<Vec<String>>()
        .join("&")
}

fn get_canonical_request(
    r: &minio::S3Req,
    hdrs_to_use: &Vec<(String, String)>,
    signed_hdrs_str: &str,
) -> String {
    let path_str = r.mk_path();
    let canonical_qstr = get_canonical_querystr(&r.query);
    let canonical_hdrs: String = hdrs_to_use
        .iter()
        .map(|(x, y)| format!("{}:{}\n", x.clone(), y.clone()))
        .collect();

    // FIXME: using only unsigned payload for now - need to add
    // hashing of payload.
    let payload_hash_str = String::from("UNSIGNED-PAYLOAD");
    let res = vec![
        r.method.to_string(),
        uri_encode_str(&path_str, false),
        canonical_qstr,
        canonical_hdrs,
        signed_hdrs_str.to_string(),
        payload_hash_str,
    ];
    res.join("\n")
}

fn string_to_sign(ts: &Tm, scope: &str, canonical_request: &str) -> String {
    let sha256_digest: String = digest::digest(&digest::SHA256, canonical_request.as_bytes())
        .as_ref()
        .iter()
        .map(|x| format!("{:02x}", x))
        .collect();
    vec![
        "AWS4-HMAC-SHA256",
        &aws_format_time(&ts),
        scope,
        &sha256_digest,
    ]
        .join("\n")
}

fn hmac_sha256(msg: &str, key: &[u8]) -> hmac::Signature {
    let key = hmac::SigningKey::new(&digest::SHA256, key);
    hmac::sign(&key, msg.as_bytes())
}

fn get_signing_key(ts: &Tm, region: &str, secret_key: &str) -> Vec<u8> {
    let kstr = format!("AWS4{}", secret_key);
    let s1 = hmac_sha256(&aws_format_date(&ts), kstr.as_bytes());
    let s2 = hmac_sha256(&region, s1.as_ref());
    let s3 = hmac_sha256("s3", s2.as_ref());
    let s4 = hmac_sha256("aws4_request", s3.as_ref());
    // FIXME: can this be done better?
    s4.as_ref().iter().map(|x| x.clone()).collect()
}

fn compute_sign(str_to_sign: &str, key: &Vec<u8>) -> String {
    let s1 = hmac_sha256(&str_to_sign, key.as_slice());
    s1.as_ref().iter().map(|x| format!("{:02x}", x)).collect()
}

pub fn sign_v4(
    request: &minio::S3Req,
    credentials: Option<minio::Credentials>,
    region: Region,
) -> Vec<(HeaderName, HeaderValue)> {
    credentials.map_or(Vec::new(), |creds| {
        let scope = mk_scope(&request.ts, &region);
        let date_hdr = (
            HeaderName::from_static("x-amz-date"),
            HeaderValue::from_str(&aws_format_time(&request.ts)).unwrap(),
        );
        let mut hmap = request.headers.clone();
        hmap.insert(date_hdr.0.clone(), date_hdr.1.clone());

        let headers = get_headers_to_sign(hmap);
        let signed_hdrs_str: String = headers
            .iter()
            .map(|(x, _)| x.clone())
            .collect::<Vec<String>>()
            .join(";");
        let cr = get_canonical_request(request, &headers, &signed_hdrs_str);
        debug!("canonicalreq: {}", cr);
        let s2s = string_to_sign(&request.ts, &scope, &cr);
        debug!("s2s: {}", s2s);
        let skey = get_signing_key(&request.ts, &region.to_string(), &creds.secret_key);
        debug!("skey: {:?}", skey);
        let signature = compute_sign(&s2s, &skey);
        debug!("sign: {}", signature);

        let auth_hdr_val = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            &creds.access_key, &scope, &signed_hdrs_str, &signature,
        );
        let auth_hdr = (AUTHORIZATION, HeaderValue::from_str(&auth_hdr_val).unwrap());
        vec![auth_hdr, date_hdr]
    })
}

#[cfg(test)]
mod sign_tests {
    use super::*;

    #[test]
    fn canonical_ordered() {
        let mut query_params: HashMap<String, Vec<Option<String>>> = HashMap::new();

        query_params.insert("key2".to_string(), vec![Some("val3".to_string()), None]);

        query_params.insert(
            "key1".to_string(),
            vec![Some("val1".to_string()), Some("val2".to_string())],
        );

        assert_eq!(
            get_canonical_querystr(&query_params),
            "key1=val1&key1=val2&key2=val3&key2="
        );
    }

    #[test]
    fn headers_to_sign_remove_ignored_and_sort() {
        let mut map = HeaderMap::new();
        map.insert(AUTHORIZATION, "hello".parse().unwrap());
        map.insert(CONTENT_LENGTH, "123".parse().unwrap());
        map.insert("second", "123".parse().unwrap());
        map.insert("first", "123".parse().unwrap());

        assert_eq!(
            get_headers_to_sign(map),
            vec![("first".parse().unwrap(), "123".parse().unwrap()),
                 ("second".parse().unwrap(), "123".parse().unwrap())]
        );
    }
}
