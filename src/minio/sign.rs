use hyper::header::{
    HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, USER_AGENT,
};
use ring::{digest, hmac};
use std::collections::{HashMap, HashSet};
use time::Tm;

use crate::minio;

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

// Returns list of SORTED headers that will be signed. TODO: verify
// that input headermap contains only ASCII valued headers
fn get_headers_to_sign(h: &HeaderMap) -> Vec<(String, String)> {
    let mut ignored_hdrs: HashSet<HeaderName> = HashSet::new();
    ignored_hdrs.insert(AUTHORIZATION);
    ignored_hdrs.insert(CONTENT_LENGTH);
    ignored_hdrs.insert(CONTENT_TYPE);
    ignored_hdrs.insert(USER_AGENT);
    let mut res: Vec<(String, String)> = h
        .iter()
        .map(|(x, y)| (x.clone(), y.clone()))
        .filter(|(x, _)| !ignored_hdrs.contains(x))
        .map(|(x, y)| {
            (
                x.as_str().to_string(),
                y.to_str()
                    .expect("Unexpected non-ASCII header value!")
                    .to_string(),
            )
        })
        .collect();
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

fn get_canonical_querystr(q: &HashMap<String, Option<String>>) -> String {
    let mut hs: Vec<(String, Option<String>)> = q.clone().drain().collect();
    hs.sort();
    let vs: Vec<String> = hs
        .drain(..)
        .map(|(x, y)| {
            let val_str = match y {
                Some(s) => uri_encode_str(&s, true),
                None => "".to_string(),
            };
            uri_encode_str(&x, true) + "=" + &val_str
        })
        .collect();
    vs[..].join("&")
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

pub fn sign_v4(r: &minio::S3Req, c: &minio::Client) -> Vec<(HeaderName, HeaderValue)> {
    c.credentials.clone().map_or(Vec::new(), |creds| {
        let scope = mk_scope(&r.ts, &c.region);
        let date_hdr = (
            HeaderName::from_static("x-amz-date"),
            HeaderValue::from_str(&aws_format_time(&r.ts)).unwrap(),
        );
        let mut hmap = r.headers.clone();
        hmap.insert(date_hdr.0.clone(), date_hdr.1.clone());

        let hs = get_headers_to_sign(&hmap);
        let signed_hdrs_str: String = hs
            .iter()
            .map(|(x, _)| x.clone())
            .collect::<Vec<String>>()
            .join(";");
        let cr = get_canonical_request(r, &hs, &signed_hdrs_str);
        println!("canonicalreq: {}", cr);
        let s2s = string_to_sign(&r.ts, &scope, &cr);
        println!("s2s: {}", s2s);
        let skey = get_signing_key(&r.ts, &c.region.to_string(), &creds.secret_key);
        println!("skey: {:?}", skey);
        let signature = compute_sign(&s2s, &skey);
        println!("sign: {}", signature);

        let auth_hdr_val = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            &creds.access_key, &scope, &signed_hdrs_str, &signature,
        );
        let auth_hdr = (AUTHORIZATION, HeaderValue::from_str(&auth_hdr_val).unwrap());
        vec![auth_hdr, date_hdr]
    })
}
