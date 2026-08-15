// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

use std::ffi::CStr;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use http::Method;
use std::sync::LazyLock;

use crate::s3::client::MinioClient;
use crate::s3::header_constants::*;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::signer::sign_v4_s3;
use crate::s3::types::{BucketName, ObjectKey, Region};
use crate::s3::utils::{to_amz_date, utc_now};

use super::transport::RdmaClient;

pub const X_AMZ_RDMA_TOKEN: &str = "x-amz-rdma-token";
pub const X_AMZ_RDMA_REPLY: &str = "x-amz-rdma-reply";
pub const X_AMZ_RDMA_BYTES_TRANSFERRED: &str = "x-amz-rdma-bytes-transferred";

pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

pub const RDMA_REPLY_SUCCESS: i32 = 200;
pub const RDMA_REPLY_NO_CONTENT: i32 = 204;
pub const RDMA_REPLY_PARTIAL_CONTENT: i32 = 206;
pub const RDMA_REPLY_NOT_IMPLEMENTED: i32 = 501;

pub const RDMA_NOT_SUPPORTED: isize = -2;

/// The attempt failed for a reason that does not implicate the rail: a bad
/// argument, a URL that would not build, or a 4xx from the server. The first
/// two never reached the fabric; the third proves it was crossed, since the
/// server had to receive the request to reject it.
///
/// Distinct from -1 so these skip both the retry and the rail-failure report.
/// Charging a rail here takes healthy hardware out of rotation over an
/// application mistake -- a missing bucket drove `healthy_nic_count()` to 0 on
/// a two-rail host -- and the same error would repeat on the next rail anyway.
const RDMA_NO_RAIL_FAULT: isize = -3;

/// The two sentinels must stay distinct and negative: `from_ssize` tells them
/// apart by value, and the retry loops route on them.
const _: () = {
    assert!(RDMA_NO_RAIL_FAULT != RDMA_NOT_SUPPORTED);
    assert!(RDMA_NO_RAIL_FAULT != -1);
    assert!(RDMA_NO_RAIL_FAULT < 0);
};

const RDMA_MAX_ATTEMPTS: u32 = 2;
const RDMA_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RDMA_TIMEOUT: Duration = Duration::from_secs(10);

/// Per-call context for an RDMA PUT/GET, mirroring `s3_rdma_client_ctx_t`.
#[derive(Debug, Clone)]
pub struct S3RdmaClientCtx {
    pub bucket: BucketName,
    pub object: ObjectKey,
    pub region: Region,
    pub upload_id: Option<String>,
    /// 0 for non-multipart, otherwise 1..=10000.
    pub part_number: u32,
    /// Optional CRC64NVME checksum for multipart parts.
    pub checksum_crc64nvme: Option<String>,
    /// ETag populated by callee on success.
    pub etag: String,
}

/// Outcome of an RDMA transfer.
#[derive(Debug, Clone, Copy)]
pub enum RdmaOutcome {
    /// Transfer succeeded; payload `usize` is the bytes the server actually moved.
    Ok(usize),
    /// Server responded `x-amz-rdma-reply: 501` — caller should fall back to HTTP.
    Declined,
    /// Transport-level failure; caller may fall back to HTTP.
    Failed,
}

impl RdmaOutcome {
    fn from_ssize(v: isize, fallback_size: usize) -> Self {
        if v > 0 {
            Self::Ok(v as usize)
        } else if v == RDMA_NOT_SUPPORTED {
            Self::Declined
        } else if v == 0 {
            Self::Ok(fallback_size)
        } else {
            Self::Failed
        }
    }
}

/// Extract the client NIC IP from the 81-char RDMA token, whose trailing 32
/// hex chars are the source NIC's GID. Returns `None` unless that GID is an
/// IPv4-mapped IPv6 address ("...ffffAABBCCDD"), which is what a RoCEv2 GID
/// over IPv4 looks like -- an IB or IPv6 GID names no address to pin to.
pub fn parse_client_nic_from_token(token: &str) -> Option<IpAddr> {
    let bytes = token.as_bytes();
    if bytes.len() < 32 {
        return None;
    }
    let tail = &bytes[bytes.len() - 32..];
    if tail[..20].iter().any(|&b| b != b'0') {
        return None;
    }
    if &tail[20..24] != b"ffff" {
        return None;
    }
    let parse_octet = |hex: &[u8]| -> Option<u8> {
        let s = std::str::from_utf8(hex).ok()?;
        u8::from_str_radix(s, 16).ok()
    };
    let a = parse_octet(&tail[24..26])?;
    let b = parse_octet(&tail[26..28])?;
    let c = parse_octet(&tail[28..30])?;
    let d = parse_octet(&tail[30..32])?;
    Some(IpAddr::V4(std::net::Ipv4Addr::new(a, b, c, d)))
}

/// Map server's `x-amz-rdma-reply` header to a transfer outcome.
/// `> 0`: reply code (200/204/206), treat as success.
/// `0`:   absent or unparseable, treat as `-1` failure (non-RDMA error
///        such as 403/404 with no reply header).
/// `-2`:  reply explicitly says 501, server declined RDMA.
pub fn parse_rdma_reply(reply: &str) -> i32 {
    if reply == "501" {
        return RDMA_NOT_SUPPORTED as i32;
    }
    reply.parse::<i32>().unwrap_or(0)
}

/// Cache of `reqwest::Client`s keyed by local NIC IP, used so a token that
/// embedded a specific HCA's GID sends its HTTP control-plane out the same
/// interface. Without this a multi-NIC host can split TCP and RDMA across
/// NICs, and the server's RDMA_READ has no path back to the token's peer.
static NIC_CLIENT_CACHE: LazyLock<DashMap<IpAddr, Arc<reqwest::Client>>> =
    LazyLock::new(DashMap::new);

/// Default RDMA control-plane client, used when the token names no pinnable
/// address (single-HCA host, IB or IPv6 GID). Kept in a LazyLock so the
/// aggressive connect/total timeouts still apply when NIC pinning is
/// unavailable, and so we don't allocate a fresh client per op.
static DEFAULT_RDMA_CLIENT: LazyLock<Arc<reqwest::Client>> = LazyLock::new(|| {
    let c = reqwest::Client::builder()
        .tcp_nodelay(true)
        .connect_timeout(RDMA_CONNECT_TIMEOUT)
        .timeout(RDMA_TIMEOUT)
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    Arc::new(c)
});

fn http_client_for_nic(nic: IpAddr) -> Arc<reqwest::Client> {
    if let Some(c) = NIC_CLIENT_CACHE.get(&nic) {
        return Arc::clone(c.value());
    }
    let client = reqwest::Client::builder()
        .tcp_nodelay(true)
        .local_address(Some(nic))
        .connect_timeout(RDMA_CONNECT_TIMEOUT)
        .timeout(RDMA_TIMEOUT)
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    let arc = Arc::new(client);
    NIC_CLIENT_CACHE.insert(nic, Arc::clone(&arc));
    arc
}

fn http_client_for_token(token: &CStr) -> Arc<reqwest::Client> {
    // Parse against the ORIGINAL token (the GID suffix lives in its last 32
    // hex chars), not the formatted "token:addr:size" header value.
    match token.to_str().ok().and_then(parse_client_nic_from_token) {
        Some(nic) => http_client_for_nic(nic),
        None => Arc::clone(&DEFAULT_RDMA_CLIENT),
    }
}

/// Mirrors C++ `rdmaPut`: signs and issues the HTTP PUT control plane carrying
/// the RDMA token, then parses the server reply. Uses the
/// [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
/// request form, or [UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html)
/// when `ctx.upload_id` is set.
pub async fn rdma_put(
    client: &MinioClient,
    ctx: &mut S3RdmaClientCtx,
    token: &CStr,
    size: u64,
) -> isize {
    // The token is the RDMA descriptor verbatim; its leading fields already
    // carry the buffer address and transfer size, so it is sent as-is.
    let rdma_token = token.to_str().unwrap_or("");

    let mut query_params = Multimap::default();
    if let Some(upload_id) = &ctx.upload_id {
        query_params.add("uploadId", upload_id.as_str());
        if ctx.part_number == 0 || ctx.part_number > 10000 {
            return RDMA_NO_RAIL_FAULT;
        }
        query_params.add("partNumber", ctx.part_number.to_string());
    }

    let url = match client.shared.base_url.build_url(
        &Method::PUT,
        &ctx.region,
        &query_params,
        Some(&ctx.bucket),
        Some(&ctx.object),
    ) {
        Ok(u) => u,
        Err(_) => return RDMA_NO_RAIL_FAULT,
    };

    let date = utc_now();
    let host = url.host_header_value();

    let mut headers = Multimap::default();
    headers.add(HOST, host);
    headers.add(X_AMZ_DATE, to_amz_date(date));
    headers.add(X_AMZ_CONTENT_SHA256, UNSIGNED_PAYLOAD);
    headers.add(X_AMZ_RDMA_TOKEN, rdma_token);
    headers.add(CONTENT_TYPE, "application/octet-stream");
    headers.add(CONTENT_LENGTH, "0");
    if let Some(cs) = &ctx.checksum_crc64nvme {
        headers.add("x-amz-checksum-crc64nvme", cs.as_str());
    }

    let creds = client.shared.provider.as_ref().map(|p| p.fetch());
    if let Some(c) = &creds
        && let Some(st) = &c.session_token
    {
        headers.add(X_AMZ_SECURITY_TOKEN, st.as_str());
    }

    if let Some(c) = &creds {
        sign_v4_s3(
            &client.shared.signing_key_cache,
            &Method::PUT,
            &url.path,
            &ctx.region,
            &mut headers,
            &query_params,
            &c.access_key,
            &c.secret_key,
            UNSIGNED_PAYLOAD,
            date,
        );
    }

    let http_client = http_client_for_token(token);

    let mut req = http_client.put(url.to_string());
    for (k, vs) in headers.iter_all() {
        for v in vs {
            req = req.header(k, v);
        }
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(_) => return -1,
    };

    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let etag = resp_headers
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim_matches('"')
        .to_owned();

    if status.as_u16() == 200 && !etag.is_empty() {
        ctx.etag = etag;
        return size as isize;
    }

    let reply = resp_headers
        .get(X_AMZ_RDMA_REPLY)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let reply_code = parse_rdma_reply(&reply);
    if reply_code != RDMA_REPLY_SUCCESS && reply_code != RDMA_REPLY_NO_CONTENT {
        if log::log_enabled!(log::Level::Debug) {
            let body = resp.bytes().await.unwrap_or_default();
            log::debug!(
                "rdma_put PUT {} status={} rdma-reply={} body={}",
                ctx.object,
                status,
                reply,
                String::from_utf8_lossy(&body)
            );
        }
        if reply_code == RDMA_NOT_SUPPORTED as i32 {
            return RDMA_NOT_SUPPORTED;
        }
        // `x-amz-rdma-reply` is the RDMA status channel. Absent, the server
        // failed at the S3 layer -- a missing bucket answers 404, a busy node
        // 503 -- having received this request over the rail, which is proof
        // the rail carried it. Retrying gains nothing the fallback to HTTP
        // does not, and charging the rail sidelines working hardware.
        if reply.is_empty() || status.is_client_error() {
            return RDMA_NO_RAIL_FAULT;
        }
        return -1;
    }

    if let Some(cs) = resp_headers
        .get("x-amz-checksum-crc64nvme")
        .and_then(|v| v.to_str().ok())
    {
        ctx.checksum_crc64nvme = Some(cs.to_owned());
    }
    ctx.etag = etag;
    size as isize
}

/// Mirrors C++ `rdmaGet`: signs and issues the HTTP GET control plane carrying
/// the RDMA token, then trusts `x-amz-rdma-bytes-transferred` for the actual
/// transferred byte count (which can be less than requested on ranged GETs).
/// Uses the [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)
/// request form.
pub async fn rdma_get(
    client: &MinioClient,
    ctx: &mut S3RdmaClientCtx,
    token: &CStr,
    size: u64,
) -> isize {
    // The token is the RDMA descriptor verbatim; its leading fields already
    // carry the buffer address and transfer size, so it is sent as-is.
    let rdma_token = token.to_str().unwrap_or("");

    let query_params = Multimap::default();
    let url = match client.shared.base_url.build_url(
        &Method::GET,
        &ctx.region,
        &query_params,
        Some(&ctx.bucket),
        Some(&ctx.object),
    ) {
        Ok(u) => u,
        Err(_) => return RDMA_NO_RAIL_FAULT,
    };

    let date = utc_now();
    let host = url.host_header_value();

    let mut headers = Multimap::default();
    headers.add(HOST, host);
    headers.add(X_AMZ_DATE, to_amz_date(date));
    headers.add(X_AMZ_CONTENT_SHA256, UNSIGNED_PAYLOAD);
    headers.add(X_AMZ_RDMA_TOKEN, rdma_token);

    let creds = client.shared.provider.as_ref().map(|p| p.fetch());
    if let Some(c) = &creds
        && let Some(st) = &c.session_token
    {
        headers.add(X_AMZ_SECURITY_TOKEN, st.as_str());
    }

    if let Some(c) = &creds {
        sign_v4_s3(
            &client.shared.signing_key_cache,
            &Method::GET,
            &url.path,
            &ctx.region,
            &mut headers,
            &query_params,
            &c.access_key,
            &c.secret_key,
            UNSIGNED_PAYLOAD,
            date,
        );
    }

    let http_client = http_client_for_token(token);

    let mut req = http_client.get(url.to_string());
    for (k, vs) in headers.iter_all() {
        for v in vs {
            req = req.header(k, v);
        }
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(_) => return -1,
    };

    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let reply = resp_headers
        .get(X_AMZ_RDMA_REPLY)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let reply_code = parse_rdma_reply(reply);
    if reply_code == RDMA_NOT_SUPPORTED as i32 {
        return RDMA_NOT_SUPPORTED;
    }
    if reply_code != RDMA_REPLY_SUCCESS && reply_code != RDMA_REPLY_PARTIAL_CONTENT {
        // See rdma_put: no RDMA status means the failure was not the rail's.
        if reply.is_empty() || status.is_client_error() {
            return RDMA_NO_RAIL_FAULT;
        }
        return -1;
    }

    if let Some(etag) = resp_headers.get("etag").and_then(|v| v.to_str().ok()) {
        ctx.etag = etag.trim_matches('"').to_owned();
    }

    if let Some(bytes_str) = resp_headers
        .get(X_AMZ_RDMA_BYTES_TRANSFERRED)
        .and_then(|v| v.to_str().ok())
    {
        return match bytes_str.parse::<i64>() {
            Ok(n) if n >= 0 => n as isize,
            _ => -1,
        };
    }

    size as isize
}

/// PUT a registered buffer, retrying a transient RDMA failure once.
///
/// # Safety
/// `buf_ptr` must point to a `size`-byte region that stays valid, and is not
/// concurrently mutated, for the whole call. `rdma` must hold a live
/// registration covering it -- see [`ScopedRegistration`](super::ScopedRegistration).
/// The remote reads that memory directly, so a stale pointer is a use-after-free
/// the caller never observes locally.
pub async unsafe fn rdma_put_with_retry(
    rdma: &RdmaClient,
    client: &MinioClient,
    ctx: &mut S3RdmaClientCtx,
    buf_ptr: *mut libc::c_void,
    size: usize,
) -> RdmaOutcome {
    let mut last: isize = -1;
    for _ in 0..RDMA_MAX_ATTEMPTS {
        let token = match unsafe { rdma.get_rdma_token(buf_ptr, size, 0) } {
            Some(t) => t,
            None => return RdmaOutcome::Failed,
        };
        last = rdma_put(client, ctx, token.as_cstr(), size as u64).await;
        if last > 0 || last == RDMA_NOT_SUPPORTED || last == RDMA_NO_RAIL_FAULT {
            drop(token);
            break;
        }
        // The transfer failed on the wire. Charge it to the rail this token
        // named, so the next request skips that rail rather than
        // round-robinning back onto it. Two results are excluded above: a 501,
        // because the server declining RDMA says nothing about the rail, and a
        // local error, because nothing was ever sent.
        //
        // A server-side failure marks every rail in turn, which is safe: the
        // library clears all marks once no rail is left usable, so a fault
        // that was never the fabric's heals itself.
        rdma.report_token_failure(token.as_cstr());
        drop(token);
    }
    RdmaOutcome::from_ssize(last, size)
}

/// GET into a registered buffer.
///
/// # Safety
/// Same contract as [`rdma_put_with_retry`]: `buf_ptr` must name a live,
/// registered `size`-byte region for the whole call. The remote writes into
/// that memory directly.
pub async unsafe fn rdma_get_with_retry(
    rdma: &RdmaClient,
    client: &MinioClient,
    ctx: &mut S3RdmaClientCtx,
    buf_ptr: *mut libc::c_void,
    size: usize,
) -> RdmaOutcome {
    let mut last: isize = -1;
    for _ in 0..RDMA_MAX_ATTEMPTS {
        let token = match unsafe { rdma.get_rdma_token(buf_ptr, size, 0) } {
            Some(t) => t,
            None => return RdmaOutcome::Failed,
        };
        last = rdma_get(client, ctx, token.as_cstr(), size as u64).await;
        if last > 0 || last == RDMA_NOT_SUPPORTED || last == RDMA_NO_RAIL_FAULT {
            drop(token);
            break;
        }
        // See rdma_put_with_retry: take the failing rail out of rotation.
        rdma.report_token_failure(token.as_cstr());
        drop(token);
    }
    RdmaOutcome::from_ssize(last, size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_reply_known_codes() {
        assert_eq!(parse_rdma_reply("200"), 200);
        assert_eq!(parse_rdma_reply("204"), 204);
        assert_eq!(parse_rdma_reply("206"), 206);
        assert_eq!(parse_rdma_reply("501"), RDMA_NOT_SUPPORTED as i32);
        assert_eq!(parse_rdma_reply(""), 0);
        assert_eq!(parse_rdma_reply("not-a-number"), 0);
    }

    #[test]
    fn parse_nic_extracts_ipv4_suffix() {
        let token = format!("{}{:020}ffff{:02x}{:02x}{:02x}{:02x}", "x", 0, 10, 0, 0, 5);
        let ip = parse_client_nic_from_token(&token).expect("nic");
        assert_eq!(ip, IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 5)));
    }

    #[test]
    fn parse_nic_rejects_non_mapped() {
        let token = format!("xxx{:020}eeee{:08x}", 0, 0);
        assert!(parse_client_nic_from_token(&token).is_none());
    }

    #[test]
    fn parse_nic_rejects_short_token() {
        assert!(parse_client_nic_from_token("short").is_none());
    }

    #[test]
    fn a_local_error_reports_as_a_failed_transfer() {
        assert!(matches!(
            RdmaOutcome::from_ssize(RDMA_NO_RAIL_FAULT, 4096),
            RdmaOutcome::Failed
        ));
        assert!(matches!(
            RdmaOutcome::from_ssize(RDMA_NOT_SUPPORTED, 4096),
            RdmaOutcome::Declined
        ));
        assert!(matches!(
            RdmaOutcome::from_ssize(-1, 4096),
            RdmaOutcome::Failed
        ));
    }
}
