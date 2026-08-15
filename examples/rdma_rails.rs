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

//! Report the RDMA rails this host can carry traffic on, and prove that
//! consecutive tokens spread across them.
//!
//! Exits non-zero if they do not, so it works as a fabric check.
//!
//! Run with: cargo run --features rdma --example rdma_rails
use minio::s3::rdma::{RdmaBuffer, RdmaClient, ScopedRegistration, shared_rdma_client};
use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::error::Error;

const PAGE: usize = 4096;
const BUF_LEN: usize = 1 << 20;
const MINTS: usize = 6;

fn main() -> Result<(), Box<dyn Error>> {
    let Some(client) = shared_rdma_client() else {
        println!(
            "RDMA unavailable: {}",
            minio::s3::rdma::rdma_init_error().unwrap_or("no reason reported")
        );
        return Ok(());
    };
    let healthy = client.healthy_nic_count();
    println!("rails: {} total, {healthy} healthy", client.nic_count());

    let layout = Layout::from_size_align(BUF_LEN, PAGE)?;
    // SAFETY: `layout` has a non-zero size, so `alloc_zeroed` returns either
    // null or a BUF_LEN-byte page-aligned allocation.
    let mem = unsafe { alloc_zeroed(layout) };
    if mem.is_null() {
        return Err("failed to allocate a page-aligned buffer".into());
    }

    let result = mint_across_rails(client, mem.cast(), BUF_LEN, healthy);

    // SAFETY: `mint_across_rails` has returned, so the registration it held is
    // dropped and the NIC no longer has this memory pinned.
    unsafe { dealloc(mem, layout) };
    result
}

/// Mint [`MINTS`] tokens against one registered buffer and require them to
/// name `min(MINTS, healthy)` distinct rails, which is what the round-robin
/// contract promises.
fn mint_across_rails(
    client: &RdmaClient,
    ptr: *mut libc::c_void,
    len: usize,
    healthy: usize,
) -> Result<(), Box<dyn Error>> {
    // SAFETY: `ptr` is a live page-aligned `len`-byte allocation owned by the
    // caller, which outlives this call and so outlives the registration.
    let buf = unsafe { RdmaBuffer::from_raw(ptr, len) };
    // SAFETY: as above. `_reg` deregisters on drop, before the caller frees.
    let _reg = unsafe { ScopedRegistration::register(client, buf.ptr(), buf.len()) }
        .ok_or("failed to register the buffer on every rail")?;

    let mut seen = std::collections::BTreeSet::new();
    for i in 0..MINTS {
        // SAFETY: `_reg` holds the registration for `buf` across this call.
        let token = unsafe { client.get_rdma_token(buf.ptr(), buf.len(), 0) }
            .ok_or("failed to mint a token")?;
        let gid = gid_of(token.as_cstr().to_str()?).ok_or("token carries no GID")?;
        println!("  token {i} gid=...{}", &gid[20..]);
        seen.insert(gid.to_owned());
    }

    let expected = expected_rails(MINTS, healthy);
    println!(
        "distinct rails across {MINTS} mints: {} (expected {expected})",
        seen.len()
    );
    if seen.len() != expected {
        return Err(format!(
            "tokens did not spread round-robin: {} distinct rails, expected {expected}",
            seen.len()
        )
        .into());
    }
    Ok(())
}

/// The GID is the trailing 32 hex chars of the descriptor.
fn gid_of(descriptor: &str) -> Option<&str> {
    descriptor.get(descriptor.len().checked_sub(32)?..)
}

/// Distinct rails `mints` round-robin tokens should name. A client reporting
/// no healthy rail still mints on one, so the floor is 1.
fn expected_rails(mints: usize, healthy: usize) -> usize {
    mints.min(healthy.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gid_is_the_trailing_32_hex_chars() {
        let gid = "0000000000000000ffffc0a80001aabb";
        let descriptor = format!("{}:{gid}", "0".repeat(48));
        assert_eq!(gid_of(&descriptor), Some(gid));
    }

    #[test]
    fn gid_of_rejects_a_short_descriptor() {
        for short in ["", "abc", &"f".repeat(31)] {
            assert_eq!(gid_of(short), None, "{short:?}");
        }
    }

    #[test]
    fn gid_of_accepts_an_exactly_32_char_descriptor() {
        let gid = "f".repeat(32);
        assert_eq!(gid_of(&gid), Some(gid.as_str()));
    }

    #[test]
    fn expected_rails_is_capped_by_both_mints_and_rails() {
        assert_eq!(expected_rails(6, 1), 1);
        assert_eq!(expected_rails(6, 2), 2);
        assert_eq!(expected_rails(6, 8), 6);
        assert_eq!(expected_rails(6, 0), 1);
    }
}
