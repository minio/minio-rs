//! Report the RDMA rails this host can carry traffic on, and prove that
//! consecutive tokens spread across them.
//!
//! Run with: cargo run --features rdma --example rdma_rails
use minio::s3::rdma::{RdmaBuffer, ScopedRegistration, shared_rdma_client};

fn main() {
    let Some(client) = shared_rdma_client() else {
        println!(
            "RDMA unavailable: {}",
            minio::s3::rdma::rdma_init_error().unwrap_or("no reason reported")
        );
        return;
    };
    println!(
        "rails: {} total, {} healthy",
        client.nic_count(),
        client.healthy_nic_count()
    );

    let len = 1 << 20;
    let mut buf = vec![0u8; len];
    let raw = unsafe { RdmaBuffer::from_raw(buf.as_mut_ptr() as *mut _, len) };
    let _reg = unsafe { ScopedRegistration::register(client, raw.ptr(), raw.len()) }
        .expect("register on every rail");

    // The GID is the trailing 32 hex chars of the descriptor.
    let mut seen = std::collections::BTreeSet::new();
    for i in 0..6 {
        let tok = unsafe { client.get_rdma_token(raw.ptr(), raw.len(), 0) }.expect("mint");
        let s = tok.as_cstr().to_str().unwrap();
        let gid = &s[s.len() - 32..];
        println!("  token {i} gid=...{}", &gid[20..]);
        seen.insert(gid.to_string());
    }
    println!("distinct rails across 6 mints: {}", seen.len());
}
