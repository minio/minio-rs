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

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    if env::var("CARGO_FEATURE_RDMA").is_err() {
        return;
    }

    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("linux") {
        panic!("`rdma` feature is only supported on Linux");
    }

    let arch = match env::var("CARGO_CFG_TARGET_ARCH").as_deref() {
        Ok("x86_64") => "x86_64",
        Ok("aarch64") => "aarch64",
        Ok(other) => panic!("`rdma` feature: unsupported target arch `{other}`"),
        Err(_) => panic!("CARGO_CFG_TARGET_ARCH not set"),
    };

    // The vendored copy is the default, but its path is this build host's
    // absolute CARGO_MANIFEST_DIR and it gets baked into every downstream
    // binary as an rpath. A packager shipping libs3rdma to a system prefix
    // points at it here instead.
    println!("cargo:rerun-if-env-changed=S3RDMA_LIB_DIR");
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let lib_dir = match env::var_os("S3RDMA_LIB_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => manifest_dir
            .join("vendor")
            .join("s3rdma")
            .join("lib")
            .join(arch),
    };

    let probe = lib_dir.join("libs3rdma.so");
    if !probe.exists() {
        panic!(
            "`rdma` feature: libs3rdma not found for {arch} at {} (missing {}). \
             Run ./build-libs.sh in the s3rdma repo to build and vendor both \
             arches, or set $S3RDMA_LIB_DIR to an installed copy.",
            lib_dir.display(),
            probe.display()
        );
    }

    println!("cargo:rerun-if-changed={}", probe.display());
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());

    // libs3rdma pulls in libibverbs itself, at runtime -- an SDK that links
    // nothing but libs3rdma still builds on a host with no RDMA stack
    // installed, and fails on the first client_init instead.
    println!("cargo:rustc-link-lib=dylib=s3rdma");
}
