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
//
// Port of minio-cpp examples/GetPutRDMA.cc. PUT then GET an object over the
// RDMA fast path, into either page-aligned host memory or GPU device memory.
//
// CUDA dependency model: minio-rs does NOT link libcudart. This example
// dlopens libcudart.so so the binary runs on hosts that only have the
// NVIDIA driver (no CUDA Toolkit). Production code should link cuda_runtime
// directly via a real CUDA crate (e.g. `cust`, `rustacuda`) for type safety.

use libc::{c_int, c_void, size_t};
use std::env;
use std::ffi::{CStr, CString};
use std::fs::File;
use std::io::Write;
use std::process::ExitCode;
use std::ptr;

use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::rdma::RdmaBuffer;

type CudaSetDevice = unsafe extern "C" fn(c_int) -> c_int;
type CudaMalloc = unsafe extern "C" fn(*mut *mut c_void, size_t) -> c_int;
type CudaFree = unsafe extern "C" fn(*mut c_void) -> c_int;
type CudaMemset = unsafe extern "C" fn(*mut c_void, c_int, size_t) -> c_int;
type CudaMemcpy = unsafe extern "C" fn(*mut c_void, *const c_void, size_t, c_int) -> c_int;
type CudaDeviceSync = unsafe extern "C" fn() -> c_int;

const CUDA_MEMCPY_DEVICE_TO_HOST: c_int = 2;

struct Cudart {
    lib: *mut c_void,
    set_device: CudaSetDevice,
    malloc: CudaMalloc,
    free: CudaFree,
    memset: CudaMemset,
    memcpy: CudaMemcpy,
    device_synchronize: CudaDeviceSync,
}

impl Cudart {
    unsafe fn load() -> Option<Self> {
        unsafe {
            for soname in &["libcudart.so.13\0", "libcudart.so.12\0", "libcudart.so\0"] {
                let lib = libc::dlopen(soname.as_ptr() as _, libc::RTLD_LAZY | libc::RTLD_GLOBAL);
                if !lib.is_null() {
                    let sym = |name: &CStr| libc::dlsym(lib, name.as_ptr());
                    let s_set = sym(c"cudaSetDevice");
                    let s_malloc = sym(c"cudaMalloc");
                    let s_free = sym(c"cudaFree");
                    let s_memset = sym(c"cudaMemset");
                    let s_memcpy = sym(c"cudaMemcpy");
                    let s_sync = sym(c"cudaDeviceSynchronize");
                    if s_set.is_null()
                        || s_malloc.is_null()
                        || s_free.is_null()
                        || s_memset.is_null()
                        || s_memcpy.is_null()
                        || s_sync.is_null()
                    {
                        continue;
                    }
                    return Some(Self {
                        lib,
                        set_device: std::mem::transmute::<*mut c_void, CudaSetDevice>(s_set),
                        malloc: std::mem::transmute::<*mut c_void, CudaMalloc>(s_malloc),
                        free: std::mem::transmute::<*mut c_void, CudaFree>(s_free),
                        memset: std::mem::transmute::<*mut c_void, CudaMemset>(s_memset),
                        memcpy: std::mem::transmute::<*mut c_void, CudaMemcpy>(s_memcpy),
                        device_synchronize: std::mem::transmute::<*mut c_void, CudaDeviceSync>(
                            s_sync,
                        ),
                    });
                }
            }
            None
        }
    }
}

impl Drop for Cudart {
    fn drop(&mut self) {
        if !self.lib.is_null() {
            unsafe { libc::dlclose(self.lib) };
        }
    }
}

fn usage(prog: &str) {
    eprintln!("usage: {prog} <server_address> <access_key> <secret_key> [size] [gpu]");
    eprintln!("  server_address: http://host:port");
    eprintln!("  size: bytes (default 10485760)");
    eprintln!("  gpu: literal 'gpu' to allocate via cudaMalloc instead of posix_memalign");
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        usage(&args[0]);
        return ExitCode::from(1);
    }

    let host = &args[1];
    let access_key = &args[2];
    let secret_key = &args[3];
    let bufsize: usize = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10 * 1024 * 1024);
    let gpu_enabled = args.get(5).map(|s| s == "gpu").unwrap_or(false);

    println!("size={bufsize} gpu={gpu_enabled}");

    let base_url: BaseUrl = match host.parse() {
        Ok(u) => u,
        Err(e) => {
            eprintln!("invalid endpoint {host}: {e}");
            return ExitCode::from(1);
        }
    };
    let client = match MinioClient::new(
        base_url,
        Some(StaticProvider::new(access_key, secret_key, None)),
        None,
        None,
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("client construction failed: {e}");
            return ExitCode::from(1);
        }
    };

    let cuda: Option<Cudart> = if gpu_enabled {
        match unsafe { Cudart::load() } {
            Some(c) => Some(c),
            None => {
                eprintln!("libcudart.so unavailable - install CUDA Toolkit or omit 'gpu'");
                return ExitCode::from(1);
            }
        }
    } else {
        None
    };

    let (buf_ptr, owned_host): (*mut c_void, Option<Vec<u8>>) = if let Some(cuda) = cuda.as_ref() {
        unsafe {
            if (cuda.set_device)(0) != 0 {
                eprintln!("cudaSetDevice failed");
                return ExitCode::from(1);
            }
            let mut dptr: *mut c_void = ptr::null_mut();
            if (cuda.malloc)(&mut dptr, bufsize) != 0 {
                eprintln!("cudaMalloc failed");
                return ExitCode::from(1);
            }
            if (cuda.memset)(dptr, b'A' as c_int, bufsize) != 0 {
                eprintln!("cudaMemset failed");
                return ExitCode::from(1);
            }
            (cuda.device_synchronize)();
            println!("GPU enabled");
            (dptr, None)
        }
    } else {
        let mut p: *mut c_void = ptr::null_mut();
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        if unsafe { libc::posix_memalign(&mut p, page, bufsize) } != 0 {
            eprintln!("posix_memalign failed for page={page} size={bufsize}");
            return ExitCode::from(1);
        }
        unsafe { ptr::write_bytes(p as *mut u8, b'A', bufsize) };
        (p, Some(Vec::new()))
    };

    let buffer = unsafe { RdmaBuffer::from_raw(buf_ptr, bufsize) };
    let bucket = "my-bucket";
    let object = "my-object";

    match client.rdma_put_object(bucket, object, buffer).await {
        Ok(resp) => println!(
            "\ndata uploaded successfully etag={} bytes={}",
            resp.etag, resp.bytes_transferred
        ),
        Err(e) => println!("PUT failed: {e}"),
    }

    if let Some(cuda) = cuda.as_ref() {
        unsafe {
            (cuda.memset)(buf_ptr, b'U' as c_int, bufsize);
            (cuda.device_synchronize)();
        }
    }

    match client.rdma_get_object(bucket, object, buffer).await {
        Ok(resp) => println!(
            "\ndata of {object} received successfully etag={} bytes={}",
            resp.etag, resp.bytes_transferred
        ),
        Err(e) => println!("GET failed: {e}"),
    }

    let mut hostbuf = vec![0u8; bufsize];
    if let Some(cuda) = cuda.as_ref() {
        unsafe {
            (cuda.memcpy)(
                hostbuf.as_mut_ptr() as *mut c_void,
                buf_ptr,
                bufsize,
                CUDA_MEMCPY_DEVICE_TO_HOST,
            );
        }
    } else {
        unsafe {
            ptr::copy_nonoverlapping(buf_ptr as *const u8, hostbuf.as_mut_ptr(), bufsize);
        }
    }

    match File::create("output.txt").and_then(|mut f| f.write_all(&hostbuf)) {
        Ok(()) => println!("Buffer written to output.txt"),
        Err(e) => eprintln!("Error writing output.txt: {e}"),
    }

    if let Some(cuda) = cuda.as_ref() {
        unsafe { (cuda.free)(buf_ptr) };
    } else {
        unsafe { libc::free(buf_ptr) };
    }
    drop(owned_host);
    drop(CString::default()); // silence unused-import warning for CString

    ExitCode::from(0)
}
