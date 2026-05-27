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
// Port of minio-cpp examples/GPUHostDisk.cc. Pure CUDA driver-API exercise
// (allocate GPU memory, fill, copy back, write to disk) with no SDK calls.
// Lives in examples/ as a companion to get_put_rdma.rs because the C++
// repo ships them together; useful for verifying libcuda.so on the host.

use libc::{c_int, c_uint, c_void};
use std::env;
use std::ffi::CStr;
use std::fs::File;
use std::io::Write;
use std::process::ExitCode;
use std::ptr;

type CuInit = unsafe extern "C" fn(c_uint) -> c_int;
type CuDeviceGet = unsafe extern "C" fn(*mut c_int, c_int) -> c_int;
type CuCtxCreate = unsafe extern "C" fn(*mut *mut c_void, c_uint, c_int) -> c_int;
type CuCtxDestroy = unsafe extern "C" fn(*mut c_void) -> c_int;
type CuMemAlloc = unsafe extern "C" fn(*mut u64, libc::size_t) -> c_int;
type CuMemFree = unsafe extern "C" fn(u64) -> c_int;
type CuMemsetD8 = unsafe extern "C" fn(u64, u8, libc::size_t) -> c_int;
type CuMemcpyDtoH = unsafe extern "C" fn(*mut c_void, u64, libc::size_t) -> c_int;
type CuCtxSynchronize = unsafe extern "C" fn() -> c_int;

struct Cuda {
    lib: *mut c_void,
    init: CuInit,
    device_get: CuDeviceGet,
    ctx_create: CuCtxCreate,
    ctx_destroy: CuCtxDestroy,
    mem_alloc: CuMemAlloc,
    mem_free: CuMemFree,
    memset_d8: CuMemsetD8,
    memcpy_dtoh: CuMemcpyDtoH,
    ctx_synchronize: CuCtxSynchronize,
}

unsafe fn sym(lib: *mut c_void, versioned: &CStr, fallback: &CStr) -> *mut c_void {
    unsafe {
        let s = libc::dlsym(lib, versioned.as_ptr());
        if !s.is_null() {
            return s;
        }
        libc::dlsym(lib, fallback.as_ptr())
    }
}

impl Cuda {
    unsafe fn load() -> Option<Self> {
        unsafe {
            for soname in &["libcuda.so.1\0", "libcuda.so\0"] {
                let lib = libc::dlopen(soname.as_ptr() as _, libc::RTLD_LAZY | libc::RTLD_GLOBAL);
                if lib.is_null() {
                    continue;
                }
                let s_init = sym(lib, c"cuInit", c"cuInit");
                let s_dev = sym(lib, c"cuDeviceGet", c"cuDeviceGet");
                let s_ctx_create = sym(lib, c"cuCtxCreate_v2", c"cuCtxCreate");
                let s_ctx_destroy = sym(lib, c"cuCtxDestroy_v2", c"cuCtxDestroy");
                let s_alloc = sym(lib, c"cuMemAlloc_v2", c"cuMemAlloc");
                let s_free = sym(lib, c"cuMemFree_v2", c"cuMemFree");
                let s_memset = sym(lib, c"cuMemsetD8_v2", c"cuMemsetD8");
                let s_memcpy = sym(lib, c"cuMemcpyDtoH_v2", c"cuMemcpyDtoH");
                let s_sync = sym(lib, c"cuCtxSynchronize", c"cuCtxSynchronize");
                if [
                    s_init,
                    s_dev,
                    s_ctx_create,
                    s_ctx_destroy,
                    s_alloc,
                    s_free,
                    s_memset,
                    s_memcpy,
                    s_sync,
                ]
                .iter()
                .any(|p| p.is_null())
                {
                    continue;
                }
                return Some(Self {
                    lib,
                    init: std::mem::transmute::<*mut c_void, CuInit>(s_init),
                    device_get: std::mem::transmute::<*mut c_void, CuDeviceGet>(s_dev),
                    ctx_create: std::mem::transmute::<*mut c_void, CuCtxCreate>(s_ctx_create),
                    ctx_destroy: std::mem::transmute::<*mut c_void, CuCtxDestroy>(s_ctx_destroy),
                    mem_alloc: std::mem::transmute::<*mut c_void, CuMemAlloc>(s_alloc),
                    mem_free: std::mem::transmute::<*mut c_void, CuMemFree>(s_free),
                    memset_d8: std::mem::transmute::<*mut c_void, CuMemsetD8>(s_memset),
                    memcpy_dtoh: std::mem::transmute::<*mut c_void, CuMemcpyDtoH>(s_memcpy),
                    ctx_synchronize: std::mem::transmute::<*mut c_void, CuCtxSynchronize>(s_sync),
                });
            }
            None
        }
    }
}

impl Drop for Cuda {
    fn drop(&mut self) {
        if !self.lib.is_null() {
            unsafe { libc::dlclose(self.lib) };
        }
    }
}

fn main() -> ExitCode {
    let bufsize: usize = env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10 * 1024 * 1024);

    let cuda = match unsafe { Cuda::load() } {
        Some(c) => c,
        None => {
            eprintln!("libcuda.so not found - install NVIDIA driver");
            return ExitCode::from(1);
        }
    };

    unsafe {
        if (cuda.init)(0) != 0 {
            eprintln!("cuInit failed");
            return ExitCode::from(1);
        }
        let mut dev: c_int = 0;
        if (cuda.device_get)(&mut dev, 0) != 0 {
            eprintln!("cuDeviceGet failed");
            return ExitCode::from(1);
        }
        let mut ctx: *mut c_void = ptr::null_mut();
        if (cuda.ctx_create)(&mut ctx, 0, dev) != 0 {
            eprintln!("cuCtxCreate failed");
            return ExitCode::from(1);
        }
        let mut dptr: u64 = 0;
        if (cuda.mem_alloc)(&mut dptr, bufsize) != 0 {
            eprintln!("cuMemAlloc failed");
            (cuda.ctx_destroy)(ctx);
            return ExitCode::from(1);
        }
        (cuda.memset_d8)(dptr, b'A', bufsize);
        (cuda.ctx_synchronize)();

        let mut hostbuf = vec![0u8; bufsize];
        (cuda.memcpy_dtoh)(hostbuf.as_mut_ptr() as *mut c_void, dptr, bufsize);

        match File::create("output.txt").and_then(|mut f| f.write_all(&hostbuf)) {
            Ok(()) => println!("Buffer written to output.txt"),
            Err(e) => eprintln!("Error writing output.txt: {e}"),
        }

        (cuda.mem_free)(dptr);
        (cuda.ctx_destroy)(ctx);
    }

    ExitCode::from(0)
}
