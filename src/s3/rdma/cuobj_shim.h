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
// C ABI wrapping NVIDIA libcuobjclient's `cuObjClient` C++ class so the Rust
// side can bind to it without a C++ compiler. Mirrors the surface used by
// minio-cpp's RDMA path: descriptor lifecycle, token mint/release,
// connectivity probe, memory-type detection.

#ifndef MINIORS_CUOBJ_SHIM_H
#define MINIORS_CUOBJ_SHIM_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct miniors_cuobj_client miniors_cuobj_client;

#define MINIORS_CUOBJ_SUCCESS 0
#define MINIORS_CUOBJ_FAIL 1

#define MINIORS_CUOBJ_OP_GET 0
#define MINIORS_CUOBJ_OP_PUT 1

#define MINIORS_CUOBJ_MEM_SYSTEM 0
#define MINIORS_CUOBJ_MEM_CUDA_MANAGED 1
#define MINIORS_CUOBJ_MEM_CUDA_DEVICE 2
#define MINIORS_CUOBJ_MEM_UNKNOWN 3

miniors_cuobj_client* miniors_cuobj_client_new(void);
void miniors_cuobj_client_free(miniors_cuobj_client* client);

int miniors_cuobj_is_connected(miniors_cuobj_client* client);

int miniors_cuobj_get_descriptor(miniors_cuobj_client* client, void* ptr,
                                 size_t size);
int miniors_cuobj_put_descriptor(miniors_cuobj_client* client, void* ptr);

int miniors_cuobj_get_rdma_token(miniors_cuobj_client* client, void* ptr,
                                 size_t size, size_t offset, int op,
                                 char** token_out);
int miniors_cuobj_put_rdma_token(miniors_cuobj_client* client, char* token);

int miniors_cuobj_memory_type(const void* ptr);

#ifdef __cplusplus
}
#endif

#endif
