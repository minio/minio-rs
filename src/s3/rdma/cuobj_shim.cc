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

#include "cuobj_shim.h"

#include <new>

#include "nvidia-cuobjclient.h"

namespace {

cuObjOpType_t map_op(int op) {
  return op == MINIORS_CUOBJ_OP_PUT ? CUOBJ_PUT : CUOBJ_GET;
}

int map_memory_type(cuObjMemoryType_t t) {
  switch (t) {
    case CUOBJ_MEMORY_SYSTEM:
      return MINIORS_CUOBJ_MEM_SYSTEM;
    case CUOBJ_MEMORY_CUDA_MANAGED:
      return MINIORS_CUOBJ_MEM_CUDA_MANAGED;
    case CUOBJ_MEMORY_CUDA_DEVICE:
      return MINIORS_CUOBJ_MEM_CUDA_DEVICE;
    default:
      return MINIORS_CUOBJ_MEM_UNKNOWN;
  }
}

struct ClientHolder {
  CUObjIOOps ops{};
  cuObjClient client;
  ClientHolder() : client(ops, CUOBJ_PROTO_RDMA_DC_V1) {}
};

}  // namespace

extern "C" {

miniors_cuobj_client* miniors_cuobj_client_new(void) {
  try {
    return reinterpret_cast<miniors_cuobj_client*>(new ClientHolder());
  } catch (...) {
    return nullptr;
  }
}

void miniors_cuobj_client_free(miniors_cuobj_client* client) {
  delete reinterpret_cast<ClientHolder*>(client);
}

int miniors_cuobj_is_connected(miniors_cuobj_client* client) {
  if (client == nullptr) return 0;
  auto* holder = reinterpret_cast<ClientHolder*>(client);
  try {
    return holder->client.isConnected() ? 1 : 0;
  } catch (...) {
    return 0;
  }
}

int miniors_cuobj_get_descriptor(miniors_cuobj_client* client, void* ptr,
                                 size_t size) {
  if (client == nullptr) return MINIORS_CUOBJ_FAIL;
  auto* holder = reinterpret_cast<ClientHolder*>(client);
  try {
    return holder->client.cuMemObjGetDescriptor(ptr, size) == CU_OBJ_SUCCESS
               ? MINIORS_CUOBJ_SUCCESS
               : MINIORS_CUOBJ_FAIL;
  } catch (...) {
    return MINIORS_CUOBJ_FAIL;
  }
}

int miniors_cuobj_put_descriptor(miniors_cuobj_client* client, void* ptr) {
  if (client == nullptr) return MINIORS_CUOBJ_FAIL;
  auto* holder = reinterpret_cast<ClientHolder*>(client);
  try {
    return holder->client.cuMemObjPutDescriptor(ptr) == CU_OBJ_SUCCESS
               ? MINIORS_CUOBJ_SUCCESS
               : MINIORS_CUOBJ_FAIL;
  } catch (...) {
    return MINIORS_CUOBJ_FAIL;
  }
}

int miniors_cuobj_get_rdma_token(miniors_cuobj_client* client, void* ptr,
                                 size_t size, size_t offset, int op,
                                 char** token_out) {
  if (client == nullptr || token_out == nullptr) return MINIORS_CUOBJ_FAIL;
  *token_out = nullptr;
  auto* holder = reinterpret_cast<ClientHolder*>(client);
  try {
    char* token = nullptr;
    cuObjErr_t err = holder->client.cuMemObjGetRDMAToken(ptr, size, offset,
                                                         map_op(op), &token);
    if (err != CU_OBJ_SUCCESS || token == nullptr) {
      return MINIORS_CUOBJ_FAIL;
    }
    *token_out = token;
    return MINIORS_CUOBJ_SUCCESS;
  } catch (...) {
    return MINIORS_CUOBJ_FAIL;
  }
}

int miniors_cuobj_put_rdma_token(miniors_cuobj_client* client, char* token) {
  if (client == nullptr || token == nullptr) return MINIORS_CUOBJ_FAIL;
  auto* holder = reinterpret_cast<ClientHolder*>(client);
  try {
    return holder->client.cuMemObjPutRDMAToken(token) == CU_OBJ_SUCCESS
               ? MINIORS_CUOBJ_SUCCESS
               : MINIORS_CUOBJ_FAIL;
  } catch (...) {
    return MINIORS_CUOBJ_FAIL;
  }
}

int miniors_cuobj_memory_type(const void* ptr) {
  try {
    return map_memory_type(cuObjClient::getMemoryType(ptr));
  } catch (...) {
    return MINIORS_CUOBJ_MEM_UNKNOWN;
  }
}

}  // extern "C"
