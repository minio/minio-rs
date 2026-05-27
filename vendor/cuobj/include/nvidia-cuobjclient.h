/*
 * SPDX-FileCopyrightText: Copyright (c) 2024  NVIDIA CORPORATION & AFFILIATES.
 * All rights reserved. SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */
#ifndef _CUOBJCLIENT_H_
#define _CUOBJCLIENT_H_

#define OBJ_RDMA_V1 "CUOBJ"

#include <stdio.h>
#include <unistd.h>

#include <iostream>
#include <memory>
#include <mutex>
#include <string>

#include "nvidia-cufile.h"

/**
 * @brief cuObject error numbers.
 *
 * @note  These errors will be expanded in future
 *
 */

typedef enum cuObjErr_enum {
  CU_OBJ_SUCCESS = 0, /**< Operation successfully completed */
  CU_OBJ_FAIL = 1,    /**< Operation failed */
} cuObjErr_t;

/**
 * @brief cuObject RDMA descriptor protocol version
 *
 */
typedef enum cuObjProto_enum {
  CUOBJ_PROTO_RDMA_DC_V1 = 1001, /**< RDMA support version 1 */
  CUOBJ_PROTO_MAX
} cuObjProto_t;

/**
 * @brief cuObject Operation type
 *
 */
typedef enum cuObjOpType_enum {
  CUOBJ_GET = 0, /**< GET operation */
  CUOBJ_PUT = 1, /**< PUT operation */
  CUOBJ_INVALID = 9999
} cuObjOpType_t;

/**
 * @brief cuObject Operation callbacks
 * This struct specifies the callback interfaces used by cuObjClient class
 * object during IO operations.
 * @note The callbacks can be called from a different thread than the caller
 * thread. user must lock any shared resources that can be used concurrently
 * across multiple callers.
 */
typedef struct CUObjIOOps {
  /**
   * @brief cuObject GET callback
   * @param handle cookie to the user context provided in the cuObjGet call.
   * cuObjClient::getCtx(handle) should be called for getting the user context
   * @param ptr pointer to the start of the memory chunk
   * @param size size of the memory chunk being read.
   * @param offset starting object offset for this memory chunk.
   * @param cufileRDMAInfo_t Pointer to a RDMA memory descriptor string
   *
   * @return size of the data read on success or negative -1, the data read is
   * obtained from control path
   *
   * @note offset will be set to zero for cases where the MaxReqCallbackSize is
   * equal to or greater the cuObjectGet call size
   * @note size will be set to total requested size n cuObjectGet for cases
   * where the MaxReqCallbackSize is equal to or greater the cuObjectGet call
   * size
   *
   *
   * @see cuObjClient::cuObjGet
   */

  ssize_t (*get)(const void *handle, char *ptr, size_t size, loff_t offset,
                 const cufileRDMAInfo_t *);
  /**
   * @brief cuObject PUT callback
   * @param handle to the user context provided in the cuObjPut call.
   * cuObjClient::getCtx(handle) should be called for getting the user context
   * @param ptr pointer to the start of the memory chunk
   * @param size size of the memory chunk being written
   * @param offset starting object offset for this memory chunk.
   * @param cufileRDMAInfo_t Pointer to a RDMA memory descriptor string
   *
   * @return size of the data written on success or negative -1, the data
   * written is obtained from control path
   *
   * @note offset will be set to zero for cases where the MaxReqCallbackSize is
   * equal to or greater the cuObjectPut call size
   * @note size will be set to total requested size n cuObjectGet for cases
   * where the MaxReqCallbackSize is equal to or greater the cuObjectPut call
   * size
   *
   *
   * @see cuObjClient::cuObjPut
   */

  ssize_t (*put)(const void *handle, const char *ptr, size_t size,
                 loff_t offset, const cufileRDMAInfo_t *);
} CUObjOps_t;

/**
 * @brief cuObject memory type
 *
 */
typedef enum cuObjMemoryType_enum {
  CUOBJ_MEMORY_SYSTEM = 0,
  CUOBJ_MEMORY_CUDA_MANAGED = 1,
  CUOBJ_MEMORY_CUDA_DEVICE = 2,
  CUOBJ_MEMORY_UNKNOWN = 3
} cuObjMemoryType_t;

/**
 * @brief cuObjClient class.
 *
 * cuObjClient Provides client side APIs to prepare PUT/GET operations for
 * out-of-band RDMA IO operations. The user of this object is expected to
 * implement a set of callback interfaces specified in CUObjIOOps. Once the
 * client object is created, user is expected to optionally register the memory
 * for RDMA and perform PUT and GET operations on this client object. The
 * cuObjClient will validate the memory and prepare a memory region for RDMA
 * transer. The cuObjClient will call one or more callback operations with
 * relevant RDMA information. The user is expected to relay the RDMA information
 * and other data to cuObjServer using standard control path. After the IO
 * completion, the callback should return the total data read or written in this
 * context. The client object may perform additional operations before the
 * cuObjectPut/cuObjectGet operation finishes.
 */

class cuObjClient {
 public:
  /**
   * @brief constructor for cuObjClient class.
   * @param ops callback reference to CUObjIOOps
   * @param proto RDMA descriptor protocol used for this client. defaults to
   * CUOBJ_PROTO_RDMA_DC_V1
   */
  cuObjClient(CUObjOps_t &ops, cuObjProto_t proto = CUOBJ_PROTO_RDMA_DC_V1);
  ~cuObjClient();

  /**
   * @brief Acquire a RDMA memory descriptor for the user memory
   * @param ptr start address of user memory
   * @param size size of memory that needs pinning starting from the start
   * address of user memory
   */
  cuObjErr_t cuMemObjGetDescriptor(void *ptr, size_t size);
  /**
   * @brief Get a RDMA memory descriptor for the user memory
   * @param ptr start address of user memory
   * @return max size of the callback for this memory pointer
   * @note: The size can be smaller than allocated memory if the memory is not
   * registered or the underlying RDMA subsystem does not allow for
   * pinning/transfer of the entire memory in a single callback
   */
  ssize_t cuMemObjGetMaxRequestCallbackSize(void *ptr);

  /**
   * @brief release the RDMA memory descriptor for the user memory
   * @param ptr start address of user memory used during cuMemObjGetDescriptor
   * @return error status if the memory cannot be unregistered
   */

  cuObjErr_t cuMemObjPutDescriptor(void *ptr);

  /**
   * @brief Get the user context provided in the cuObjGet and cuObjPut from
   * handle in the callback
   * @param handle pointer to the handle from the callback
   * @return void pointer to the user context.
   */

  static void *getCtx(const void *handle);

  /**
   * @brief API to perform GET operation using cuObject
   * @param ctx pointer to a user control context, used in GET callback
   * @param ptr pointer to a user memory
   * @param size size of the GET operation
   * @param offset currently set to 0. reserved for future use
   * @param buf_offset currently set to 0. reserved for future use
   * @return data returned by the cuObjServer or a negative error code.
   */

  ssize_t cuObjGet(void *ctx, void *ptr, size_t size, loff_t offset = 0,
                   loff_t buf_offset = 0);
  /**
   * @brief API to perform PUT operation using cuObject
   * @param ctx pointer to a user control context, used in PUT callback
   * @param ptr pointer to a user memory
   * @param size size of the PUT operation
   * @param offset currently set to 0. reserved for future use
   * @param buf_offset currently set to 0. reserved for future use
   * @return data returned by the cuObjServer or a negative error code.
   */

  ssize_t cuObjPut(void *ctx, void *ptr, size_t size, loff_t offset = 0,
                   loff_t buf_offset = 0);
  /**
   * @brief check if the client is connected
   */
  bool isConnected(void);
  /**
   * @brief Acquire an RDMA token for the registered memory
   * @param ptr start address of registered user memory
   * @param size size of the transfer
   * @param offset offset within the buffer
   * @param op operation type (CUOBJ_GET or CUOBJ_PUT)
   * @param token output pointer to the token string (caller must release with
   * cuMemObjPutRDMAToken)
   * @return CU_OBJ_SUCCESS on success, CU_OBJ_FAIL on failure
   */
  cuObjErr_t cuMemObjGetRDMAToken(void *ptr, size_t size, size_t offset,
                                  cuObjOpType_t op, char **token);

  /**
   * @brief Release an RDMA token acquired by cuMemObjGetRDMAToken
   * @param token the token string to release
   * @return CU_OBJ_SUCCESS on success, CU_OBJ_FAIL on failure
   */
  cuObjErr_t cuMemObjPutRDMAToken(char *token);

  /**
   * @brief setup telemetry output stream
   */
  static void setupTelemetry(bool use_OTEL, std::ostream *os);

  /**
   * @brief shutdown telemetry
   */
  static void shutdownTelemetry();

  /**
   * @brief setup telemetry stream logging level
   */
  static void setTelemFlags(unsigned flags);

  /**
   * @brief Get the memory type of a given pointer
   * @param ptr pointer to the memory
   * @return memory type
   */
  static cuObjMemoryType_t getMemoryType(const void *ptr);

 private:
  bool cuObjRegisterKey();
  void *_ctx;
  CUfileHandle_t _cufh;
  CUfileFSOps _objectFsOps;
  bool _connected;
  cuObjProto_t _proto;
  static std::mutex _telemMutex;
  static std::ostream *_os;
  static int _telemRefCnt;
  static unsigned _debugFlags;
  static bool _useOTEL;
};

/**
 * @brief Abstract base class for cuObject telemetry
 */
class cuObjTelem {
 public:
  virtual ~cuObjTelem() = default;
};

/**
 * @brief Telemetry span for tracing cuObject operations
 */
class cuObjSpan {
 public:
  cuObjSpan(std::string name, std::ostream &os);
  ~cuObjSpan();

 private:
  std::string _name;
  std::ostream &_os;
};

/**
 * @brief ostream-based telemetry implementation for cuObject
 */
class cuObjTelem_ostream : public cuObjTelem {
 public:
  cuObjTelem_ostream(std::ostream &os);
  ~cuObjTelem_ostream() override;

  cuObjSpan getSpan(std::string name);
  void incPutCounter(int count);
  void incGetCounter(int count);
  void logError(const char *fmt, ...);
  void logDebug(const char *fmt, ...);
  void logInfo(const char *fmt, ...);

 private:
  std::ostream &_os;
};

std::shared_ptr<cuObjTelem> getSpan(std::shared_ptr<cuObjTelem> &telem,
                                    std::string name);

#endif
