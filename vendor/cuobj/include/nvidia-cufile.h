/*
 * Copyright 1993-2023 NVIDIA Corporation.  All rights reserved.
 *
 * NOTICE TO LICENSEE:
 *
 * This source code and/or documentation ("Licensed Deliverables") are
 * subject to NVIDIA intellectual property rights under U.S. and
 * international Copyright laws.
 *
 * These Licensed Deliverables contained herein is PROPRIETARY and
 * CONFIDENTIAL to NVIDIA and is being provided under the terms and
 * conditions of a form of NVIDIA software license agreement by and
 * between NVIDIA and Licensee ("License Agreement") or electronically
 * accepted by Licensee.  Notwithstanding any terms or conditions to
 * the contrary in the License Agreement, reproduction or disclosure
 * of the Licensed Deliverables to any third party without the express
 * written consent of NVIDIA is prohibited.
 *
 * NOTWITHSTANDING ANY TERMS OR CONDITIONS TO THE CONTRARY IN THE
 * LICENSE AGREEMENT, NVIDIA MAKES NO REPRESENTATION ABOUT THE
 * SUITABILITY OF THESE LICENSED DELIVERABLES FOR ANY PURPOSE.  IT IS
 * PROVIDED "AS IS" WITHOUT EXPRESS OR IMPLIED WARRANTY OF ANY KIND.
 * NVIDIA DISCLAIMS ALL WARRANTIES WITH REGARD TO THESE LICENSED
 * DELIVERABLES, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY,
 * NONINFRINGEMENT, AND FITNESS FOR A PARTICULAR PURPOSE.
 * NOTWITHSTANDING ANY TERMS OR CONDITIONS TO THE CONTRARY IN THE
 * LICENSE AGREEMENT, IN NO EVENT SHALL NVIDIA BE LIABLE FOR ANY
 * SPECIAL, INDIRECT, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, OR ANY
 * DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS,
 * WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
 * ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE
 * OF THESE LICENSED DELIVERABLES.
 *
 * U.S. Government End Users.  These Licensed Deliverables are a
 * "commercial item" as that term is defined at 48 C.F.R. 2.101 (OCT
 * 1995), consisting of "commercial computer software" and "commercial
 * computer software documentation" as such terms are used in 48
 * C.F.R. 12.212 (SEPT 1995) and is provided to the U.S. Government
 * only as a commercial end item.  Consistent with 48 C.F.R.12.212 and
 * 48 C.F.R. 227.7202-1 through 227.7202-4 (JUNE 1995), all
 * U.S. Government End Users acquire the Licensed Deliverables with
 * only those rights set forth herein.
 *
 * Any use of the Licensed Deliverables in individual and commercial
 * software must include, in the user documentation and internal
 * comments to the code, the above Disclaimer and U.S. Government End
 * Users Notice.
 */

/**
 * @file cufile.h
 * @brief  cuFile C APIs
 *
 * This file contains all the C APIs to perform GPUDirect Storage supported IO
 * operations
 */

#if __cplusplus
extern "C" {
#endif

/// @cond DOXYGEN_SKIP_MACRO
#ifndef __CUFILE_H_
#define __CUFILE_H_

#include <arpa/inet.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/socket.h>

#include "cuda.h"

#define CUFILEOP_BASE_ERR 5000

// Note :Data path errors are captured via standard error codes
#define CUFILEOP_STATUS_ENTRIES                                                \
  CUFILE_OP(0, CU_FILE_SUCCESS, cufile success)                                \
  CUFILE_OP(CUFILEOP_BASE_ERR + 1, CU_FILE_DRIVER_NOT_INITIALIZED,            \
            nvidia - fs driver is not loaded.Set allow_compat_mode to true in  \
                cufile.json file to enable compatible mode)                    \
  CUFILE_OP(CUFILEOP_BASE_ERR + 2, CU_FILE_DRIVER_INVALID_PROPS,              \
            invalid property)                                                 \
  CUFILE_OP(CUFILEOP_BASE_ERR + 3, CU_FILE_DRIVER_UNSUPPORTED_LIMIT,          \
            property range error)                                             \
  CUFILE_OP(CUFILEOP_BASE_ERR + 4, CU_FILE_DRIVER_VERSION_MISMATCH,           \
            nvidia - fs driver version mismatch)                              \
  CUFILE_OP(CUFILEOP_BASE_ERR + 5, CU_FILE_DRIVER_VERSION_READ_ERROR,         \
            nvidia - fs driver version read error)                            \
  CUFILE_OP(CUFILEOP_BASE_ERR + 6, CU_FILE_DRIVER_CLOSING,                    \
            driver shutdown in progress)                                      \
  CUFILE_OP(CUFILEOP_BASE_ERR + 7, CU_FILE_PLATFORM_NOT_SUPPORTED,            \
            GPUDirect Storage not supported on current platform)              \
  CUFILE_OP(CUFILEOP_BASE_ERR + 8, CU_FILE_IO_NOT_SUPPORTED,                  \
            GPUDirect Storage not supported on current file)                  \
  CUFILE_OP(CUFILEOP_BASE_ERR + 9, CU_FILE_DEVICE_NOT_SUPPORTED,              \
            GPUDirect Storage not supported on current GPU)                   \
  CUFILE_OP(CUFILEOP_BASE_ERR + 10, CU_FILE_NVFS_DRIVER_ERROR,                \
            nvidia - fs driver ioctl error)                                   \
  CUFILE_OP(CUFILEOP_BASE_ERR + 11, CU_FILE_CUDA_DRIVER_ERROR,                \
            CUDA Driver API error)                                            \
  CUFILE_OP(CUFILEOP_BASE_ERR + 12, CU_FILE_CUDA_POINTER_INVALID,             \
            invalid device pointer)                                           \
  CUFILE_OP(CUFILEOP_BASE_ERR + 13, CU_FILE_CUDA_MEMORY_TYPE_INVALID,         \
            invalid pointer memory type)                                      \
  CUFILE_OP(CUFILEOP_BASE_ERR + 14, CU_FILE_CUDA_POINTER_RANGE_ERROR,         \
            pointer range exceeds allocated address range)                    \
  CUFILE_OP(CUFILEOP_BASE_ERR + 15, CU_FILE_CUDA_CONTEXT_MISMATCH,            \
            cuda context mismatch)                                            \
  CUFILE_OP(CUFILEOP_BASE_ERR + 16, CU_FILE_INVALID_MAPPING_SIZE,             \
            access beyond maximum pinned size)                                \
  CUFILE_OP(CUFILEOP_BASE_ERR + 17, CU_FILE_INVALID_MAPPING_RANGE,            \
            access beyond mapped size)                                        \
  CUFILE_OP(CUFILEOP_BASE_ERR + 18, CU_FILE_INVALID_FILE_TYPE,                \
            unsupported file type)                                            \
  CUFILE_OP(CUFILEOP_BASE_ERR + 19, CU_FILE_INVALID_FILE_OPEN_FLAG,           \
            unsupported file open flags)                                      \
  CUFILE_OP(CUFILEOP_BASE_ERR + 20, CU_FILE_DIO_NOT_SET,                      \
            fd direct IO not set)                                             \
  CUFILE_OP(CUFILEOP_BASE_ERR + 22, CU_FILE_INVALID_VALUE,                    \
            invalid arguments)                                                \
  CUFILE_OP(CUFILEOP_BASE_ERR + 23, CU_FILE_MEMORY_ALREADY_REGISTERED,        \
            device pointer already registered)                                \
  CUFILE_OP(CUFILEOP_BASE_ERR + 24, CU_FILE_MEMORY_NOT_REGISTERED,            \
            device pointer lookup failure)                                    \
  CUFILE_OP(CUFILEOP_BASE_ERR + 25, CU_FILE_PERMISSION_DENIED,                \
            driver or file access error)                                      \
  CUFILE_OP(CUFILEOP_BASE_ERR + 26, CU_FILE_DRIVER_ALREADY_OPEN,              \
            driver is already open)                                           \
  CUFILE_OP(CUFILEOP_BASE_ERR + 27, CU_FILE_HANDLE_NOT_REGISTERED,            \
            file descriptor is not registered)                                \
  CUFILE_OP(CUFILEOP_BASE_ERR + 28, CU_FILE_HANDLE_ALREADY_REGISTERED,        \
            file descriptor is already registered)                            \
  CUFILE_OP(CUFILEOP_BASE_ERR + 29, CU_FILE_DEVICE_NOT_FOUND,                 \
            GPU device not found)                                             \
  CUFILE_OP(CUFILEOP_BASE_ERR + 30, CU_FILE_INTERNAL_ERROR, internal error)   \
  CUFILE_OP(CUFILEOP_BASE_ERR + 31, CU_FILE_GETNEWFD_FAILED,                  \
            failed to obtain new file descriptor)                             \
  CUFILE_OP(CUFILEOP_BASE_ERR + 33, CU_FILE_NVFS_SETUP_ERROR,                 \
            NVFS driver initialization error)                                 \
  CUFILE_OP(CUFILEOP_BASE_ERR + 34, CU_FILE_IO_DISABLED,                      \
            GPUDirect Storage disabled by config on current file)             \
  CUFILE_OP(CUFILEOP_BASE_ERR + 35, CU_FILE_BATCH_SUBMIT_FAILED,              \
            failed to submit batch operation)                                 \
  CUFILE_OP(CUFILEOP_BASE_ERR + 36, CU_FILE_GPU_MEMORY_PINNING_FAILED,        \
            Failed to allocate pinned GPU Memory)                             \
  CUFILE_OP(CUFILEOP_BASE_ERR + 37, CU_FILE_BATCH_FULL,                       \
            queue full for batch operation)                                   \
  CUFILE_OP(CUFILEOP_BASE_ERR + 38, CU_FILE_ASYNC_NOT_SUPPORTED,              \
            cuFile stream operation not supported)                            \
  CUFILE_OP(CUFILEOP_BASE_ERR + 39,                                           \
            CU_FILE_INTERNAL_BATCH_SETUP_ERROR,                               \
            batch setup internal error - retry later)                         \
  CUFILE_OP(CUFILEOP_BASE_ERR + 40,                                           \
            CU_FILE_INTERNAL_BATCH_SUBMIT_ERROR,                              \
            batch submit internal error - retry later)                        \
  CUFILE_OP(CUFILEOP_BASE_ERR + 41,                                           \
            CU_FILE_INTERNAL_BATCH_GETSTATUS_ERROR,                           \
            batch get status internal error - retry later)                    \
  CUFILE_OP(CUFILEOP_BASE_ERR + 42,                                           \
            CU_FILE_INTERNAL_BATCH_CANCEL_ERROR,                              \
            batch cancel internal error - retry later)                        \
  CUFILE_OP(CUFILEOP_BASE_ERR + 43, CU_FILE_NOMEM_ERROR,                      \
            cufile no memory error - retry later)                             \
  CUFILE_OP(CUFILEOP_BASE_ERR + 44, CU_FILE_IO_ERROR, cufile io error)        \
  CUFILE_OP(CUFILEOP_BASE_ERR + 45,                                           \
            CU_FILE_INTERNAL_BUF_REGISTER_ERROR,                              \
            cufile buf registration error)                                    \
  CUFILE_OP(CUFILEOP_BASE_ERR + 46, CU_FILE_HASH_OPR_ERROR,                   \
            cufile hash operation error)                                      \
  CUFILE_OP(CUFILEOP_BASE_ERR + 47, CU_FILE_INVALID_CONTEXT_ERROR,            \
            cufile invalid context error)                                     \
  CUFILE_OP(CUFILEOP_BASE_ERR + 48,                                           \
            CU_FILE_NVFS_INTERNAL_DRIVER_ERROR,                               \
            nvfs internal driver error)                                       \
  CUFILE_OP(CUFILEOP_BASE_ERR + 49, CU_FILE_BATCH_NOCOMPAT_ERROR,             \
            compat mode off error)                                            \
  CUFILE_OP(CUFILEOP_BASE_ERR + 50, CU_FILE_IO_MAX_ERROR,                     \
            GPUDirect Storage Max Error)

/**
 * @brief cufileop status enum
 */
typedef enum CUfileOpError {
/// @cond DOXYGEN_SKIP_MACRO
#define CUFILE_OP(code, name, string) name = code,
  CUFILEOP_STATUS_ENTRIES
#undef CUFILE_OP
  ///@endcond
} CUfileOpError;

/// @endcond

/**
 * @brief cufileop status string
 */
static inline const char *cufileop_status_error(CUfileOpError status) {
  switch (status) {
/// @cond DOXYGEN_SKIP_MACRO
#define CUFILE_OP(code, name, string) \
  case name:                          \
    return #string;
    CUFILEOP_STATUS_ENTRIES
#undef CUFILE_OP
    ///@endcond
    default:
      return "unknown cufile error";
  }
}

/**
 * @brief cufileop status string
 */
typedef struct CUfileError {
  CUfileOpError err;  // cufile error

  CUresult cu_err;  // cuda driver error

} CUfileError_t;

/**
 * @brief  error macros to inspect error status of type @ref CUfileOpError
 */

#define IS_CUFILE_ERR(err) (abs((err)) > CUFILEOP_BASE_ERR)

#define CUFILE_ERRSTR(err) cufileop_status_error((CUfileOpError)abs((err)))

#define IS_CUDA_ERR(status) ((status).err == CU_FILE_CUDA_DRIVER_ERROR)

#define CU_FILE_CUDA_ERR(status) ((status).cu_err)

/* driver properties */
typedef enum CUfileDriverStatusFlags {
  CU_FILE_LUSTRE_SUPPORTED = 0,     /*!< Support for DDN LUSTRE */
  CU_FILE_WEKAFS_SUPPORTED = 1,     /*!< Support for WEKAFS */
  CU_FILE_NFS_SUPPORTED = 2,        /*!< Support for NFS */
  CU_FILE_GPFS_SUPPORTED = 3,       /*!< Support for GPFS */
  CU_FILE_NVME_SUPPORTED = 4,       /*!< Support for NVMe */
  CU_FILE_NVMEOF_SUPPORTED = 5,     /*!< Support for NVMeOF */
  CU_FILE_SCSI_SUPPORTED = 6,       /*!< Support for SCSI */
  CU_FILE_SCALEFLUX_CSD_SUPPORTED = 7, /*!< Support for Scaleflux CSD */
  CU_FILE_NVMESH_SUPPORTED = 8,     /*!< Support for NVMesh Block Dev */
  CU_FILE_BEEGFS_SUPPORTED = 9,     /*!< Support for BeeGFS */
  // 10 is reserved for YRCloudFile
  CU_FILE_NVME_P2P_SUPPORTED = 11,  /*!< Deprecated */
  CU_FILE_SCATEFS_SUPPORTED = 12,   /*!< Support for ScateFS */
  CU_FILE_VIRTIOFS_SUPPORTED = 13,  /*!< Support for VirtioFS */
  CU_FILE_MAX_TARGET_TYPES,         /*!< Maximum FS supported */
} CUfileDriverStatusFlags_t;

typedef enum CUfileDriverControlFlags {
  CU_FILE_USE_POLL_MODE = 0,     /*!< use POLL mode */
  CU_FILE_ALLOW_COMPAT_MODE = 1, /*!< allow COMPATIBILITY mode */
  CU_FILE_POSIX_IO_MODE = 2,     /*!< Vanilla posix io mode */
  CU_FILE_FALLBACK_IO_MODE = 4   /*!< Fallback io mode */
} CUfileDriverControlFlags_t;

typedef enum CUfileFeatureFlags {
  CU_FILE_DYN_ROUTING_SUPPORTED = 0,  /*!< Dynamic routing support */
  CU_FILE_BATCH_IO_SUPPORTED = 1,     /*!< Batch IO support */
  CU_FILE_STREAMS_SUPPORTED = 2,      /*!< Streams support */
  CU_FILE_PARALLEL_IO_SUPPORTED = 3,  /*!< Parallel IO support */
  CU_FILE_P2P_SUPPORTED = 4           /*!< PCI P2PDMA support */
} CUfileFeatureFlags_t;

typedef enum CUfileP2PFlags {
  CUFILE_P2PDMA = 0,         /*!< Support for PCI P2PDMA */
  CUFILE_NVFS = 1,           /*!< Support for nvidia-fs */
  CUFILE_DMABUF = 2,         /*!< Support for DMA Buffer */
  CUFILE_C2C = 3,            /*!< Support for Chip-to-Chip (Grace) */
  CUFILE_NVIDIA_PEERMEM = 4  /*!< Only for IBM Spectrum Scale and WekaFS */
} CUfileP2PFlags_t;

/* P2P Flag constants */
#define CU_FILE_P2P_FLAG_PCI_P2PDMA ((CUfileP2PFlags_t)(1 << CUFILE_P2PDMA))
#define CU_FILE_P2P_FLAG_NVFS ((CUfileP2PFlags_t)(1 << CUFILE_NVFS))
#define CU_FILE_P2P_FLAG_DMABUF ((CUfileP2PFlags_t)(1 << CUFILE_DMABUF))
#define CU_FILE_P2P_FLAG_C2C ((CUfileP2PFlags_t)(1 << CUFILE_C2C))

typedef struct CUfileDrvProps {
  struct {
    unsigned int major_version;

    unsigned int minor_version;

    size_t poll_thresh_size;

    size_t max_direct_io_size;

    unsigned int dstatusflags;

    unsigned int dcontrolflags;

  } nvfs;

  unsigned int fflags;

  unsigned int max_device_cache_size;

  unsigned int per_buffer_cache_size;

  unsigned int max_device_pinned_mem_size;

  unsigned int max_batch_io_size;
  unsigned int max_batch_io_timeout_msecs;
} CUfileDrvProps_t;

typedef struct sockaddr sockaddr_t;

typedef struct cufileRDMAInfo {
  int version;
  int desc_len;
  const char *desc_str;
} cufileRDMAInfo_t;

#define CU_FILE_RDMA_REGISTER 1
#define CU_FILE_RDMA_RELAXED_ORDERING (1 << 1)

typedef struct CUfileFSOps {
  /* NULL means discover using fstat */
  const char *(*fs_type)(const void *handle);

  /* list of host addresses to use,  NULL means no restriction */
  int (*getRDMADeviceList)(const void *handle, sockaddr_t **hostaddrs);

  /* -1 no pref */
  int (*getRDMADevicePriority)(const void *handle, char *, size_t, loff_t,
                               const sockaddr_t *hostaddr);

  /* NULL means try VFS */
  ssize_t (*read)(const void *handle, char *, size_t, loff_t,
                  const cufileRDMAInfo_t *);
  ssize_t (*write)(const void *handle, const char *, size_t, loff_t,
                   const cufileRDMAInfo_t *);
} CUfileFSOps_t;

/* File Handle */
enum CUfileFileHandleType {
  CU_FILE_HANDLE_TYPE_OPAQUE_FD = 1, /*!< Linux based fd */

  CU_FILE_HANDLE_TYPE_OPAQUE_WIN32 =
      2, /*!< Windows based handle (unsupported) */

  CU_FILE_HANDLE_TYPE_USERSPACE_FS = 3, /* Userspace based FS */
};

typedef struct CUfileDescr_t {
  enum CUfileFileHandleType type; /* type of file being registered */
  union {
    int fd;       /* Linux   */
    void *handle; /* Windows */
  } handle;
  const CUfileFSOps_t *fs_ops; /* file system operation table */
} CUfileDescr_t;

/**
 * @brief File handle type
 */
typedef void *CUfileHandle_t;

#pragma GCC visibility push(default)

CUfileError_t cuFileHandleRegister(CUfileHandle_t *fh, CUfileDescr_t *descr);
void cuFileHandleDeregister(CUfileHandle_t fh);
CUfileError_t cuFileBufRegister(const void *bufPtr_base, size_t length,
                                int flags);
CUfileError_t cuFileBufDeregister(const void *bufPtr_base);
ssize_t cuFileRead(CUfileHandle_t fh, void *bufPtr_base, size_t size,
                   off_t file_offset, off_t bufPtr_offset);
ssize_t cuFileWrite(CUfileHandle_t fh, const void *bufPtr_base, size_t size,
                    off_t file_offset, off_t bufPtr_offset);

// CUFile Driver APIs
CUfileError_t cuFileDriverOpen(void);

CUfileError_t cuFileDriverClose(void);
#define cuFileDriverClose cuFileDriverClose_v2
CUfileError_t cuFileDriverClose(void);

long cuFileUseCount(void);
CUfileError_t cuFileDriverGetProperties(CUfileDrvProps_t *props);
CUfileError_t cuFileDriverSetPollMode(bool poll, size_t poll_threshold_size);
CUfileError_t cuFileDriverSetMaxDirectIOSize(size_t max_direct_io_size);
CUfileError_t cuFileDriverSetMaxCacheSize(size_t max_cache_size);
CUfileError_t cuFileDriverSetMaxPinnedMemSize(size_t max_pinned_size);

// Batch API's

typedef enum CUfileOpcode { CUFILE_READ = 0, CUFILE_WRITE } CUfileOpcode_t;

typedef enum CUFILEStatus_enum {
  CUFILE_WAITING = 0x000001,  /* required value prior to submission */
  CUFILE_PENDING = 0x000002,  /* once enqueued */
  CUFILE_INVALID = 0x000004,  /* ill-formed or could not be enqueued */
  CUFILE_CANCELED = 0x000008, /* request successfully canceled */
  CUFILE_COMPLETE = 0x0000010, /* request successfully completed */
  CUFILE_TIMEOUT = 0x0000020,  /* request timed out */
  CUFILE_FAILED = 0x0000040    /* unable to complete */
} CUfileStatus_t;

typedef enum cufileBatchMode {
  CUFILE_BATCH = 1,
} CUfileBatchMode_t;

typedef struct CUfileIOParams {
  CUfileBatchMode_t mode;  // Must be the very first field.
  union {
    struct {
      void *devPtr_base;
      off_t file_offset;
      off_t devPtr_offset;
      size_t size;
    } batch;
  } u;
  CUfileHandle_t fh;
  CUfileOpcode_t opcode;
  void *cookie;
} CUfileIOParams_t;

typedef struct CUfileIOEvents {
  void *cookie;
  CUfileStatus_t status; /* status of the operation */
  size_t ret;            /* -ve error or amount of I/O done. */
} CUfileIOEvents_t;

typedef void *CUfileBatchHandle_t;

CUfileError_t cuFileBatchIOSetUp(CUfileBatchHandle_t *batch_idp, unsigned nr);
CUfileError_t cuFileBatchIOSubmit(CUfileBatchHandle_t batch_idp, unsigned nr,
                                  CUfileIOParams_t *iocbp, unsigned int flags);
CUfileError_t cuFileBatchIOGetStatus(CUfileBatchHandle_t batch_idp,
                                     unsigned min_nr, unsigned *nr,
                                     CUfileIOEvents_t *iocbp,
                                     struct timespec *timeout);
CUfileError_t cuFileBatchIOCancel(CUfileBatchHandle_t batch_idp);
void cuFileBatchIODestroy(CUfileBatchHandle_t batch_idp);

// Async API's with CUDA streams

#define CU_FILE_STREAM_FIXED_BUF_OFFSET 1
#define CU_FILE_STREAM_FIXED_FILE_OFFSET 2
#define CU_FILE_STREAM_FIXED_FILE_SIZE 4
#define CU_FILE_STREAM_PAGE_ALIGNED_INPUTS 8

CUfileError_t cuFileReadAsync(CUfileHandle_t fh, void *bufPtr_base,
                              size_t *size_p, off_t *file_offset_p,
                              off_t *bufPtr_offset_p, ssize_t *bytes_read_p,
                              CUstream stream);
CUfileError_t cuFileWriteAsync(CUfileHandle_t fh, void *bufPtr_base,
                               size_t *size_p, off_t *file_offset_p,
                               off_t *bufPtr_offset_p, ssize_t *bytes_written_p,
                               CUstream stream);
CUfileError_t cuFileStreamRegister(CUstream stream, unsigned flags);
CUfileError_t cuFileStreamDeregister(CUstream stream);

// Scatter/gather IO

typedef struct CUfileIOVec {
  void *base;   // Pointer to data (device or host memory)
  size_t len;   // Length of data
} CUfileIOVec_t;

ssize_t cuFileReadv(CUfileHandle_t fh, const CUfileIOVec_t *iov,
                    size_t iovcnt, off_t file_offset, unsigned flags);
ssize_t cuFileWritev(CUfileHandle_t fh, const CUfileIOVec_t *iov,
                     size_t iovcnt, off_t file_offset, unsigned flags);

// Version and topology

CUfileError_t cuFileGetVersion(int *version);
CUfileError_t cuFileExportPCIeTopology(const char *filename);

#pragma GCC visibility pop

/// @cond DOXYGEN_SKIP_MACRO
#endif  // CUFILE_H
/// @endcond
#if __cplusplus
}
#endif
