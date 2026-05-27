/* Copyright (c) 2024, NVIDIA CORPORATION.  All rights reserved.

   NVIDIA CORPORATION and its licensors retain all intellectual property
   and proprietary rights in and to this software, related documentation
   and any modifications thereto.  Any use, reproduction, disclosure or
   distribution of this software and related documentation without an express
   license agreement from NVIDIA CORPORATION is strictly prohibited.
   */

#ifndef __CUFILE_INFO_H_
#define __CUFILE_INFO_H_

#define CUFILE_DLL_PUBLIC  __attribute__ ((visibility ("default")))

#include <stdio.h>

namespace cuFileInfo {
    ssize_t cuFileGetBufferSize(void *ptr);
    CUmemorytype cuFileGetMemoryType(const void *ptr);
}

#endif
