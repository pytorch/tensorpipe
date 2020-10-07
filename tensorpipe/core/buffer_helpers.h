/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/config.h>
#include <tensorpipe/core/buffer.h>

#define TP_BUFFER_CPU_FIELD_AND_ACCESSOR(type) \
  type cpu;                                    \
  template <>                                  \
  constexpr auto& get<CpuBuffer>() {           \
    return cpu;                                \
  }

#if TENSORPIPE_SUPPORTS_CUDA
#define TP_BUFFER_CUDA_FIELD_AND_ACCESSOR(type) \
  type cuda;                                    \
  template <>                                   \
  constexpr auto& get<CudaBuffer>() {           \
    return cuda;                                \
  }
#else
#define TP_BUFFER_CUDA_FIELD_AND_ACCESSOR(type)
#endif

#define TP_BUFFER_FIELD_AND_ACCESSOR(cpu_type, cuda_type) \
  struct {                                                \
    template <typename TBuffer>                           \
    constexpr auto& get();                                \
    TP_BUFFER_CPU_FIELD_AND_ACCESSOR(cpu_type);           \
    TP_BUFFER_CUDA_FIELD_AND_ACCESSOR(cuda_type);         \
  }

namespace tensorpipe {

template <typename TVisitor>
constexpr auto switchOnDeviceType(DeviceType dt, TVisitor visitor) {
  switch (dt) {
    case DeviceType::kCpu:
      return visitor(CpuBuffer{});
#if TENSORPIPE_SUPPORTS_CUDA
    case DeviceType::kCuda:
      return visitor(CudaBuffer{});
#endif // TENSORPIPE_SUPPORTS_CUDA
    default:
      TP_THROW_ASSERT() << "Unknown device type.";
  };
}

template <typename TVisitor>
void forEachDeviceType(TVisitor visitor) {
  visitor(CpuBuffer{});
#if TENSORPIPE_SUPPORTS_CUDA
  visitor(CudaBuffer{});
#endif // TENSORPIPE_SUPPORTS_CUDA
}

} // namespace tensorpipe
