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

#if TENSORPIPE_SUPPORTS_CUDA
#define TP_IF_CUDA(x) x
#else
#define TP_IF_CUDA(x)
#endif // TENSORPIPE_SUPPORTS_CUDA

#define TP_DEVICE_FIELD(cpu_type, cuda_type)                               \
  struct {                                                                 \
    template <typename TBuffer>                                            \
    constexpr auto& get() {                                                \
      if (std::is_same<TBuffer, CpuBuffer>::value) {                       \
        return cpu;                                                        \
      }                                                                    \
      TP_IF_CUDA(                                                          \
          if (std::is_same<TBuffer, CudaBuffer>::value) { return cuda; }); \
    }                                                                      \
    cpu_type cpu;                                                          \
    TP_IF_CUDA(cuda_type cuda);                                            \
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
