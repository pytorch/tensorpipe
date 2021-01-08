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

#define TP_CPU_DEVICE_FIELD_AND_ACCESSOR(t)         \
  t cpu;                                            \
  auto& get(::tensorpipe::CpuBuffer /* unused */) { \
    return cpu;                                     \
  }

#if TENSORPIPE_SUPPORTS_CUDA
#define TP_CUDA_DEVICE_FIELD_AND_ACCESSOR(t)         \
  t cuda;                                            \
  auto& get(::tensorpipe::CudaBuffer /* unused */) { \
    return cuda;                                     \
  }
#else
#define TP_CUDA_DEVICE_FIELD_AND_ACCESSOR(t)
#endif // TENSORPIPE_SUPPORTS_CUDA

#define TP_DEVICE_FIELD(cpu_type, cuda_type)      \
  class {                                         \
   private:                                       \
    TP_CPU_DEVICE_FIELD_AND_ACCESSOR(cpu_type);   \
    TP_CUDA_DEVICE_FIELD_AND_ACCESSOR(cuda_type); \
                                                  \
   public:                                        \
    template <typename TBuffer>                   \
    auto& get() {                                 \
      return get(TBuffer());                      \
    }                                             \
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
  // Dummy return to make compiler happy.
  return visitor(CpuBuffer{});
}

template <typename TVisitor>
void forEachDeviceType(TVisitor visitor) {
  visitor(CpuBuffer{});
#if TENSORPIPE_SUPPORTS_CUDA
  visitor(CudaBuffer{});
#endif // TENSORPIPE_SUPPORTS_CUDA
}

} // namespace tensorpipe
