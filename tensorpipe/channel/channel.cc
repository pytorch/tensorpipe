/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/channel.h>
#include <tensorpipe/common/tensor.h>

namespace tensorpipe {
namespace channel {
template <>
void Channel<CpuTensor>::send(
    const void* ptr,
    size_t length,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  send(
      CpuTensor{const_cast<void*>(ptr), length},
      std::move(descriptorCallback),
      std::move(callback));
}

template <>
void Channel<CpuTensor>::recv(
    TDescriptor descriptor,
    void* ptr,
    size_t length,
    TRecvCallback callback) {
  recv(std::move(descriptor), CpuTensor{ptr, length}, std::move(callback));
}

} // namespace channel
} // namespace tensorpipe
