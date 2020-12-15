/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <string>

#include <tensorpipe/channel/channel_impl_boilerplate.h>
#include <tensorpipe/channel/cpu_context.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_buffer.h>
#include <tensorpipe/common/cuda_loop.h>

namespace tensorpipe {
namespace channel {
namespace cuda_basic {

class ContextImpl;

class ChannelImpl final
    : public ChannelImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl> {
 public:
  ChannelImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      std::shared_ptr<CpuChannel> cpuChannel,
      CudaLoop& cudaLoop);

 protected:
  // Implement the entry points called by ChannelImplBoilerplate.
  void initImplFromLoop() override;
  void sendImplFromLoop(
      uint64_t sequenceNumber,
      CudaBuffer buffer,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback) override;
  void recvImplFromLoop(
      uint64_t sequenceNumber,
      TDescriptor descriptor,
      CudaBuffer buffer,
      TRecvCallback callback) override;
  void handleErrorImpl() override;
  void setIdImpl() override;

 private:
  const std::shared_ptr<CpuChannel> cpuChannel_;
  CudaLoop& cudaLoop_;

  void onTempBufferReadyForSend(
      CudaBuffer buffer,
      CudaPinnedBuffer tmpBuffer,
      TDescriptorCallback descriptorCallback);

  void onCpuChannelRecv(
      CudaBuffer buffer,
      CudaPinnedBuffer tmpBuffer,
      TRecvCallback callback);
};

} // namespace cuda_basic
} // namespace channel
} // namespace tensorpipe
