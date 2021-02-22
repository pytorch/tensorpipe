/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <deque>
#include <memory>
#include <string>

#include <tensorpipe/channel/channel_impl_boilerplate.h>
#include <tensorpipe/channel/cpu_context.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_buffer.h>
#include <tensorpipe/common/cuda_host_allocator.h>
#include <tensorpipe/common/cuda_loop.h>

namespace tensorpipe {
namespace channel {
namespace cuda_basic {

class ContextImpl;

struct Operation {
  uint64_t sequenceNumber{0};
  size_t chunkId{0};
  size_t numChunks{0};
  cudaStream_t stream{cudaStreamDefault};
  void* cudaPtr{nullptr};
  size_t length{0};
  std::shared_ptr<uint8_t[]> tmpBuffer;
  std::function<void(const Error&)> callback;
  bool done{false};
};

class ChannelImpl final
    : public ChannelImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl> {
 public:
  ChannelImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      std::shared_ptr<transport::Connection> connection,
      std::shared_ptr<CpuChannel> cpuChannel,
      CudaLoop& cudaLoop,
      CudaHostAllocator& cudaHostAllocator);

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
  const std::shared_ptr<transport::Connection> connection_;
  const std::shared_ptr<CpuChannel> cpuChannel_;
  CudaLoop& cudaLoop_;
  CudaHostAllocator& cudaHostAllocator_;
  std::deque<Operation> sendOperations_;
  std::deque<Operation> recvOperations_;

  void cudaCopy(
      void* dst,
      const void* src,
      size_t length,
      int deviceIdx,
      cudaStream_t stream,
      std::function<void(const Error&)> callback);

  void onSendOpReadyForCopy(Operation& op);
  void onSendOpDone();
  void sendChunkDescriptor(Operation op, std::string descriptor);
  void sendChunkThroughCpuChannel(Operation op);

  void onRecvOpReadDescriptor(Operation& op, std::string descriptor);
  void onRecvOpReadyForRecv(Operation& op, std::string descriptor);
  void onRecvOpReadyForCopy(Operation& op);
  void onRecvOpDone();

  void tryCleanup();
  void cleanup();
};

} // namespace cuda_basic
} // namespace channel
} // namespace tensorpipe
