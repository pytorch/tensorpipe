/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <list>
#include <memory>
#include <string>

#include <cuda_runtime.h>

#include <tensorpipe/channel/channel_impl_boilerplate.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_buffer.h>
#include <tensorpipe/common/cuda_lib.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

class ContextImpl;

struct Descriptor;
struct Reply;

class SendOperation {
 public:
  uint64_t sequenceNumber;
  TSendCallback callback;

  SendOperation(
      uint64_t sequenceNumber,
      TSendCallback callback,
      int deviceIdx,
      const void* ptr,
      cudaStream_t stream);

  Descriptor descriptor(const CudaLib& cudaLib);

  void process(const cudaIpcEventHandle_t& stopEvHandle);

 private:
  const int deviceIdx_;
  const void* ptr_;
  cudaStream_t stream_;
  CudaEvent startEv_;
};

struct RecvOperation {
 public:
  uint64_t sequenceNumber;

  RecvOperation(
      uint64_t sequenceNumber,
      int deviceIdx,
      void* ptr,
      cudaStream_t stream,
      size_t length);

  Reply reply();

  void process(
      const cudaIpcEventHandle_t& startEvHandle,
      const cudaIpcMemHandle_t& remoteHandle,
      size_t offset);

 private:
  const int deviceIdx_;
  void* ptr_;
  cudaStream_t stream_;
  size_t length_;
  CudaEvent stopEv_;
};

class ChannelImpl final
    : public ChannelImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl> {
 public:
  ChannelImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      std::shared_ptr<transport::Connection> connection);

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

 private:
  const std::shared_ptr<transport::Connection> connection_;

  // List of alive send operations.
  std::list<SendOperation> sendOperations_;

  // List of alive recv operations.
  std::list<RecvOperation> recvOperations_;

  void readPackets();
  void onReply(const Reply& nopReply);
  void onAck();
};

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
