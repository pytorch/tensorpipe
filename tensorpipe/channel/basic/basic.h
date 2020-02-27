/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <list>

#include <tensorpipe/channel/channel.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/proto/channel/basic.pb.h>

namespace tensorpipe {
namespace channel {
namespace basic {

class BasicChannelFactory : public ChannelFactory {
 public:
  explicit BasicChannelFactory();

  ~BasicChannelFactory() override;

  const std::string& domainDescriptor() const override;

  std::shared_ptr<Channel> createChannel(
      std::shared_ptr<transport::Connection>,
      Channel::Endpoint) override;

 private:
  std::string domainDescriptor_;
};

class BasicChannel : public Channel,
                     public std::enable_shared_from_this<BasicChannel> {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  BasicChannel(
      ConstructorToken,
      std::shared_ptr<transport::Connection> connection);

  // Send memory region to peer.
  TDescriptor send(const void* ptr, size_t length, TSendCallback callback)
      override;

  // Receive memory region from peer.
  void recv(
      TDescriptor descriptor,
      void* ptr,
      size_t length,
      TRecvCallback callback) override;

 private:
  // Each time a thread starts running some of the channel's code, we acquire
  // this mutex. There are two "entry points" where control is handed to the
  // channel: the public user-facing functions, and the callbacks (which we
  // always wrap with wrapFooCallback_, which first calls fooEntryPoint_, which
  // is where the mutex is acquired). Some internal methods may however want to
  // temporarily release the lock, so we give all of them a reference to the
  // lock that has been acquired at the entry point.
  std::mutex mutex_;
  using TLock = std::unique_lock<std::mutex>&;

  // Called by factory class after construction.
  void init_();

  // Arm connection to read next protobuf packet.
  void readPacket_(TLock lock);

  // Called when a protobuf packet was received.
  void onPacket_(const proto::Packet& packet, TLock lock);

  // Called when protobuf packet is a request.
  void onRequest_(const proto::Request& request, TLock lock);

  // Called when protobuf packet is a reply.
  void onReply_(const proto::Reply& reply, TLock lock);

  // Allow factory class to call `init_()`.
  friend class BasicChannelFactory;

  std::shared_ptr<transport::Connection> connection_;
  Error error_{Error::kSuccess};

  // Increasing identifier for send operations.
  uint64_t id_{0};

  // State capturing a single send operation.
  struct SendOperation {
    const uint64_t id;
    const void* ptr;
    size_t length;
    TSendCallback callback;
  };

  // State capturing a single recv operation.
  struct RecvOperation {
    const uint64_t id;
    void* ptr;
    size_t length;
    TRecvCallback callback;
  };

  std::list<SendOperation> sendOperations_;
  std::list<RecvOperation> recvOperations_;

  // Called if send operation was successful.
  void sendCompleted(const uint64_t, TLock);

  // Called if recv operation was successful.
  void recvCompleted(const uint64_t, TLock);

  // Helpers to prepare callbacks from transports
  CallbackWrapper<BasicChannel, const void*, size_t> readCallbackWrapper_;
  CallbackWrapper<BasicChannel> readProtoCallbackWrapper_;
  CallbackWrapper<BasicChannel> writeCallbackWrapper_;

  // Helper function to process transport error.
  // Shared between read and write callback entry points.
  void handleError_(TLock lock);

  // For some odd reason it seems we need to use a qualified name here...
  template <typename T, typename... Args>
  friend class tensorpipe::CallbackWrapper;
};

} // namespace basic
} // namespace channel
} // namespace tensorpipe
