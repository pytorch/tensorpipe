/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <limits>
#include <list>
#include <mutex>

#include <tensorpipe/channel/channel.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/proto/channel/cma.pb.h>

namespace tensorpipe {
namespace channel {
namespace cma {

class CmaChannelFactory
    : public ChannelFactory,
      public std::enable_shared_from_this<CmaChannelFactory> {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  static std::shared_ptr<CmaChannelFactory> create();

  explicit CmaChannelFactory(ConstructorToken, int queueCapacity = INT_MAX);

  ~CmaChannelFactory() override;

  const std::string& domainDescriptor() const override;

  std::shared_ptr<Channel> createChannel(
      std::shared_ptr<transport::Connection>,
      Channel::Endpoint) override;

  void join() override;

 private:
  using copy_request_callback_fn = std::function<void(const Error&)>;

  struct CopyRequest {
    pid_t remotePid;
    void* remotePtr;
    void* localPtr;
    size_t length;
    copy_request_callback_fn callback;
  };

  mutable std::mutex mutex_;
  std::string domainDescriptor_;
  std::thread thread_;
  Queue<optional<CopyRequest>> requests_;
  std::atomic<bool> joined_{false};

  void init_();

  void requestCopy_(
      pid_t remotePid,
      void* remotePtr,
      void* localPtr,
      size_t length,
      copy_request_callback_fn fn);

  void handleCopyRequests_();

  friend class CmaChannel;
};

class CmaChannel : public Channel,
                   public std::enable_shared_from_this<CmaChannel> {
  struct ConstructorToken {};

 public:
  CmaChannel(
      ConstructorToken,
      std::shared_ptr<CmaChannelFactory>,
      std::shared_ptr<transport::Connection> connection);

  // Send memory region to peer.
  void send(
      const void* ptr,
      size_t length,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback) override;

  // Receive memory region from peer.
  void recv(
      TDescriptor descriptor,
      void* ptr,
      size_t length,
      TRecvCallback callback) override;

 private:
  mutable std::mutex mutex_;
  using TLock = std::unique_lock<std::mutex>&;

  // Called by factory class after construction.
  void init_();

  // Arm connection to read next protobuf packet.
  void readPacket_();

  // Called when a protobuf packet was received.
  void onPacket_(const proto::Packet& packet, TLock lock);

  // Called when protobuf packet is a notification.
  void onNotification_(const proto::Notification& notification, TLock lock);

  // Allow factory class to call `init_()`.
  friend class CmaChannelFactory;

  std::shared_ptr<CmaChannelFactory> factory_;
  std::shared_ptr<transport::Connection> connection_;
  Error error_{Error::kSuccess};

  // Increasing identifier for send operations.
  uint64_t id_{0};

  // State capturing a single send operation.
  struct SendOperation {
    const uint64_t id;
    TSendCallback callback;
  };

  std::list<SendOperation> sendOperations_;

  // Callback types used by the transport.
  using TReadProtoCallback = transport::Connection::read_proto_callback_fn;
  using TWriteCallback = transport::Connection::write_callback_fn;

  // Callback types used in this class (in case of success).
  using TBoundReadProtoCallback = std::function<void(CmaChannel&)>;
  using TBoundWriteCallback = std::function<void(CmaChannel&)>;

  CallbackWrapper<CmaChannel> readPacketCallbackWrapper_{*this};
  CallbackWrapper<CmaChannel> writeCallbackWrapper_{*this};
  CallbackWrapper<CmaChannel> copyCallbackWrapper_{*this};

  // Helper function to process transport error.
  // Shared between read and write callback entry points.
  void handleError_(TLock lock);

  // For some odd reason it seems we need to use a qualified name here...
  template <typename T, typename... Args>
  friend class tensorpipe::CallbackWrapper;
};

} // namespace cma
} // namespace channel
} // namespace tensorpipe
