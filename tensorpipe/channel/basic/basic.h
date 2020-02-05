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
#include <tensorpipe/proto/all.pb.h>
#include <tensorpipe/transport/error.h>

namespace tensorpipe {
namespace channel {
namespace basic {

class BasicChannelFactory : public ChannelFactory {
 public:
  explicit BasicChannelFactory();

  ~BasicChannelFactory() override;

  const std::string& domainDescriptor() const override;

  std::shared_ptr<Channel> createChannel(
      std::shared_ptr<transport::Connection>) override;

 private:
  std::string domainDescriptor_;
};

class BasicChannel : public Channel,
                     public std::enable_shared_from_this<BasicChannel> {
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
  // Called by factory class after construction.
  void init_();

  // Arm connection to read next protobuf packet.
  void readPacket_();

  // Called when a protobuf packet was received.
  void onPacket_(const proto::BasicChannelPacket& packet);

  // Called when protobuf packet is a request.
  void onRequest_(const proto::BasicChannelOperation& request);

  // Called when protobuf packet is a reply.
  void onReply_(const proto::BasicChannelOperation& reply);

  // Allow factory class to call `init_()`.
  friend class BasicChannelFactory;

 private:
  std::mutex mutex_;
  std::shared_ptr<transport::Connection> connection_;
  transport::Error error_{transport::Error::kSuccess};

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
  void sendCompleted(const uint64_t);

  // Called if recv operation was successful.
  void recvCompleted(const uint64_t);

 protected:
  // Callback types used by the transport.
  using TReadCallback = transport::Connection::read_callback_fn;
  using TReadProtoCallback =
      transport::Connection::read_proto_callback_fn<proto::BasicChannelPacket>;
  using TWriteCallback = transport::Connection::write_callback_fn;

  // Callback types used in this class (in case of success).
  using TBoundReadCallback =
      std::function<void(BasicChannel&, const void*, size_t)>;
  using TBoundReadProtoCallback =
      std::function<void(BasicChannel&, const proto::BasicChannelPacket&)>;
  using TBoundWriteCallback = std::function<void(BasicChannel&)>;

  // Generate callback to use with the underlying connection. The
  // wrapped callback ensures that the channel stays alive while it
  // is called and calls into `readCallbackEntryPoint_` such that
  // all error handling can be consolidated.
  //
  // Note: the bound callback is only called if the read from the
  // connection completed successfully.
  //

  // Read bytes.
  TReadCallback wrapReadCallback_(TBoundReadCallback fn = nullptr);

  // Read protobuf packet.
  TReadProtoCallback wrapReadProtoCallback_(
      TBoundReadProtoCallback fn = nullptr);

  // Generic write (for both bytes and protobuf packets).
  TWriteCallback wrapWriteCallback_(TBoundWriteCallback fn = nullptr);

  // Called when callback returned by `wrapReadCallback_` gets called.
  void readCallbackEntryPoint_(
      const transport::Error& error,
      const void* ptr,
      size_t length,
      TBoundReadCallback fn);

  // Called when callback returned by `wrapReadCallback_` gets called.
  void readProtoCallbackEntryPoint_(
      const transport::Error& error,
      const proto::BasicChannelPacket& packet,
      TBoundReadProtoCallback fn);

  // Called when callback returned by `wrapWriteCallback_` gets called.
  void writeCallbackEntryPoint_(
      const transport::Error& error,
      TBoundWriteCallback fn);

  // Helper function to process transport error.
  // Shared between read and write callback entry points.
  bool processTransportError(const transport::Error& error);
};

} // namespace basic
} // namespace channel
} // namespace tensorpipe
