/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <list>

#include <tensorpipe/core/channel.h>
#include <tensorpipe/core/error.h>
#include <tensorpipe/proto/all.pb.h>

namespace tensorpipe {

class ProcessVmReadvChannelFactory : public ChannelFactory {
 public:
  explicit ProcessVmReadvChannelFactory();

  ~ProcessVmReadvChannelFactory() override;

  const std::string& domainDescriptor() const override;

  std::shared_ptr<Channel> createChannel(
      std::shared_ptr<transport::Connection>) override;

 private:
  std::string domainDescriptor_;
};

class ProcessVmReadvChannel
    : public Channel,
      public std::enable_shared_from_this<ProcessVmReadvChannel> {
  struct ConstructorToken {};

 public:
  ProcessVmReadvChannel(
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
  void onPacket_(const proto::ProcessVmReadvChannelPacket& packet);

  // Called when protobuf packet is a notification.
  void onNotification_(
      const proto::ProcessVmReadvChannelOperation& notification);

  // Allow factory class to call `init_()`.
  friend class ProcessVmReadvChannelFactory;

 private:
  std::mutex mutex_;
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

 protected:
  // Callback types used by the transport.
  using TReadProtoCallback = transport::Connection::read_proto_callback_fn<
      proto::ProcessVmReadvChannelPacket>;
  using TWriteCallback = transport::Connection::write_callback_fn;

  // Callback types used in this class (in case of success).
  using TBoundReadProtoCallback = std::function<
      void(ProcessVmReadvChannel&, const proto::ProcessVmReadvChannelPacket&)>;
  using TBoundWriteCallback = std::function<void(ProcessVmReadvChannel&)>;

  // Generate callback to use with the underlying connection. The
  // wrapped callback ensures that the channel stays alive while it
  // is called and calls into `readCallbackEntryPoint_` such that
  // all error handling can be consolidated.
  //
  // Note: the bound callback is only called if the read from the
  // connection completed successfully.
  //

  // Read protobuf packet.
  TReadProtoCallback wrapReadProtoCallback_(
      TBoundReadProtoCallback fn = nullptr);

  // Generic write (for both bytes and protobuf packets).
  TWriteCallback wrapWriteCallback_(TBoundWriteCallback fn = nullptr);

  // Called when callback returned by `wrapReadCallback_` gets called.
  void readProtoCallbackEntryPoint_(
      const transport::Error& error,
      const proto::ProcessVmReadvChannelPacket& packet,
      TBoundReadProtoCallback fn);

  // Called when callback returned by `wrapWriteCallback_` gets called.
  void writeCallbackEntryPoint_(
      const transport::Error& error,
      TBoundWriteCallback fn);

  // Helper function to process transport error.
  // Shared between read and write callback entry points.
  bool processTransportError(const transport::Error& error);
};

} // namespace tensorpipe
