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
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/proto/channel/intra_process.pb.h>

namespace tensorpipe {
namespace channel {
namespace intra_process {

class IntraProcessChannel
    : public Channel,
      public std::enable_shared_from_this<IntraProcessChannel> {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  IntraProcessChannel(
      ConstructorToken,
      std::shared_ptr<IntraProcessChannelFactory> factory,
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
  // Allow factory class to call `init_()`.
  friend class IntraProcessChannelFactory;
  // Called by factory class after construction.
  void init_();

  // Arm connection to read next protobuf packet.
  void readPacket_();

  // Called when a protobuf packet was received.
  void onPacket_(const proto::Packet& packet);

  // Called when protobuf packet is a notification.
  void onNotification_(const proto::Notification& notification);

  std::mutex mutex_;
  std::shared_ptr<IntraProcessChannelFactory> factory_;
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
  using TReadProtoCallback =
      transport::Connection::read_proto_callback_fn<proto::Packet>;
  using TWriteCallback = transport::Connection::write_callback_fn;

  // Callback types used in this class (in case of success).
  using TBoundReadProtoCallback =
      std::function<void(ProcessVmReadvChannel&, const proto::Packet&)>;
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
      const Error& error,
      const proto::Packet& pbPacketIn,
      TBoundReadProtoCallback fn);

  // Called when callback returned by `wrapWriteCallback_` gets called.
  void writeCallbackEntryPoint_(const Error& error, TBoundWriteCallback fn);

  // Helper function to process transport error.
  // Shared between read and write callback entry points.
  bool processError(const Error& error);
};

class IntraProcessChannelFactory
    : public ChannelFactory,
      public std::enable_shared_from_this<IntraProcessChannelFactory> {
 public:
  explicit IntraProcessChannelFactory();

  ~IntraProcessChannelFactory() override;

  const std::string& domainDescriptor() const override;

  std::shared_ptr<Channel> createChannel(
      std::shared_ptr<transport::Connection>,
      Channel::Endpoint) override;

  void join() override;

 private:
  using copy_request_callback_fn = std::function<void(const Error&)>;

  struct CopyRequest {
    void* remotePtr;
    void* localPtr;
    size_t length;
    copy_request_callback_fn callback;
  };

  std::string domainDescriptor_;
  std::thread thread_;
  Queue<optional<CopyRequest>> requests_;
  std::atomic<bool> joined_{false};

  void requestCopy_(
      void* remotePtr,
      void* localPtr,
      size_t length,
      copy_request_callback_fn fn);

  void handleCopyRequests_();

  friend class IntraProcessChannel;
};

} // namespace intra_process
} // namespace channel
} // namespace tensorpipe