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
#include <tensorpipe/channel/cma/context.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/proto/channel/cma.pb.h>

namespace tensorpipe {
namespace channel {
namespace cma {

class Channel : public channel::Channel {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  Channel(
      ConstructorToken,
      std::shared_ptr<Context::PrivateIface>,
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

  void close() override;

  ~Channel() override;

 private:
  class Impl : public std::enable_shared_from_this<Impl> {
    // Use the passkey idiom to allow make_shared to call what should be a
    // private constructor. See https://abseil.io/tips/134 for more information.
    struct ConstructorToken {};

   public:
    static std::shared_ptr<Impl> create(
        std::shared_ptr<Context::PrivateIface>,
        std::shared_ptr<transport::Connection>);

    Impl(
        ConstructorToken,
        std::shared_ptr<Context::PrivateIface>,
        std::shared_ptr<transport::Connection>);

    void send(
        const void* ptr,
        size_t length,
        TDescriptorCallback descriptorCallback,
        TSendCallback callback);

    void recv(
        TDescriptor descriptor,
        void* ptr,
        size_t length,
        TRecvCallback callback);

    void close();

   private:
    std::mutex mutex_;
    std::thread::id currentLoop_{std::thread::id()};
    std::deque<std::function<void()>> pendingTasks_;

    bool inLoop_();
    void deferToLoop_(std::function<void()> fn);

    // Called by context class after construction.
    void init_();
    void initFromLoop_();

    // Send memory region to peer.
    void sendFromLoop_(
        const void* ptr,
        size_t length,
        TDescriptorCallback descriptorCallback,
        TSendCallback callback);

    // Receive memory region from peer.
    void recvFromLoop_(
        TDescriptor descriptor,
        void* ptr,
        size_t length,
        TRecvCallback callback);

    void closeFromLoop_();

    // Arm connection to read next protobuf packet.
    void readPacket_();

    // Called when a protobuf packet was received.
    void onPacket_(const proto::Packet& packet);

    // Called when protobuf packet is a notification.
    void onNotification_(const proto::Notification& notification);

    std::shared_ptr<Context::PrivateIface> context_;
    std::shared_ptr<transport::Connection> connection_;
    Error error_{Error::kSuccess};
    ClosingReceiver closingReceiver_;

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
    using TBoundReadProtoCallback = std::function<void(Channel&)>;
    using TBoundWriteCallback = std::function<void(Channel&)>;

    DeferringCallbackWrapper<Impl> readPacketCallbackWrapper_{*this};
    DeferringCallbackWrapper<Impl> writePacketCallbackWrapper_{*this};
    DeferringTolerantCallbackWrapper<Impl> copyCallbackWrapper_{*this};

    // Helper function to process transport error.
    // Shared between read and write callback entry points.
    void handleError_();

    // For some odd reason it seems we need to use a qualified name here...
    template <typename T, typename... Args>
    friend class tensorpipe::DeferringCallbackWrapper;
    template <typename T, typename... Args>
    friend class tensorpipe::DeferringTolerantCallbackWrapper;
  };

  // Using a shared_ptr allows us to detach the lifetime of the implementation
  // from the public object's one and perform the destruction asynchronously.
  std::shared_ptr<Impl> impl_;

  // Allow context class to call `init_()`.
  friend class Context;
};

} // namespace cma
} // namespace channel
} // namespace tensorpipe
