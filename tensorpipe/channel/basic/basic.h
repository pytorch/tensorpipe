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

class BasicChannelFactory
    : public ChannelFactory,
      public std::enable_shared_from_this<BasicChannelFactory> {
 public:
  explicit BasicChannelFactory();

  const std::string& domainDescriptor() const override;

  std::shared_ptr<Channel> createChannel(
      std::shared_ptr<transport::Connection>,
      Channel::Endpoint) override;

  void close() override;

  void join() override;

  ~BasicChannelFactory() override;

 private:
  std::string domainDescriptor_;
  ClosingEmitter closingEmitter_;

  friend class BasicChannel;
};

class BasicChannel : public Channel {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  BasicChannel(
      ConstructorToken,
      std::shared_ptr<BasicChannelFactory> factory,
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

  ~BasicChannel() override;

 private:
  class Impl : public std::enable_shared_from_this<Impl> {
    // Use the passkey idiom to allow make_shared to call what should be a
    // private constructor. See https://abseil.io/tips/134 for more information.
    struct ConstructorToken {};

   public:
    static std::shared_ptr<Impl> create(
        std::shared_ptr<BasicChannelFactory>,
        std::shared_ptr<transport::Connection>);

    Impl(
        ConstructorToken,
        std::shared_ptr<BasicChannelFactory>,
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

    void initFromLoop_();

    void closeFromLoop_();

    // Called by factory class after construction.
    void init_();

    // Arm connection to read next protobuf packet.
    void readPacket_();

    // Called when a protobuf packet was received.
    void onPacket_(const proto::Packet& packet);

    // Called when protobuf packet is a request.
    void onRequest_(const proto::Request& request);

    // Called when protobuf packet is a reply.
    void onReply_(const proto::Reply& reply);

    std::shared_ptr<BasicChannelFactory> factory_;
    std::shared_ptr<transport::Connection> connection_;
    Error error_{Error::kSuccess};
    ClosingReceiver closingReceiver_;

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

    // Helpers to prepare callbacks from transports
    DeferringTolerantCallbackWrapper<Impl, const void*, size_t>
        readCallbackWrapper_;
    DeferringCallbackWrapper<Impl> readProtoCallbackWrapper_;
    DeferringTolerantCallbackWrapper<Impl> writeCallbackWrapper_;
    DeferringCallbackWrapper<Impl> writeProtoCallbackWrapper_;

    // Helper function to process transport error.
    // Shared between read and write callback entry points.
    void handleError_();

    // For some odd reason it seems we need to use a qualified name here...
    template <typename T, typename... Args>
    friend class tensorpipe::DeferringCallbackWrapper;
    template <typename T, typename... Args>
    friend class tensorpipe::DeferringTolerantCallbackWrapper;
  };

  std::shared_ptr<Impl> impl_;

  // Allow factory class to call constructor.
  friend class BasicChannelFactory;
};

} // namespace basic
} // namespace channel
} // namespace tensorpipe
