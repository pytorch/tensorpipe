/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <deque>
#include <type_traits>

#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace transport {

template <typename... Ts>
class MultiplexedConnection {
  using TPacket = nop::Variant<Ts...>;

  struct ReadRequest {
    // template <typename T>
    // ReadRequest(T& object, Connection::read_nop_callback_fn callback)
    //     : object(object), callback(std::move(callback)) {}

    void* ptr;
    Connection::read_nop_callback_fn callback;
  };

 public:
  MultiplexedConnection(
      std::shared_ptr<Connection> connection,
      DeferredExecutor& deferredExecutor)
      : connection_(std::move(connection)),
        deferredExecutor_(deferredExecutor) {
    readLoop();
  }

  template <typename T>
  void read(T& object, Connection::read_nop_callback_fn fn) {
    // NOTE: This is C++17
    // static_assert(std::disjunction_v<std::is_same<T, Ts>...>, "");
    deferredExecutor_.deferToLoop([this, &object, fn{std::move(fn)}]() {
      size_t packetTypeIndex = TPacket().template index_of<T>();
      pendingRequests_[packetTypeIndex].push_back(
          ReadRequest{&object, std::move(fn)});
      processPendingReadRequests(
          pendingRequests_[packetTypeIndex], pendingPackets_[packetTypeIndex]);
    });
  }

  template <typename T>
  void write(const T& object, Connection::write_callback_fn fn) {
    // NOTE: This is C++17.
    // static_assert(std::disjunction_v<std::is_same<T, Ts>...>, "");
    auto nopHolder = std::make_shared<NopHolder<TPacket>>();
    nopHolder->getObject() = object;
    connection_->write(
        *nopHolder,
        [nopHolder, fn{std::move(fn)}](const Error& error) { fn(error); });
  }

  void close() {
    connection_->close();
  }

  void join() {
    close();
    while (!error_)
      ;
  }

  ~MultiplexedConnection() {
    join();
  }

 private:
  std::shared_ptr<Connection> connection_;
  DeferredExecutor& deferredExecutor_;
  Error error_{Error::kSuccess};
  std::array<std::deque<ReadRequest>, sizeof...(Ts)> pendingRequests_;
  std::array<std::deque<TPacket>, sizeof...(Ts)> pendingPackets_;

  void readLoop() {
    auto nopHolderIn = std::make_shared<NopHolder<TPacket>>();
    connection_->read(*nopHolderIn, [this, nopHolderIn](const Error& error) {
      deferredExecutor_.deferToLoop([this, nopHolderIn, error]() {
        if (error) {
          error_ = error;
          for (auto& pendingRequests : pendingRequests_) {
            while (!pendingRequests.empty()) {
              auto& request = pendingRequests.front();
              request.callback(error);
              pendingRequests.pop_front();
            }
          }
          return;
        }
        onReadPacket(std::move(nopHolderIn->getObject()));
        readLoop();
      });
    });
  }

  void onReadPacket(TPacket packet) {
    size_t packetTypeIndex = packet.index();
    pendingPackets_[packetTypeIndex].push_back(std::move(packet));
    processPendingReadRequests(
        pendingRequests_[packetTypeIndex], pendingPackets_[packetTypeIndex]);
  }

  void processPendingReadRequests(
      std::deque<ReadRequest>& pendingRequests,
      std::deque<TPacket>& pendingPackets) {
    TP_DCHECK(deferredExecutor_.inLoop());
    while (!pendingRequests.empty()) {
      auto& request = pendingRequests.front();
      if (error_) {
          request.callback(error_);
      } else {
          if (pendingPackets.empty()) {
            break;
          }
          auto& packet = pendingPackets.front();
          TP_DCHECK(!packet.empty());
          nop::IfAnyOf<Ts...>::Call(&packet, [&request](auto& value) {
            *static_cast<std::remove_reference_t<decltype(value)>*>(
                request.ptr) = std::move(value);
          });

          request.callback(Error::kSuccess);
          pendingPackets.pop_front();
      }
      pendingRequests.pop_front();
    }
  }
};

} // namespace transport
} // namespace tensorpipe
