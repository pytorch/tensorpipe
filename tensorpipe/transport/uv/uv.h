/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <uv.h>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/macros.h>
#include <tensorpipe/transport/uv/sockaddr.h>

namespace tensorpipe {
namespace transport {
namespace uv {

// Libuv resources can be either a long lived handle (e.g. a socket), or a short
// lived request (e.g. connecting a socket, reading bytes off a socket, etc.).
// In either case, these resources must keep the underlying loop alive, and they
// themselves must be kept alive until completed. To do the latter all resources
// store a shared_ptr to themselves which is reset when they their lifetime is
// up and can be safely destructed (see `leak` and `unleak` functions). The loop
// on the other hand can't be destroyed until it has been joined and it can't be
// joined until all its handles or requests have been closed.
template <typename T, typename U>
class BaseResource : public std::enable_shared_from_this<T> {
 protected:
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  template <typename... Args>
  static std::shared_ptr<T> create(Loop& loop, Args&&... args) {
    auto resource = std::make_shared<T>(
        ConstructorToken(), loop, std::forward<Args>(args)...);
    resource->leak();
    return resource;
  }

  explicit BaseResource(ConstructorToken /* unused */, Loop& loop)
      : loop_(loop) {}

 protected:
  // As long as the libuv handle of this instance is open the loop won't be
  // destroyed.
  Loop& loop_;

  // Keep this instance alive by leaking it, until either:
  // * the handle is closed, or...
  // * the request has completed.
  std::shared_ptr<T> leak_;

  void leak() {
    leak_ = this->shared_from_this();
  }

  void unleak() {
    leak_.reset();
  }

  friend class Loop;
};

template <typename T, typename U>
class BaseHandle : public BaseResource<T, U> {
  static void uv__close_cb(uv_handle_t* handle) {
    T& ref = *reinterpret_cast<T*>(handle->data);
    if (ref.closeCallback_.has_value()) {
      ref.closeCallback_.value()();
    }
    ref.unleak();
  }

 public:
  using TCloseCallback = std::function<void()>;

  explicit BaseHandle(
      typename BaseResource<T, U>::ConstructorToken /* unused */,
      Loop& loop)
      : BaseResource<T, U>::BaseResource(
            typename BaseResource<T, U>::ConstructorToken(),
            loop) {
    handle_.data = this;
  }

  virtual ~BaseHandle() = default;

  U* ptr() {
    return &handle_;
  }

  void armCloseCallbackFromLoop(TCloseCallback fn) {
    TP_DCHECK(this->loop_.inLoopThread());
    TP_THROW_ASSERT_IF(closeCallback_.has_value());
    closeCallback_ = std::move(fn);
  }

  void closeFromLoop() {
    if (!uv_is_closing(reinterpret_cast<uv_handle_t*>(ptr()))) {
      uv_close(reinterpret_cast<uv_handle_t*>(ptr()), uv__close_cb);
    }
  }

 protected:
  // Underlying libuv handle.
  U handle_;

  optional<TCloseCallback> closeCallback_;
};

template <typename T, typename U>
class BaseRequest : public BaseResource<T, U> {
 public:
  explicit BaseRequest(
      typename BaseResource<T, U>::ConstructorToken /* unused */,
      Loop& loop)
      : BaseResource<T, U>::BaseResource(
            typename BaseResource<T, U>::ConstructorToken(),
            loop) {
    request_.data = this;
  }

  U* ptr() {
    return &request_;
  }

 protected:
  // Underlying libuv request.
  U request_;
};

class WriteRequest final : public BaseRequest<WriteRequest, uv_write_t> {
  static void uv__write_cb(uv_write_t* req, int status) {
    WriteRequest* request = reinterpret_cast<WriteRequest*>(req->data);
    request->writeCallback(status);
    request->unleak();
  }

 public:
  using TWriteCallback = std::function<void(int status)>;

  WriteRequest(ConstructorToken /* unused */, Loop& loop, TWriteCallback fn)
      : BaseRequest<WriteRequest, uv_write_t>(ConstructorToken(), loop),
        fn_(std::move(fn)) {}

  uv_write_cb callback() {
    return uv__write_cb;
  }

  void writeCallback(int status) {
    fn_(status);
  }

 protected:
  TWriteCallback fn_;
};

template <typename T, typename U>
class StreamHandle : public BaseHandle<T, U> {
  static void uv__connection_cb(uv_stream_t* server, int status) {
    T& ref = *reinterpret_cast<T*>(server->data);
    TP_DCHECK(ref.connectionCallback_.has_value());
    ref.connectionCallback_.value()(status);
  }

  static void uv__alloc_cb(
      uv_handle_t* handle,
      size_t /* unused */,
      uv_buf_t* buf) {
    T& ref = *reinterpret_cast<T*>(handle->data);
    TP_DCHECK(ref.allocCallback_.has_value());
    ref.allocCallback_.value()(buf);
  }

  static void uv__read_cb(
      uv_stream_t* server,
      ssize_t nread,
      const uv_buf_t* buf) {
    T& ref = *reinterpret_cast<T*>(server->data);
    TP_DCHECK(ref.readCallback_.has_value());
    ref.readCallback_.value()(nread, buf);
  }

  static constexpr int kBacklog = 128;

 public:
  using TConnectionCallback = std::function<void(int status)>;
  using TAcceptCallback = std::function<void(int status)>;
  using TAllocCallback = std::function<void(uv_buf_t* buf)>;
  using TReadCallback = std::function<void(ssize_t nread, const uv_buf_t* buf)>;

  using BaseHandle<T, U>::BaseHandle;

  ~StreamHandle() override = default;

  // TODO Split this into a armConnectionCallback, a listenStart and a
  // listenStop method, to propagate the backpressure to the clients.
  void listenFromLoop(TConnectionCallback connectionCallback) {
    TP_DCHECK(this->loop_.inLoopThread());
    TP_THROW_ASSERT_IF(connectionCallback_.has_value());
    connectionCallback_ = std::move(connectionCallback);
    auto rv = uv_listen(
        reinterpret_cast<uv_stream_t*>(this->ptr()),
        kBacklog,
        uv__connection_cb);
    TP_THROW_UV_IF(rv < 0, rv);
  }

  template <typename V>
  void acceptFromLoop(std::shared_ptr<V> other) {
    TP_DCHECK(this->loop_.inLoopThread());
    auto rv = uv_accept(
        reinterpret_cast<uv_stream_t*>(this->ptr()),
        reinterpret_cast<uv_stream_t*>(other->ptr()));
    TP_THROW_UV_IF(rv < 0, rv);
  }

  void armAllocCallbackFromLoop(TAllocCallback fn) {
    TP_DCHECK(this->loop_.inLoopThread());
    TP_THROW_ASSERT_IF(allocCallback_.has_value());
    allocCallback_ = std::move(fn);
  }

  void armReadCallbackFromLoop(TReadCallback fn) {
    TP_DCHECK(this->loop_.inLoopThread());
    TP_THROW_ASSERT_IF(readCallback_.has_value());
    readCallback_ = std::move(fn);
  }

  void readStartFromLoop() {
    TP_DCHECK(this->loop_.inLoopThread());
    TP_THROW_ASSERT_IF(!allocCallback_.has_value());
    TP_THROW_ASSERT_IF(!readCallback_.has_value());
    auto rv = uv_read_start(
        reinterpret_cast<uv_stream_t*>(this->ptr()), uv__alloc_cb, uv__read_cb);
    TP_THROW_UV_IF(rv < 0, rv);
  }

  void readStopFromLoop() {
    TP_DCHECK(this->loop_.inLoopThread());
    auto rv = uv_read_stop(reinterpret_cast<uv_stream_t*>(this->ptr()));
    TP_THROW_UV_IF(rv < 0, rv);
  }

  void writeFromLoop(
      const uv_buf_t bufs[],
      unsigned int nbufs,
      WriteRequest::TWriteCallback fn) {
    TP_DCHECK(this->loop_.inLoopThread());
    auto request = WriteRequest::create(this->loop_, std::move(fn));
    auto rv = uv_write(
        request->ptr(),
        reinterpret_cast<uv_stream_t*>(this->ptr()),
        bufs,
        nbufs,
        request->callback());
    TP_THROW_UV_IF(rv < 0, rv);
  }

 protected:
  optional<TConnectionCallback> connectionCallback_;
  optional<TAllocCallback> allocCallback_;
  optional<TReadCallback> readCallback_;
};

class ConnectRequest : public BaseRequest<ConnectRequest, uv_connect_t> {
  static void uv__connect_cb(uv_connect_t* req, int status) {
    ConnectRequest* request = reinterpret_cast<ConnectRequest*>(req->data);
    request->connectCallback(status);
    request->unleak();
  }

 public:
  using TConnectCallback = std::function<void(int status)>;

  ConnectRequest(
      BaseResource<ConnectRequest, uv_connect_t>::ConstructorToken /* unused */,
      Loop& loop,
      TConnectCallback fn)
      : BaseRequest<ConnectRequest, uv_connect_t>(
            BaseResource<ConnectRequest, uv_connect_t>::ConstructorToken(),
            loop),
        fn_(std::move(fn)) {}

  uv_connect_cb callback() {
    return uv__connect_cb;
  }

  void connectCallback(int status) {
    fn_(status);
  }

 protected:
  TConnectCallback fn_;
};

class TCPHandle : public StreamHandle<TCPHandle, uv_tcp_t> {
 public:
  using StreamHandle<TCPHandle, uv_tcp_t>::StreamHandle;

  void initFromLoop();

  [[nodiscard]] int bindFromLoop(const Sockaddr& addr);

  Sockaddr sockNameFromLoop();

  void connectFromLoop(
      const Sockaddr& addr,
      ConnectRequest::TConnectCallback fn);
};

struct AddrinfoDeleter {
  void operator()(struct addrinfo* ptr) const {
    uv_freeaddrinfo(ptr);
  }
};

using Addrinfo = std::unique_ptr<struct addrinfo, AddrinfoDeleter>;

std::tuple<int, Addrinfo> getAddrinfoFromLoop(Loop& loop, std::string hostname);

struct InterfaceAddressesDeleter {
  int count_{-1};

  explicit InterfaceAddressesDeleter(int count) : count_(count) {}

  InterfaceAddressesDeleter() = default;

  void operator()(uv_interface_address_t* ptr) const {
    uv_free_interface_addresses(ptr, count_);
  }
};

using InterfaceAddresses =
    std::unique_ptr<uv_interface_address_t[], InterfaceAddressesDeleter>;

std::tuple<int, InterfaceAddresses, int> getInterfaceAddresses();

std::tuple<int, std::string> getHostname();

std::string formatUvError(int status);

} // namespace uv
} // namespace transport
} // namespace tensorpipe
