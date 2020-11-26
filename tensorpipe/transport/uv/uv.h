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
#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/macros.h>
#include <tensorpipe/transport/uv/sockaddr.h>

namespace tensorpipe {
namespace transport {
namespace uv {

template <typename T, typename U>
class BaseHandle {
  static void uv__close_cb(uv_handle_t* handle) {
    T& ref = *reinterpret_cast<T*>(handle->data);
    if (ref.closeCallback_ != nullptr) {
      ref.closeCallback_();
    }
  }

 public:
  using TCloseCallback = std::function<void()>;

  explicit BaseHandle(Loop& loop) : loop_(loop) {
    handle_.data = this;
  }

  // Libuv's handles cannot be copied or moved.
  BaseHandle(const BaseHandle&) = delete;
  BaseHandle(BaseHandle&&) = delete;
  BaseHandle& operator=(const BaseHandle&) = delete;
  BaseHandle& operator=(BaseHandle&&) = delete;

  virtual ~BaseHandle() = default;

  U* ptr() {
    return &handle_;
  }

  void armCloseCallbackFromLoop(TCloseCallback fn) {
    TP_DCHECK(this->loop_.inLoop());
    TP_THROW_ASSERT_IF(closeCallback_ != nullptr);
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

  // As long as the libuv handle of this instance is open the loop won't be
  // destroyed.
  Loop& loop_;

  TCloseCallback closeCallback_;
};

template <typename T, typename U>
class BaseRequest {
 public:
  BaseRequest() {
    request_.data = this;
  }

  // Libuv's requests cannot be copied or moved.
  BaseRequest(const BaseRequest&) = delete;
  BaseRequest(BaseRequest&&) = delete;
  BaseRequest& operator=(const BaseRequest&) = delete;
  BaseRequest& operator=(BaseRequest&&) = delete;

  U* ptr() {
    return &request_;
  }

 private:
  // Underlying libuv request.
  U request_;
};

class WriteRequest final : public BaseRequest<WriteRequest, uv_write_t> {
  static void uv__write_cb(uv_write_t* req, int status) {
    std::unique_ptr<WriteRequest> request(
        reinterpret_cast<WriteRequest*>(req->data));
    request->writeCallback_(status);
  }

 public:
  using TWriteCallback = std::function<void(int status)>;

  WriteRequest(TWriteCallback fn) : writeCallback_(std::move(fn)) {}

  static int perform(
      uv_stream_t* handle,
      const uv_buf_t bufs[],
      unsigned int nbufs,
      TWriteCallback fn) {
    auto request = std::make_unique<WriteRequest>(std::move(fn));
    auto rv = uv_write(request->ptr(), handle, bufs, nbufs, uv__write_cb);
    request.release();
    return rv;
  }

 private:
  TWriteCallback writeCallback_;
};

template <typename T, typename U>
class StreamHandle : public BaseHandle<T, U> {
  static void uv__connection_cb(uv_stream_t* server, int status) {
    T& ref = *reinterpret_cast<T*>(server->data);
    TP_DCHECK(ref.connectionCallback_ != nullptr);
    ref.connectionCallback_(status);
  }

  static void uv__alloc_cb(
      uv_handle_t* handle,
      size_t /* unused */,
      uv_buf_t* buf) {
    T& ref = *reinterpret_cast<T*>(handle->data);
    TP_DCHECK(ref.allocCallback_ != nullptr);
    ref.allocCallback_(buf);
  }

  static void uv__read_cb(
      uv_stream_t* server,
      ssize_t nread,
      const uv_buf_t* buf) {
    T& ref = *reinterpret_cast<T*>(server->data);
    TP_DCHECK(ref.readCallback_ != nullptr);
    ref.readCallback_(nread, buf);
  }

  static constexpr int kBacklog = 128;

 public:
  using TConnectionCallback = std::function<void(int status)>;
  using TAcceptCallback = std::function<void(int status)>;
  using TAllocCallback = std::function<void(uv_buf_t* buf)>;
  using TReadCallback = std::function<void(ssize_t nread, const uv_buf_t* buf)>;

  using BaseHandle<T, U>::BaseHandle;

  // TODO Split this into a armConnectionCallback, a listenStart and a
  // listenStop method, to propagate the backpressure to the clients.
  void listenFromLoop(TConnectionCallback connectionCallback) {
    TP_DCHECK(this->loop_.inLoop());
    TP_THROW_ASSERT_IF(connectionCallback_ != nullptr);
    connectionCallback_ = std::move(connectionCallback);
    auto rv = uv_listen(
        reinterpret_cast<uv_stream_t*>(this->ptr()),
        kBacklog,
        uv__connection_cb);
    TP_THROW_UV_IF(rv < 0, rv);
  }

  template <typename V>
  void acceptFromLoop(V& other) {
    TP_DCHECK(this->loop_.inLoop());
    auto rv = uv_accept(
        reinterpret_cast<uv_stream_t*>(this->ptr()),
        reinterpret_cast<uv_stream_t*>(other.ptr()));
    TP_THROW_UV_IF(rv < 0, rv);
  }

  void armAllocCallbackFromLoop(TAllocCallback fn) {
    TP_DCHECK(this->loop_.inLoop());
    TP_THROW_ASSERT_IF(allocCallback_ != nullptr);
    allocCallback_ = std::move(fn);
  }

  void armReadCallbackFromLoop(TReadCallback fn) {
    TP_DCHECK(this->loop_.inLoop());
    TP_THROW_ASSERT_IF(readCallback_ != nullptr);
    readCallback_ = std::move(fn);
  }

  void readStartFromLoop() {
    TP_DCHECK(this->loop_.inLoop());
    TP_THROW_ASSERT_IF(allocCallback_ == nullptr);
    TP_THROW_ASSERT_IF(readCallback_ == nullptr);
    auto rv = uv_read_start(
        reinterpret_cast<uv_stream_t*>(this->ptr()), uv__alloc_cb, uv__read_cb);
    TP_THROW_UV_IF(rv < 0, rv);
  }

  void readStopFromLoop() {
    TP_DCHECK(this->loop_.inLoop());
    auto rv = uv_read_stop(reinterpret_cast<uv_stream_t*>(this->ptr()));
    TP_THROW_UV_IF(rv < 0, rv);
  }

  void writeFromLoop(
      const uv_buf_t bufs[],
      unsigned int nbufs,
      WriteRequest::TWriteCallback fn) {
    TP_DCHECK(this->loop_.inLoop());
    auto rv = WriteRequest::perform(
        reinterpret_cast<uv_stream_t*>(this->ptr()),
        bufs,
        nbufs,
        std::move(fn));
    TP_THROW_UV_IF(rv < 0, rv);
  }

 protected:
  TConnectionCallback connectionCallback_;
  TAllocCallback allocCallback_;
  TReadCallback readCallback_;
};

class ConnectRequest final : public BaseRequest<ConnectRequest, uv_connect_t> {
  static void uv__connect_cb(uv_connect_t* req, int status) {
    std::unique_ptr<ConnectRequest> request(
        reinterpret_cast<ConnectRequest*>(req->data));
    request->connectCallback_(status);
  }

 public:
  using TConnectCallback = std::function<void(int status)>;

  ConnectRequest(TConnectCallback fn) : connectCallback_(std::move(fn)) {}

  static int perform(
      uv_tcp_t* handle,
      const struct sockaddr* addr,
      TConnectCallback fn) {
    auto request = std::make_unique<ConnectRequest>(std::move(fn));
    auto rv = uv_tcp_connect(request->ptr(), handle, addr, uv__connect_cb);
    request.release();
    return rv;
  }

 private:
  TConnectCallback connectCallback_;
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
