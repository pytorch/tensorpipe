/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <memory>

#include <uv.h>

#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/uv/sockaddr.h>

#define TP_THROW_UV(err) TP_THROW(std::runtime_error)
#define TP_THROW_UV_IF(cond, err) \
  if (unlikely(cond))             \
  TP_THROW_UV(err) << TP_STRINGIFY(cond) << ": " << uv_strerror(err)

namespace tensorpipe {
namespace transport {
namespace uv {

template <typename T, typename U>
class BaseHandle {
  static void uvCloseCb(uv_handle_t* handle) {
    T& ref = *reinterpret_cast<T*>(handle->data);
    if (ref.closeCallback_ != nullptr) {
      ref.closeCallback_();
    }
  }

 public:
  using TCloseCallback = std::function<void()>;

  explicit BaseHandle(uv_loop_t* loop, const DeferredExecutor& executor)
      : loop_(loop), executor_(executor) {
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
    TP_DCHECK(this->executor_.inLoop());
    TP_THROW_ASSERT_IF(closeCallback_ != nullptr);
    closeCallback_ = std::move(fn);
  }

  void closeFromLoop() {
    TP_DCHECK(!uv_is_closing(reinterpret_cast<uv_handle_t*>(ptr())));
    uv_close(reinterpret_cast<uv_handle_t*>(ptr()), uvCloseCb);
  }

 protected:
  // Underlying libuv handle.
  U handle_;

  // Underlying libuv event loop.
  uv_loop_t* const loop_;

  // This DeferredExecutor is only used to check that all calls are performed
  // from the right thread.
  const DeferredExecutor& executor_;

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
  static void uvWriteCb(uv_write_t* req, int status) {
    std::unique_ptr<WriteRequest> request(
        reinterpret_cast<WriteRequest*>(req->data));
    request->writeCallback_(status);
  }

 public:
  using TWriteCallback = std::function<void(int status)>;

  explicit WriteRequest(TWriteCallback fn) : writeCallback_(std::move(fn)) {}

  static int perform(
      uv_stream_t* handle,
      const uv_buf_t bufs[],
      unsigned int nbufs,
      TWriteCallback fn) {
    auto request = std::make_unique<WriteRequest>(std::move(fn));
    auto rv = uv_write(request->ptr(), handle, bufs, nbufs, uvWriteCb);
    request.release();
    return rv;
  }

 private:
  TWriteCallback writeCallback_;
};

template <typename T, typename U>
class StreamHandle : public BaseHandle<T, U> {
  static void uvConnectionCb(uv_stream_t* server, int status) {
    T& ref = *reinterpret_cast<T*>(server->data);
    TP_DCHECK(ref.connectionCallback_ != nullptr);
    ref.connectionCallback_(status);
  }

  static void uvAllocCb(
      uv_handle_t* handle,
      size_t /* unused */,
      uv_buf_t* buf) {
    T& ref = *reinterpret_cast<T*>(handle->data);
    TP_DCHECK(ref.allocCallback_ != nullptr);
    ref.allocCallback_(buf);
  }

  static void uvReadCb(
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
    TP_DCHECK(this->executor_.inLoop());
    TP_THROW_ASSERT_IF(connectionCallback_ != nullptr);
    connectionCallback_ = std::move(connectionCallback);
    auto rv = uv_listen(
        reinterpret_cast<uv_stream_t*>(this->ptr()), kBacklog, uvConnectionCb);
    TP_THROW_UV_IF(rv < 0, rv);
  }

  template <typename V>
  void acceptFromLoop(V& other) {
    TP_DCHECK(this->executor_.inLoop());
    auto rv = uv_accept(
        reinterpret_cast<uv_stream_t*>(this->ptr()),
        reinterpret_cast<uv_stream_t*>(other.ptr()));
    TP_THROW_UV_IF(rv < 0, rv);
  }

  void armAllocCallbackFromLoop(TAllocCallback fn) {
    TP_DCHECK(this->executor_.inLoop());
    TP_THROW_ASSERT_IF(allocCallback_ != nullptr);
    allocCallback_ = std::move(fn);
  }

  void armReadCallbackFromLoop(TReadCallback fn) {
    TP_DCHECK(this->executor_.inLoop());
    TP_THROW_ASSERT_IF(readCallback_ != nullptr);
    readCallback_ = std::move(fn);
  }

  void readStartFromLoop() {
    TP_DCHECK(this->executor_.inLoop());
    TP_THROW_ASSERT_IF(allocCallback_ == nullptr);
    TP_THROW_ASSERT_IF(readCallback_ == nullptr);
    auto rv = uv_read_start(
        reinterpret_cast<uv_stream_t*>(this->ptr()), uvAllocCb, uvReadCb);
    TP_THROW_UV_IF(rv < 0, rv);
  }

  void readStopFromLoop() {
    TP_DCHECK(this->executor_.inLoop());
    auto rv = uv_read_stop(reinterpret_cast<uv_stream_t*>(this->ptr()));
    TP_THROW_UV_IF(rv < 0, rv);
  }

  void writeFromLoop(
      const uv_buf_t bufs[],
      unsigned int nbufs,
      WriteRequest::TWriteCallback fn) {
    TP_DCHECK(this->executor_.inLoop());
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
  static void uvConnectCb(uv_connect_t* req, int status) {
    std::unique_ptr<ConnectRequest> request(
        reinterpret_cast<ConnectRequest*>(req->data));
    request->connectCallback_(status);
  }

 public:
  using TConnectCallback = std::function<void(int status)>;

  explicit ConnectRequest(TConnectCallback fn)
      : connectCallback_(std::move(fn)) {}

  static int perform(
      uv_tcp_t* handle,
      const struct sockaddr* addr,
      TConnectCallback fn) {
    auto request = std::make_unique<ConnectRequest>(std::move(fn));
    auto rv = uv_tcp_connect(request->ptr(), handle, addr, uvConnectCb);
    request.release();
    return rv;
  }

 private:
  TConnectCallback connectCallback_;
};

class TCPHandle : public StreamHandle<TCPHandle, uv_tcp_t> {
 public:
  using StreamHandle<TCPHandle, uv_tcp_t>::StreamHandle;

  void initFromLoop() {
    TP_DCHECK(this->executor_.inLoop());
    int rv;
    rv = uv_tcp_init(loop_, this->ptr());
    TP_THROW_UV_IF(rv < 0, rv);
    rv = uv_tcp_nodelay(this->ptr(), 1);
    TP_THROW_UV_IF(rv < 0, rv);
  }

  [[nodiscard]] int bindFromLoop(const Sockaddr& addr) {
    TP_DCHECK(this->executor_.inLoop());
    auto rv = uv_tcp_bind(ptr(), addr.addr(), 0);
    // We don't throw in case of errors here because sometimes we bind in order
    // to try if an address works and want to handle errors gracefully.
    return rv;
  }

  Sockaddr sockNameFromLoop() {
    TP_DCHECK(this->executor_.inLoop());
    struct sockaddr_storage ss;
    struct sockaddr* addr = reinterpret_cast<struct sockaddr*>(&ss);
    int addrlen = sizeof(ss);
    auto rv = uv_tcp_getsockname(ptr(), addr, &addrlen);
    TP_THROW_UV_IF(rv < 0, rv);
    return Sockaddr(addr, addrlen);
  }

  void connectFromLoop(
      const Sockaddr& addr,
      ConnectRequest::TConnectCallback fn) {
    TP_DCHECK(this->executor_.inLoop());
    auto rv = ConnectRequest::perform(ptr(), addr.addr(), std::move(fn));
    TP_THROW_UV_IF(rv < 0, rv);
  }
};

struct AddrinfoDeleter {
  void operator()(struct addrinfo* ptr) const {
    uv_freeaddrinfo(ptr);
  }
};

using Addrinfo = std::unique_ptr<struct addrinfo, AddrinfoDeleter>;

inline std::tuple<int, Addrinfo> getAddrinfoFromLoop(
    uv_loop_t* loop,
    std::string hostname) {
  struct addrinfo hints;
  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  uv_getaddrinfo_t request;
  // Don't use a callback, and thus perform the call synchronously, because the
  // asynchronous version uses a thread pool, and it's not worth spawning new
  // threads for a functionality which is used so sparingly.
  auto rv = uv_getaddrinfo(
      loop,
      &request,
      /*getaddrinfo_cb=*/nullptr,
      hostname.c_str(),
      /*service=*/nullptr,
      &hints);
  if (rv != 0) {
    return std::make_tuple(rv, Addrinfo());
  }

  return std::make_tuple(0, Addrinfo(request.addrinfo, AddrinfoDeleter()));
}

struct InterfaceAddressesDeleter {
  explicit InterfaceAddressesDeleter(int count) : count_(count) {}

  InterfaceAddressesDeleter() = default;

  void operator()(uv_interface_address_t* ptr) const {
    uv_free_interface_addresses(ptr, count_);
  }

 private:
  int count_{-1};
};

using InterfaceAddresses =
    std::unique_ptr<uv_interface_address_t[], InterfaceAddressesDeleter>;

inline std::tuple<int, InterfaceAddresses, int> getInterfaceAddresses() {
  uv_interface_address_t* info;
  int count;
  auto rv = uv_interface_addresses(&info, &count);
  if (rv != 0) {
    return std::make_tuple(rv, InterfaceAddresses(), 0);
  }
  return std::make_tuple(
      0, InterfaceAddresses(info, InterfaceAddressesDeleter(count)), count);
}

inline std::tuple<int, std::string> getHostname() {
  std::array<char, UV_MAXHOSTNAMESIZE> hostname;
  size_t size = hostname.size();
  auto rv = uv_os_gethostname(hostname.data(), &size);
  if (rv != 0) {
    return std::make_tuple(rv, std::string());
  }
  return std::make_tuple(
      0, std::string(hostname.data(), hostname.data() + size));
}

inline std::string formatUvError(int status) {
  if (status == 0) {
    return "success";
  } else {
    std::ostringstream ss;
    ss << uv_err_name(status) << ": " << uv_strerror(status);
    return ss.str();
  }
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
