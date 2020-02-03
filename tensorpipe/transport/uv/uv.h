#pragma once

#include <memory>

#include <uv.h>

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
// themselves must be kept alive until completed. To do so, all resources store
// a shared_ptr to the loop they are associated with, and a shared_ptr to
// themselves. The shared_ptr to themselves is reset when they their lifetime is
// up and can be safely destructed (see `leak` and `unleak` functions).
template <typename T, typename U>
class BaseResource : public std::enable_shared_from_this<T> {
 public:
  explicit BaseResource(std::shared_ptr<Loop> loop) : loop_(std::move(loop)) {}

 protected:
  // Refer to the loop to keep it alive.
  std::shared_ptr<Loop> loop_;

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
    ref.unleak();
  }

 public:
  explicit BaseHandle(std::shared_ptr<Loop> loop)
      : BaseResource<T, U>::BaseResource(std::move(loop)) {
    handle_.data = this;
  }

  virtual ~BaseHandle() = default;

  U* ptr() {
    return &handle_;
  }

  virtual void close() {
    this->loop_->run([&] {
      uv_close(reinterpret_cast<uv_handle_t*>(&handle_), uv__close_cb);
    });
  }

 protected:
  // Underlying libuv handle.
  U handle_;
};

template <typename T, typename U>
class BaseRequest : public BaseResource<T, U> {
 public:
  explicit BaseRequest(std::shared_ptr<Loop> loop)
      : BaseResource<T, U>::BaseResource(std::move(loop)) {
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

  WriteRequest(std::shared_ptr<Loop> loop, TWriteCallback fn)
      : BaseRequest<WriteRequest, uv_write_t>(std::move(loop)),
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
  using TAllocCallback = std::function<void(uv_buf_t* buf)>;
  using TReadCallback = std::function<void(ssize_t nread, const uv_buf_t* buf)>;

  using BaseHandle<T, U>::BaseHandle;

  ~StreamHandle() override = default;

  // TODO Split this into a armConnectionCallback, a listenStart and a
  // listenStop method, to propagate the backpressure to the clients.
  void listen(TConnectionCallback connectionCallback) {
    TP_THROW_ASSERT_IF(connectionCallback_.has_value());
    connectionCallback_ = std::move(connectionCallback);

    this->loop_->run([&] {
      auto rv = uv_listen(
          reinterpret_cast<uv_stream_t*>(this->ptr()),
          kBacklog,
          uv__connection_cb);
      TP_THROW_UV_IF(rv < 0, rv);
    });
  }

  template <typename V>
  void accept(std::shared_ptr<V> other) {
    this->loop_->run([&] {
      auto rv = uv_accept(
          reinterpret_cast<uv_stream_t*>(this->ptr()),
          reinterpret_cast<uv_stream_t*>(other->ptr()));
      TP_THROW_UV_IF(rv < 0, rv);
    });
  }

  void armAllocCallback(TAllocCallback fn) {
    TP_THROW_ASSERT_IF(allocCallback_.has_value());
    allocCallback_ = std::move(fn);
  }

  void armReadCallback(TReadCallback fn) {
    TP_THROW_ASSERT_IF(readCallback_.has_value());
    readCallback_ = std::move(fn);
  }

  void readStart() {
    TP_THROW_ASSERT_IF(!allocCallback_.has_value());
    TP_THROW_ASSERT_IF(!readCallback_.has_value());
    this->loop_->run([&] {
      auto rv = uv_read_start(
          reinterpret_cast<uv_stream_t*>(this->ptr()),
          uv__alloc_cb,
          uv__read_cb);
      TP_THROW_UV_IF(rv < 0, rv);
    });
  }

  void readStop() {
    this->loop_->run([&] {
      auto rv = uv_read_stop(reinterpret_cast<uv_stream_t*>(this->ptr()));
      TP_THROW_UV_IF(rv < 0, rv);
    });
  }

  void write(
      const uv_buf_t bufs[],
      unsigned int nbufs,
      WriteRequest::TWriteCallback fn) {
    auto request =
        this->loop_->template createRequest<WriteRequest>(std::move(fn));
    this->loop_->run([&] {
      auto rv = uv_write(
          request->ptr(),
          reinterpret_cast<uv_stream_t*>(this->ptr()),
          bufs,
          nbufs,
          request->callback());
      TP_THROW_UV_IF(rv < 0, rv);
    });
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

  ConnectRequest(std::shared_ptr<Loop> loop, TConnectCallback fn)
      : BaseRequest<ConnectRequest, uv_connect_t>(std::move(loop)),
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

  void init();

  void noDelay(bool enable);

  void bind(const Sockaddr& addr);

  Sockaddr sockName();

  Sockaddr peerName();

  void connect(const Sockaddr& addr);

  void connect(const Sockaddr& addr, ConnectRequest::TConnectCallback fn);
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
