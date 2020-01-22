#include <tensorpipe/transport/uv/connection.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

std::shared_ptr<Connection> Connection::create(
    std::shared_ptr<Loop> loop,
    const Sockaddr& addr) {
  auto handle = loop->createHandle<TCPHandle>();
  handle->connect(addr);
  return std::make_shared<Connection>(
      ConstructorToken(), std::move(loop), std::move(handle));
}

std::shared_ptr<Connection> Connection::create(
    std::shared_ptr<Loop> loop,
    std::shared_ptr<TCPHandle> handle) {
  return std::make_shared<Connection>(
      ConstructorToken(), std::move(loop), std::move(handle));
}

Connection::Connection(
    ConstructorToken /* unused */,
    std::shared_ptr<Loop> loop,
    std::shared_ptr<TCPHandle> handle)
    : loop_(std::move(loop)), handle_(std::move(handle)) {}

Connection::~Connection() {
  if (handle_) {
    handle_->close();
  }
}

void Connection::read(read_callback_fn fn) {
  TP_THROW_EINVAL();
}

void Connection::read(void* ptr, size_t length, read_callback_fn fn) {
  TP_THROW_EINVAL();
}

void Connection::write(const void* ptr, size_t length, write_callback_fn fn) {
  TP_THROW_EINVAL();
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
