#include <tensorpipe/transport/uv/context.h>

#include <tensorpipe/transport/uv/connection.h>
#include <tensorpipe/transport/uv/listener.h>

namespace tensorpipe {
namespace transport {
namespace uv {

Context::Context() : loop_(Loop::create()) {}

Context::~Context() {}

void Context::join() {
  loop_->join();
}

std::shared_ptr<transport::Connection> Context::connect(address_t addr) {
  return Connection::create(loop_, Sockaddr::createInetSockAddr(addr));
}

std::shared_ptr<transport::Listener> Context::listen(address_t addr) {
  return Listener::create(loop_, Sockaddr::createInetSockAddr(addr));
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
