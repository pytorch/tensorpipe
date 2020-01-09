#include <tensorpipe/transport/shm/context.h>

#include <tensorpipe/transport/shm/connection.h>
#include <tensorpipe/transport/shm/listener.h>

namespace tensorpipe {
namespace transport {
namespace shm {

Context::Context() : loop_(std::make_shared<Loop>()) {}

Context::~Context() {}

std::shared_ptr<transport::Connection> Context::connect(address_t addr) {
  auto sockaddr = Sockaddr::createAbstractUnixAddr(addr);
  auto socket = Socket::createForFamily(AF_UNIX);
  socket->connect(sockaddr);
  return std::make_shared<Connection>(loop_, std::move(socket));
}

std::shared_ptr<transport::Listener> Context::listen(address_t addr) {
  auto sockaddr = Sockaddr::createAbstractUnixAddr(addr);
  return std::make_shared<Listener>(loop_, sockaddr);
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
