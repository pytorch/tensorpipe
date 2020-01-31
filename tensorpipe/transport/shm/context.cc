#include <tensorpipe/transport/shm/context.h>

#include <tensorpipe/common/system.h>
#include <tensorpipe/transport/shm/connection.h>
#include <tensorpipe/transport/shm/listener.h>

namespace tensorpipe {
namespace transport {
namespace shm {

namespace {

// Prepend descriptor with transport name so it's easy to
// disambiguate descriptors when debugging.
const std::string kDomainDescriptorPrefix{"shm:"};

std::string generateDomainDescriptor() {
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID) << "Unable to read boot_id";
  return kDomainDescriptorPrefix + bootID.value();
}

} // namespace

Context::Context()
    : loop_(Loop::create()), domainDescriptor_(generateDomainDescriptor()) {}

Context::~Context() {}

void Context::join() {
  loop_->join();
}

std::shared_ptr<transport::Connection> Context::connect(address_t addr) {
  auto sockaddr = Sockaddr::createAbstractUnixAddr(addr);
  auto socket = Socket::createForFamily(AF_UNIX);
  socket->connect(sockaddr);
  return Connection::create(loop_, std::move(socket));
}

std::shared_ptr<transport::Listener> Context::listen(address_t addr) {
  auto sockaddr = Sockaddr::createAbstractUnixAddr(addr);
  return Listener::create(loop_, sockaddr);
}

const std::string& Context::domainDescriptor() const {
  return domainDescriptor_;
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
