#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/shm/listener.h>
#include <tensorpipe/transport/shm/loop.h>

#include <gtest/gtest.h>

using namespace tensorpipe::transport::shm;

namespace test {
namespace transport {
namespace shm {

TEST(Listener, Basics) {
  auto loop = std::make_shared<Loop>();
  auto listener = std::make_shared<Listener>(loop, "foobar");
  auto socket = Socket::createForFamily(AF_UNIX);
  socket->connect(Sockaddr::createAbstractUnixAddr("foobar"));
}

} // namespace shm
} // namespace transport
} // namespace test
