#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/shm/socket.h>

#include <gtest/gtest.h>

using namespace tensorpipe::transport;

TEST(Sockaddr, FromToString) {
  auto addr = shm::Sockaddr::createAbstractUnixAddr("foo");
  ASSERT_EQ(addr.str(), std::string("foo"));
}
