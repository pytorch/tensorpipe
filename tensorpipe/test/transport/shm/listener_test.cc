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
  auto addr = Sockaddr::createAbstractUnixAddr("foobar");

  std::mutex mutex;
  std::condition_variable cv;
  std::vector<std::shared_ptr<Connection>> connections;

  // Listener runs callback for every new connection.
  auto listener = std::make_shared<Listener>(
      loop, addr, [&](std::shared_ptr<Connection> connection) {
        std::lock_guard<std::mutex> lock(mutex);
        connections.push_back(std::move(connection));
        cv.notify_one();
      });

  // Connect to listener.
  auto socket = Socket::createForFamily(AF_UNIX);
  socket->connect(addr);

  // Wait for new connection
  {
    std::unique_lock<std::mutex> lock(mutex);
    while (connections.empty()) {
      cv.wait(lock);
    }
  }
}

} // namespace shm
} // namespace transport
} // namespace test
