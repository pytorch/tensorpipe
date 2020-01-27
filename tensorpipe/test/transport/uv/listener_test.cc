#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/uv/listener.h>
#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/uv.h>

#include <gtest/gtest.h>

using namespace tensorpipe::transport;

TEST(Listener, Basics) {
  auto loop = uv::Loop::create();
  auto addr = uv::Sockaddr::createInetSockAddr("::1");

  {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<std::shared_ptr<Connection>> connections;

    // Listener runs callback for every new connection.
    auto listener = uv::Listener::create(loop, addr);
    listener->accept([&](std::shared_ptr<Connection> connection) {
      std::lock_guard<std::mutex> lock(mutex);
      connections.push_back(std::move(connection));
      cv.notify_one();
    });

    // Connect to listener.
    auto socket = loop->createHandle<uv::TCPHandle>();
    socket->connect(listener->sockaddr());
    socket->close();

    // Wait for new connection
    {
      std::unique_lock<std::mutex> lock(mutex);
      while (connections.empty()) {
        cv.wait(lock);
      }
    }
  }

  loop->join();
}
