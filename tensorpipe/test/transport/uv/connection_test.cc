#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/uv/connection.h>
#include <tensorpipe/transport/uv/listener.h>
#include <tensorpipe/transport/uv/loop.h>

#include <gtest/gtest.h>

using namespace tensorpipe;
using namespace tensorpipe::transport;

namespace {

void initializePeers(
    std::shared_ptr<uv::Loop> loop,
    uv::Sockaddr addr,
    std::function<void(std::shared_ptr<Connection>)> listeningFn,
    std::function<void(std::shared_ptr<Connection>)> connectingFn) {
  Queue<std::shared_ptr<Connection>> queue;

  // Listener runs callback for every new connection.
  // We only care about a single one for tests.
  auto listener = uv::Listener::create(loop, addr);
  listener->accept(
      [&](std::shared_ptr<Connection> conn) { queue.push(std::move(conn)); });

  // Capture real listener address.
  auto listenerAddr = listener->addr();

  // Start thread for listening side.
  std::thread listeningThread([&]() { listeningFn(queue.pop()); });

  // Start thread for connecting side.
  std::thread connectingThread(
      [&]() { connectingFn(uv::Connection::create(loop, listenerAddr)); });

  // Wait for completion.
  listeningThread.join();
  connectingThread.join();
}

} // namespace

TEST(Connection, Initialization) {
  auto loop = uv::Loop::create();
  auto addr = uv::Sockaddr::createInetSockAddr("::1");
  constexpr size_t numBytes = 13;

  initializePeers(
      loop,
      addr,
      [&](std::shared_ptr<Connection> conn) {
        Queue<size_t> reads;
        conn->read([&](const Error& error, const void* ptr, size_t len) {
          ASSERT_FALSE(error) << error.what();
          reads.push(len);
        });
        // Wait for the read callback to be called.
        ASSERT_EQ(numBytes, reads.pop());
      },
      [&](std::shared_ptr<Connection> conn) {
        Queue<bool> writes;
        std::array<char, numBytes> garbage;
        conn->write(garbage.data(), garbage.size(), [&](const Error& error) {
          ASSERT_FALSE(error) << error.what();
          writes.push(true);
        });
        // Wait for the write callback to be called.
        writes.pop();
      });

  loop->join();
}

TEST(Connection, InitializationError) {
  auto loop = uv::Loop::create();
  auto addr = uv::Sockaddr::createInetSockAddr("::1");

  initializePeers(
      loop,
      addr,
      [&](std::shared_ptr<Connection> conn) {
        // Closes connection
      },
      [&](std::shared_ptr<Connection> conn) {
        Queue<Error> errors;
        conn->read([&](const Error& error, const void* ptr, size_t len) {
          errors.push(error);
        });
        auto error = errors.pop();
        ASSERT_TRUE(error);
      });

  loop->join();
}
