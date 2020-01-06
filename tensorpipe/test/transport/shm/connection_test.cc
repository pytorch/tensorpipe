#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/shm/connection.h>
#include <tensorpipe/transport/shm/listener.h>
#include <tensorpipe/transport/shm/loop.h>

#include <gtest/gtest.h>

using namespace tensorpipe::transport::shm;

using tensorpipe::transport::Error;

namespace test {
namespace transport {
namespace shm {

namespace {

template <typename T>
class Queue {
 public:
  explicit Queue(int capacity = 1) : capacity_(capacity) {}

  void push(T t) {
    std::unique_lock<std::mutex> lock(mutex_);
    while (items_.size() >= capacity_) {
      cv_.wait(lock);
    }
    items_.push_back(std::move(t));
    cv_.notify_all();
  }

  T pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (items_.size() == 0) {
      cv_.wait(lock);
    }
    T t(std::move(items_.front()));
    items_.pop_front();
    cv_.notify_all();
    return t;
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  const int capacity_;
  std::deque<T> items_;
};

void initializePeers(
    std::shared_ptr<Loop> loop,
    Sockaddr addr,
    std::function<void(std::shared_ptr<Connection>)> listeningFn,
    std::function<void(std::shared_ptr<Connection>)> connectingFn) {
  Queue<std::shared_ptr<Connection>> queue;

  // Listener runs callback for every new connection.
  // We only care about a single one for tests.
  auto listener = std::make_shared<Listener>(
      loop, addr, [&](std::shared_ptr<Connection> conn) {
        queue.push(std::move(conn));
      });

  // Start thread for listening side.
  std::thread listeningThread([&]() { listeningFn(queue.pop()); });

  // Start thread for connecting side.
  std::thread connectingThread([&]() {
    auto socket = Socket::createForFamily(AF_UNIX);
    socket->connect(addr);
    connectingFn(std::make_shared<Connection>(loop, std::move(socket)));
  });

  // Wait for completion.
  listeningThread.join();
  connectingThread.join();
}

} // namespace

TEST(Connection, Initialization) {
  auto loop = std::make_shared<Loop>();
  auto addr = Sockaddr::createAbstractUnixAddr("foobar");
  constexpr size_t numBytes = 13;

  initializePeers(
      loop,
      addr,
      [&](std::shared_ptr<Connection> conn) {
        Queue<size_t> reads;
        conn->read([&](const Error& error, const void* ptr, size_t len) {
          reads.push(len);
        });
        // Wait for the read callback to be called.
        ASSERT_EQ(numBytes, reads.pop());
      },
      [&](std::shared_ptr<Connection> conn) {
        Queue<bool> writes;
        std::array<char, numBytes> garbage;
        conn->write(garbage.data(), garbage.size(), [&](const Error& error) {
          writes.push(true);
        });
        // Wait for the write callback to be called.
        writes.pop();
      });
}

} // namespace shm
} // namespace transport
} // namespace test
