#include <sys/eventfd.h>

#include <deque>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/shm/loop.h>

#include <gtest/gtest.h>

using namespace tensorpipe::transport::shm;

namespace test {
namespace transport {
namespace shm {

namespace {

class Handler : public EventHandler {
 public:
  void handleEvents(int events) override {
    std::unique_lock<std::mutex> lock(m_);
    events_.push_back(events);
    cv_.notify_all();
  }

  int nextEvents() {
    std::unique_lock<std::mutex> lock(m_);
    while (events_.empty()) {
      cv_.wait(lock);
    }
    int events = events_.front();
    events_.pop_front();
    return events;
  }

 private:
  std::mutex m_;
  std::condition_variable cv_;
  std::deque<int> events_;
};

} // namespace

TEST(Loop, Create) {
  auto loop = std::make_shared<Loop>();
  ASSERT_TRUE(loop);
  loop.reset();
}

TEST(Loop, RegisterUnregister) {
  auto loop = std::make_shared<Loop>();
  auto handler = std::make_shared<Handler>();
  auto efd = Fd(eventfd(0, EFD_NONBLOCK));

  // Test if writable (always).
  loop->registerDescriptor(efd.fd(), EPOLLOUT | EPOLLONESHOT, handler.get());
  ASSERT_EQ(handler->nextEvents(), EPOLLOUT);
  efd.write<uint64_t>(1337);

  // Test if readable (only if previously written to).
  loop->registerDescriptor(efd.fd(), EPOLLIN | EPOLLONESHOT, handler.get());
  ASSERT_EQ(handler->nextEvents(), EPOLLIN);
  ASSERT_EQ(efd.read<uint64_t>(), 1337);

  // Test if we can unregister the descriptor.
  loop->unregisterDescriptor(efd.fd());
}

} // namespace shm
} // namespace transport
} // namespace test
