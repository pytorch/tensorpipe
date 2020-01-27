#include <gtest/gtest.h>

#include <tensorpipe/transport/uv/loop.h>

using namespace tensorpipe::transport::uv;

namespace test {
namespace transport {
namespace uv {

TEST(Loop, Create) {
  auto loop = Loop::create();
  ASSERT_TRUE(loop);
  loop->join();
}

TEST(Loop, RunSynchronous) {
  auto loop = Loop::create();

  {
    std::thread::id self_thread = std::this_thread::get_id();
    std::thread::id loop_thread = std::this_thread::get_id();
    ASSERT_EQ(self_thread, loop_thread);

    // Synchronously run function on event loop thread.
    loop->run([&] { loop_thread = std::this_thread::get_id(); });
    ASSERT_NE(self_thread, loop_thread);
  }

  loop->join();
}

} // namespace uv
} // namespace transport
} // namespace test
