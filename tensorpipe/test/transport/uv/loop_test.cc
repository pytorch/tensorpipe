#include <gtest/gtest.h>

#include <tensorpipe/transport/uv/loop.h>

using namespace tensorpipe::transport::uv;

namespace test {
namespace transport {
namespace uv {

TEST(Loop, Create) {
  auto loop = Loop::create();
  ASSERT_TRUE(loop);
  loop.reset();
}

} // namespace uv
} // namespace transport
} // namespace test
