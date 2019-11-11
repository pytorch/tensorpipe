#include <gtest/gtest.h>

#include <tensorpipe/context/uv/loop.h>

using namespace tensorpipe::context::uv;

namespace test {
namespace context {
namespace uv {

TEST(Loop, Create) {
  auto loop = Loop::create();
  ASSERT_TRUE(loop);
  loop.reset();
}

} // namespace uv
} // namespace context
} // namespace test
