#include <tensorpipe/tensorpipe.pb.h>

#include <gtest/gtest.h>

TEST(Proto, MessageDescriptor) {
  tensorpipe::proto::MessageDescriptor d;
  d.set_size_in_bytes(10);
  EXPECT_EQ(d.size_in_bytes(), 10);
}
