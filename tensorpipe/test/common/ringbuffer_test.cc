/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <array>

#include <tensorpipe/common/ringbuffer.h>
#include <tensorpipe/common/ringbuffer_role.h>

#include <gtest/gtest.h>

using namespace tensorpipe;

struct TestData {
  uint16_t a;
  uint16_t b;
  uint16_t c;

  bool operator==(const TestData& other) const {
    return a == other.a && b == other.b && c == other.c;
  }
};

constexpr static int kNumRingbufferRoles = 2;
constexpr static int kConsumerRoleIdx = 0;
constexpr static int kProducerRoleIdx = 1;
using Consumer = RingBufferRole<kNumRingbufferRoles, kConsumerRoleIdx>;
using Producer = RingBufferRole<kNumRingbufferRoles, kProducerRoleIdx>;

// Holds and owns the memory for the ringbuffer's header and data.
class RingBufferStorage {
 public:
  explicit RingBufferStorage(size_t size) : header_(size) {}

  RingBuffer<kNumRingbufferRoles> getRb() {
    return {&header_, data_.get()};
  }

 private:
  RingBufferHeader<kNumRingbufferRoles> header_;
  std::unique_ptr<uint8_t[]> data_ =
      std::make_unique<uint8_t[]>(header_.kDataPoolByteSize);
};

size_t usedSize(RingBuffer<kNumRingbufferRoles>& rb) {
  return rb.getHeader().template readMarker<kProducerRoleIdx>() -
      rb.getHeader().template readMarker<kConsumerRoleIdx>();
}

TEST(RingBuffer, WriteCopy) {
  EXPECT_EQ(sizeof(TestData), 6);

  // 16 bytes buffer. Fits two full TestData (each 6).
  size_t size = 1u << 4;

  RingBufferStorage storage(size);
  RingBuffer<kNumRingbufferRoles> rb = storage.getRb();
  // Make a producer.
  Producer p{rb};
  // Make a consumer.
  Consumer c{rb};

  EXPECT_EQ(usedSize(rb), 0);

  TestData d0{.a = 0xBA98, .b = 0x7654, .c = 0xA312};
  TestData d1{.a = 0xA987, .b = 0x7777, .c = 0x2812};
  TestData d2{.a = 0xFFFF, .b = 0x3333, .c = 0x1212};

  {
    ssize_t ret = p.write(&d0, sizeof(d0));
    EXPECT_EQ(ret, sizeof(TestData));
  }
  EXPECT_EQ(usedSize(rb), 6);

  {
    ssize_t ret = p.write(&d1, sizeof(d1));
    EXPECT_EQ(ret, sizeof(TestData));
  }
  EXPECT_EQ(usedSize(rb), 12);

  {
    ssize_t ret = p.write(&d2, sizeof(d2));
    EXPECT_EQ(ret, -ENODATA) << "Needs 2 more bytes to write the 6 required, "
                                "because 12 out of 16 are used.";
  }

  TestData r;

  {
    ssize_t ret = c.read(&r, sizeof(r));
    EXPECT_EQ(ret, sizeof(r));
    EXPECT_EQ(r, d0);
  }

  {
    ssize_t ret = c.read(&r, sizeof(r));
    EXPECT_EQ(ret, sizeof(r));
    EXPECT_EQ(r, d1);
  }
  // It should be empty by now.
  EXPECT_EQ(usedSize(rb), 0);

  {
    ssize_t ret = p.write(&d2, sizeof(d2));
    EXPECT_EQ(ret, sizeof(TestData));
  }
  {
    ssize_t ret = c.read(&r, sizeof(r));
    EXPECT_EQ(ret, sizeof(r));
    EXPECT_EQ(r, d2);
  }
  // It should be empty by now.
  EXPECT_EQ(usedSize(rb), 0);
}

TEST(RingBuffer, ReadMultipleElems) {
  // 256 bytes buffer.
  size_t size = 1u << 8u;

  RingBufferStorage storage(size);
  RingBuffer<kNumRingbufferRoles> rb = storage.getRb();
  // Make a producer.
  Producer p{rb};
  // Make a consumer.
  Consumer c{rb};

  EXPECT_EQ(usedSize(rb), 0);

  uint16_t n = 0xACAC; // fits 128 times

  {
    for (int i = 0; i < 128; ++i) {
      ssize_t ret = p.write(&n, sizeof(n));
      EXPECT_EQ(ret, sizeof(n));
    }

    // It must be full by now.
    EXPECT_EQ(usedSize(rb), 256);

    ssize_t ret = p.write(&n, sizeof(n));
    EXPECT_EQ(ret, -ENODATA);
  }

  {
    uint8_t b = 0xEE;

    ssize_t ret = p.write(&b, sizeof(b));
    EXPECT_EQ(ret, -ENODATA) << "Needs an extra byte";
  }

  {
    // read the three bytes at once.
    ssize_t ret;
    ret = c.startTx();
    EXPECT_EQ(ret, 0);

    std::array<uint8_t, 3> r;
    ret = c.readInTx</*AllowPartial=*/false>(r.data(), sizeof(r));
    EXPECT_EQ(ret, 3);
    EXPECT_EQ(r[0], 0xAC);
    EXPECT_EQ(r[1], 0xAC);
    EXPECT_EQ(r[2], 0xAC);
    ret = c.commitTx();
    EXPECT_EQ(ret, 0);
  }

  {
    // read 253 bytes at once.
    ssize_t ret;
    ret = c.startTx();
    EXPECT_EQ(ret, 0);

    std::array<uint8_t, 253> r;
    ret = c.readInTx</*AllowPartial=*/false>(r.data(), sizeof(r));
    EXPECT_EQ(ret, 253);
    for (int i = 0; i < 253; ++i) {
      EXPECT_EQ(r[i], 0xAC);
    }
    ret = c.commitTx();
    EXPECT_EQ(ret, 0);
  }

  {
    // No more elements
    ssize_t ret;
    ret = c.startTx();
    EXPECT_EQ(ret, 0);
    uint8_t ch;
    ret = c.readInTx</*AllowPartial=*/false>(&ch, sizeof(ch));
    EXPECT_EQ(ret, -ENODATA);
    ret = c.cancelTx();
    EXPECT_EQ(ret, 0);
    EXPECT_TRUE(!c.inTx()) << "Canceled transaction should've been canceled";
  }
}

TEST(RingBuffer, CopyWrapping) {
  // 8 bytes buffer.
  size_t size = 1u << 3;

  RingBufferStorage storage(size);
  RingBuffer<kNumRingbufferRoles> rb = storage.getRb();
  // Make a producer.
  Producer p{rb};
  // Make a consumer.
  Consumer c{rb};

  EXPECT_EQ(usedSize(rb), 0);

  uint8_t ch = 0xA7;
  uint64_t n = 0xFFFFFFFFFFFFFFFF;

  // Put one byte.
  EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 0);
  EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 0);
  EXPECT_EQ(usedSize(rb), 0);
  ssize_t ret = p.write(&ch, sizeof(ch));
  EXPECT_EQ(ret, sizeof(ch));
  EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 1);
  EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 0);
  EXPECT_EQ(usedSize(rb), 1);

  // Next 8 bytes won't fit.
  ret = p.write(&n, sizeof(n));
  EXPECT_EQ(ret, -ENODATA)
      << "Needs an extra byte to write the 8 bytes element. "
         "Capacity 8, used 1.";

  // Remove the one byte in, now head is one off.
  uint8_t cr;
  uint64_t nr;

  ret = c.read(&cr, sizeof(cr));
  EXPECT_EQ(ret, sizeof(cr));
  EXPECT_EQ(cr, ch);
  EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 1);
  EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 1);

  // Next 8 bytes will fit, but wrap.
  ret = p.write(&n, sizeof(n));
  EXPECT_EQ(ret, sizeof(n));
  EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 9);
  EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 1);

  ret = c.read(&nr, sizeof(nr));
  EXPECT_EQ(ret, sizeof(nr));
  EXPECT_EQ(nr, n);
  EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 9);
  EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 9);
}

TEST(RingBuffer, ReadTxWrappingOneCons) {
  // 8 bytes buffer.
  size_t size = 1u << 3;

  RingBufferStorage storage(size);
  RingBuffer<kNumRingbufferRoles> rb = storage.getRb();
  // Make a producer.
  Producer p{rb};
  // Make a consumer.
  Consumer c1{rb};

  EXPECT_EQ(usedSize(rb), 0);

  uint8_t ch = 0xA7;
  uint64_t n = 0xFFFFFFFFFFFFFFFF;

  // Put one byte.
  {
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 0);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 0);
    EXPECT_EQ(usedSize(rb), 0);
    ssize_t ret = p.write(&ch, sizeof(ch));
    EXPECT_EQ(ret, sizeof(ch));
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 1);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 0);
    EXPECT_EQ(usedSize(rb), 1);
  }

  // Next 8 bytes won't fit.
  {
    ssize_t ret = p.write(&n, sizeof(n));
    EXPECT_EQ(ret, -ENODATA)
        << "Needs an extra byte to write the 8 bytes element. "
           "Capacity 8, used 1.";
  }

  // Remove the one byte in, now head is one off.
  EXPECT_FALSE(c1.inTx());

  {
    // Start c1 read Tx
    ssize_t ret;
    ret = c1.startTx();
    EXPECT_EQ(ret, 0);

    uint8_t rch;
    ret = c1.readInTx</*AllowPartial=*/false>(&rch, sizeof(rch));
    EXPECT_EQ(ret, sizeof(uint8_t));
    EXPECT_EQ(rch, ch);
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 1);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 0);
    EXPECT_TRUE(c1.inTx());
  }

  {
    // Complete c1's Tx.
    ssize_t ret = c1.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 1);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 1);
  }
  {
    // Retrying to commit should fail.
    ssize_t ret = c1.commitTx();
    EXPECT_EQ(ret, -EINVAL);
  }

  {
    // Next 8 bytes will fit, but wrap.
    ssize_t ret = p.write(&n, sizeof(n));
    EXPECT_EQ(ret, sizeof(n));
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 9);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 1);
  }

  {
    // Start c1 read Tx again.
    ssize_t ret;
    ret = c1.startTx();
    EXPECT_EQ(ret, 0);

    uint64_t rn;
    ret = c1.readInTx</*AllowPartial=*/false>(&rn, sizeof(rn));
    EXPECT_EQ(ret, sizeof(uint64_t));
    EXPECT_EQ(rn, n);
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 9);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 1);
    EXPECT_TRUE(c1.inTx());
  }

  {
    // Complete c1.
    ssize_t ret = c1.commitTx();
    EXPECT_EQ(ret, 0);
    ret = c1.commitTx();
    EXPECT_EQ(ret, -EINVAL);
  }

  {
    // Next 8 bytes will fit, but wrap.
    ssize_t ret = p.write(&n, sizeof(n));
    EXPECT_EQ(ret, sizeof(n));
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 17);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 9);
  }
  {
    ssize_t ret;
    ret = c1.startTx();
    EXPECT_EQ(ret, 0);

    uint64_t rn;
    ret = c1.readInTx</*AllowPartial=*/false>(&rn, sizeof(rn));
    EXPECT_EQ(ret, sizeof(uint64_t));
    EXPECT_EQ(rn, n);
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 17);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 9);
  }

  {
    // Cancel tx, data should be readable again.
    ssize_t ret = c1.cancelTx();
    EXPECT_EQ(ret, 0);
  }

  {
    // Now c1 can read.
    ssize_t ret;
    ret = c1.startTx();
    EXPECT_EQ(ret, 0);

    uint64_t rn;
    ret = c1.readInTx</*AllowPartial=*/false>(&rn, sizeof(rn));
    EXPECT_EQ(ret, sizeof(uint64_t));
    EXPECT_EQ(rn, n);
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 17);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 9);
  }

  {
    // Commit succeds.
    ssize_t ret = c1.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_FALSE(c1.inTx());
  }
}

TEST(RingBuffer, ReadTxWrapping) {
  // 8 bytes buffer.
  size_t size = 1u << 3;

  RingBufferStorage storage(size);
  RingBuffer<kNumRingbufferRoles> rb = storage.getRb();
  // Make a producer.
  Producer p{rb};
  // Make consumers.
  Consumer c1{rb};
  Consumer c2{rb};

  EXPECT_EQ(usedSize(rb), 0);

  uint8_t ch = 0xA7;
  uint64_t n = 0x3333333333333333;

  // Put one byte.
  {
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 0);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 0);
    EXPECT_EQ(usedSize(rb), 0);
    ssize_t ret = p.write(&ch, sizeof(ch));
    EXPECT_EQ(ret, sizeof(ch));
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 1);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 0);
    EXPECT_EQ(usedSize(rb), 1);
  }

  // Next 8 bytes won't fit.
  {
    ssize_t ret = p.write(&n, sizeof(n));
    EXPECT_EQ(ret, -ENODATA)
        << "Needs an extra byte to write the 8 bytes element. "
           "Capacity 8, used 1.";
  }

  // Remove the one byte in, now head is one off.
  EXPECT_FALSE(c1.inTx());
  EXPECT_FALSE(c2.inTx());

  {
    // Start c1 read Tx
    ssize_t ret;
    ret = c1.startTx();
    EXPECT_EQ(ret, 0);

    uint8_t rch;
    ret = c1.readInTx</*AllowPartial=*/false>(&rch, sizeof(rch));
    EXPECT_EQ(ret, sizeof(uint8_t));
    EXPECT_EQ(rch, ch);
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 1);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 0);
    EXPECT_TRUE(c1.inTx());
  }

  {
    // Complete c1's Tx.
    ssize_t ret = c1.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 1);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 1);
  }
  {
    // Retrying to commit should fail.
    ssize_t ret = c1.commitTx();
    EXPECT_EQ(ret, -EINVAL);
  }

  {
    // Next 8 bytes will fit, but wrap.
    ssize_t ret = p.write(&n, sizeof(n));
    EXPECT_EQ(ret, sizeof(n));
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 9);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 1);
  }

  {
    // Start c1 read Tx again.
    ssize_t ret;
    ret = c1.startTx();
    EXPECT_EQ(ret, 0);

    uint64_t rn;
    ret = c1.readInTx</*AllowPartial=*/false>(&rn, sizeof(rn));
    EXPECT_EQ(ret, sizeof(uint64_t));
    EXPECT_EQ(rn, n);
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 9);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 1);
    EXPECT_TRUE(c1.inTx());
  }

  {
    // Try to start read tx before c1 completing and get -EAGAIN.
    ssize_t ret;
    ret = c2.startTx();
    EXPECT_EQ(ret, -EAGAIN);
  }

  {
    // Complete c1.
    ssize_t ret = c1.commitTx();
    EXPECT_EQ(ret, 0);
    ret = c1.commitTx();
    EXPECT_EQ(ret, -EINVAL);
  }

  {
    // Next 8 bytes will fit, but wrap.
    ssize_t ret = p.write(&n, sizeof(n));
    EXPECT_EQ(ret, sizeof(n));
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 17);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 9);
  }
  {
    ssize_t ret;
    ret = c2.startTx();
    EXPECT_EQ(ret, 0);

    uint64_t rn;
    ret = c2.readInTx</*AllowPartial=*/false>(&rn, sizeof(rn));
    EXPECT_EQ(ret, sizeof(uint64_t));
    EXPECT_EQ(rn, n);
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 17);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 9);
  }

  {
    // Cancel tx, data should be readable again.
    ssize_t ret = c2.cancelTx();
    EXPECT_EQ(ret, 0);
  }

  {
    // Now c1 can read.
    ssize_t ret;
    ret = c1.startTx();
    EXPECT_EQ(ret, 0);

    uint64_t rn;
    ret = c1.readInTx</*AllowPartial=*/false>(&rn, sizeof(rn));
    EXPECT_EQ(ret, sizeof(uint64_t));
    EXPECT_EQ(rn, n);
    EXPECT_EQ(rb.getHeader().template readMarker<kProducerRoleIdx>(), 17);
    EXPECT_EQ(rb.getHeader().template readMarker<kConsumerRoleIdx>(), 9);
  }

  {
    // Commit succeds.
    ssize_t ret = c1.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_FALSE(c1.inTx());
    EXPECT_FALSE(c2.inTx());
  }
}

TEST(RingBuffer, accessContiguousInTx) {
  // 256 bytes buffer.
  size_t size = 1u << 8u;

  RingBufferStorage storage(size);
  RingBuffer<kNumRingbufferRoles> rb = storage.getRb();
  // Make a producer.
  Producer p{rb};
  // Make a consumer.
  Consumer c{rb};

  EXPECT_EQ(usedSize(rb), 0);

  // Use different values for the three writing passes to tell them apart.
  uint16_t value1 = 0xACAC; // fits 128 times
  uint16_t value2 = 0xDCDC; // fits 128 times
  uint16_t value3 = 0xEFEF; // fits 128 times

  {
    for (int i = 0; i < 128; ++i) {
      ssize_t ret = p.write(&value1, sizeof(value1));
      EXPECT_EQ(ret, sizeof(value1));
    }

    // It must be full by now.
    EXPECT_EQ(usedSize(rb), 256);

    uint8_t b = 0xEE;
    ssize_t ret = p.write(&b, sizeof(b));
    EXPECT_EQ(ret, -ENODATA);
  }

  {
    // Read a 128-byte buffer that is left-aligned with the start.
    ssize_t ret;
    ret = c.startTx();
    EXPECT_EQ(ret, 0);

    std::array<Consumer::Buffer, 2> buffers;
    std::tie(ret, buffers) = c.accessContiguousInTx</*AllowPartial=*/true>(128);
    EXPECT_EQ(ret, 1);
    EXPECT_EQ(buffers[0].len, 128);
    for (int i = 0; i < 128; ++i) {
      EXPECT_EQ(buffers[0].ptr[i], 0xAC);
    }
    ret = c.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(usedSize(rb), 128);
  }

  {
    for (int i = 0; i < 64; ++i) {
      ssize_t ret = p.write(&value2, sizeof(value2));
      EXPECT_EQ(ret, sizeof(value2));
    }

    // It must be full again by now.
    EXPECT_EQ(usedSize(rb), 256);
  }

  {
    // Read a 256-byte buffer that wraps around halfway through.
    ssize_t ret;
    ret = c.startTx();
    EXPECT_EQ(ret, 0);

    std::array<Consumer::Buffer, 2> buffers;
    std::tie(ret, buffers) = c.accessContiguousInTx</*AllowPartial=*/true>(256);
    EXPECT_EQ(ret, 2);
    EXPECT_EQ(buffers[0].len, 128);
    for (int i = 0; i < 128; ++i) {
      EXPECT_EQ(buffers[0].ptr[i], 0xAC);
    }
    EXPECT_EQ(buffers[1].len, 128);
    for (int i = 0; i < 128; ++i) {
      EXPECT_EQ(buffers[1].ptr[i], 0xDC);
    }
    ret = c.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(usedSize(rb), 0);
  }

  {
    for (int i = 0; i < 64; ++i) {
      ssize_t ret = p.write(&value2, sizeof(value2));
      EXPECT_EQ(ret, sizeof(value2));
    }
    for (int i = 0; i < 64; ++i) {
      ssize_t ret = p.write(&value3, sizeof(value3));
      EXPECT_EQ(ret, sizeof(value3));
    }

    // It must be full again by now.
    EXPECT_EQ(usedSize(rb), 256);
  }

  {
    // Read a 128-byte buffer that is right-aligned with the end.
    ssize_t ret;
    ret = c.startTx();
    EXPECT_EQ(ret, 0);

    std::array<Consumer::Buffer, 2> buffers;
    std::tie(ret, buffers) = c.accessContiguousInTx</*AllowPartial=*/true>(128);
    EXPECT_EQ(ret, 1);
    EXPECT_EQ(buffers[0].len, 128);
    for (int i = 0; i < 128; ++i) {
      EXPECT_EQ(buffers[0].ptr[i], 0xDC);
    }
    ret = c.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(usedSize(rb), 128);
  }

  {
    for (int i = 0; i < 64; ++i) {
      ssize_t ret = p.write(&value3, sizeof(value3));
      EXPECT_EQ(ret, sizeof(value3));
    }

    // It must be full again by now.
    EXPECT_EQ(usedSize(rb), 256);
  }

  {
    // Reading the whole 256 bytes.
    ssize_t ret;
    ret = c.startTx();
    EXPECT_EQ(ret, 0);

    std::array<Consumer::Buffer, 2> buffers;
    std::tie(ret, buffers) = c.accessContiguousInTx</*AllowPartial=*/true>(256);
    EXPECT_EQ(ret, 1);
    EXPECT_EQ(buffers[0].len, 256);
    for (int i = 0; i < 256; ++i) {
      EXPECT_EQ(buffers[0].ptr[i], 0xEF);
    }
    ret = c.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(usedSize(rb), 0);
  }

  {
    // Attempt reading from empty buffer.
    ssize_t ret;
    ret = c.startTx();
    EXPECT_EQ(ret, 0);

    std::array<Consumer::Buffer, 2> buffers;
    std::tie(ret, buffers) = c.accessContiguousInTx</*AllowPartial=*/true>(200);
    EXPECT_EQ(ret, 0);
    ret = c.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(usedSize(rb), 0);
  }
}
