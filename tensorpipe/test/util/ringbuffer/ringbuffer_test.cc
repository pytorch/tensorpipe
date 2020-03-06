/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <array>

#include <tensorpipe/util/ringbuffer/consumer.h>
#include <tensorpipe/util/ringbuffer/producer.h>
#include <tensorpipe/util/ringbuffer/ringbuffer.h>

#include <gtest/gtest.h>

using namespace tensorpipe::util::ringbuffer;

struct TestData {
  uint16_t a;
  uint16_t b;
  uint16_t c;

  bool operator==(const TestData& other) const {
    return a == other.a && b == other.b && c == other.c;
  }
};

std::shared_ptr<RingBuffer> makeRingBuffer(size_t size) {
  auto header = std::make_shared<RingBufferHeader>(size);
  // In C++20 use std::make_shared<uint8_t[]>(size)
  auto data = std::shared_ptr<uint8_t>(
      new uint8_t[header->kDataPoolByteSize], std::default_delete<uint8_t[]>());
  return std::make_shared<RingBuffer>(std::move(header), std::move(data));
}

TEST(RingBuffer, WriteCopy) {
  EXPECT_EQ(sizeof(TestData), 6);

  // 16 bytes buffer. Fits two full TestData (each 6).
  size_t size = 1u << 4;

  auto rb = makeRingBuffer(size);
  // Make a producer.
  Producer p{rb};
  // Make a consumer.
  Consumer c{rb};

  EXPECT_EQ(rb->getHeader().usedSizeWeak(), 0);

  TestData d0{.a = 0xBA98, .b = 0x7654, .c = 0xA312};
  TestData d1{.a = 0xA987, .b = 0x7777, .c = 0x2812};
  TestData d2{.a = 0xFFFF, .b = 0x3333, .c = 0x1212};

  {
    ssize_t ret = p.write(&d0, sizeof(d0));
    EXPECT_EQ(ret, sizeof(TestData));
  }
  EXPECT_EQ(rb->getHeader().usedSizeWeak(), 6);

  {
    ssize_t ret = p.write(&d1, sizeof(d1));
    EXPECT_EQ(ret, sizeof(TestData));
  }
  EXPECT_EQ(rb->getHeader().usedSizeWeak(), 12);

  {
    ssize_t ret = p.write(&d2, sizeof(d2));
    EXPECT_EQ(ret, -ENOSPC) << "Needs 2 more bytes to write the 6 required, "
                               "because 12 out of 16 are used.";
  }

  TestData r;

  {
    ssize_t ret = c.copy(sizeof(r), &r);
    EXPECT_EQ(ret, sizeof(r));
    EXPECT_EQ(r, d0);
  }

  {
    ssize_t ret = c.copy(sizeof(r), &r);
    EXPECT_EQ(ret, sizeof(r));
    EXPECT_EQ(r, d1);
  }
  // It should be empty by now.
  EXPECT_EQ(rb->getHeader().usedSizeWeak(), 0);

  {
    ssize_t ret = p.write(&d2, sizeof(d2));
    EXPECT_EQ(ret, sizeof(TestData));
  }
  {
    ssize_t ret = c.copy(sizeof(r), &r);
    EXPECT_EQ(ret, sizeof(r));
    EXPECT_EQ(r, d2);
  }
  // It should be empty by now.
  EXPECT_EQ(rb->getHeader().usedSizeWeak(), 0);
}

TEST(RingBuffer, ReadMultipleElems) {
  // 256 bytes buffer.
  size_t size = 1u << 8u;

  auto rb = makeRingBuffer(size);
  // Make a producer.
  Producer p{rb};
  // Make a consumer.
  Consumer c{rb};

  EXPECT_EQ(rb->getHeader().usedSizeWeak(), 0);

  uint16_t n = 0xACAC; // fits 128 times

  {
    for (int i = 0; i < 128; ++i) {
      ssize_t ret = p.write(&n, sizeof(n));
      EXPECT_EQ(ret, sizeof(n));
    }

    // It must be full by now.
    EXPECT_EQ(rb->getHeader().usedSizeWeak(), 256);

    ssize_t ret = p.write(&n, sizeof(n));
    EXPECT_EQ(ret, -ENOSPC);
  }

  {
    uint8_t b = 0xEE;

    ssize_t ret = p.write(&b, sizeof(b));
    EXPECT_EQ(ret, -ENOSPC) << "Needs an extra byte";
  }

  {
    // read the three bytes at once.
    ssize_t ret;
    ret = c.startTx();
    EXPECT_EQ(ret, 0);

    std::array<uint8_t, 3> r;
    ret = c.copyInTx(sizeof(r), r.data());
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
    ret = c.copyInTx(sizeof(r), r.data());
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
    ret = c.copyInTx(sizeof(ch), &ch);
    EXPECT_EQ(ret, -ENODATA);
    ret = c.cancelTx();
    EXPECT_EQ(ret, 0);
    EXPECT_TRUE(!c.inTx()) << "Canceled transaction should've been canceled";
  }
}

TEST(RingBuffer, CopyWrapping) {
  // 8 bytes buffer.
  size_t size = 1u << 3;

  auto rb = makeRingBuffer(size);
  // Make a producer.
  Producer p{rb};
  // Make a consumer.
  Consumer c{rb};

  EXPECT_EQ(rb->getHeader().usedSizeWeak(), 0);

  uint8_t ch = 0xA7;
  uint64_t n = 0xFFFFFFFFFFFFFFFF;

  // Put one byte.
  EXPECT_EQ(rb->getHeader().readHead(), 0);
  EXPECT_EQ(rb->getHeader().readTail(), 0);
  EXPECT_EQ(rb->getHeader().usedSizeWeak(), 0);
  ssize_t ret = p.write(&ch, sizeof(ch));
  EXPECT_EQ(ret, sizeof(ch));
  EXPECT_EQ(rb->getHeader().readHead(), 1);
  EXPECT_EQ(rb->getHeader().readTail(), 0);
  EXPECT_EQ(rb->getHeader().usedSizeWeak(), 1);

  // Next 8 bytes won't fit.
  ret = p.write(&n, sizeof(n));
  EXPECT_EQ(ret, -ENOSPC)
      << "Needs an extra byte to write the 8 bytes element. "
         "Capacity 8, used 1.";

  // Remove the one byte in, now head is one off.
  uint8_t cr;
  uint64_t nr;

  ret = c.copy(sizeof(cr), &cr);
  EXPECT_EQ(ret, sizeof(cr));
  EXPECT_EQ(cr, ch);
  EXPECT_EQ(rb->getHeader().readHead(), 1);
  EXPECT_EQ(rb->getHeader().readTail(), 1);

  // Next 8 bytes will fit, but wrap.
  ret = p.write(&n, sizeof(n));
  EXPECT_EQ(ret, sizeof(n));
  EXPECT_EQ(rb->getHeader().readHead(), 9);
  EXPECT_EQ(rb->getHeader().readTail(), 1);

  ret = c.copy(sizeof(nr), &nr);
  EXPECT_EQ(ret, sizeof(nr));
  EXPECT_EQ(nr, n);
  EXPECT_EQ(rb->getHeader().readHead(), 9);
  EXPECT_EQ(rb->getHeader().readTail(), 9);
}

TEST(RingBuffer, ReadTxWrappingOneCons) {
  // 8 bytes buffer.
  size_t size = 1u << 3;

  auto rb = makeRingBuffer(size);
  // Make a producer.
  Producer p{rb};
  // Make a consumer.
  Consumer c1{rb};

  EXPECT_EQ(rb->getHeader().usedSizeWeak(), 0);

  uint8_t ch = 0xA7;
  uint64_t n = 0xFFFFFFFFFFFFFFFF;

  // Put one byte.
  {
    EXPECT_EQ(rb->getHeader().readHead(), 0);
    EXPECT_EQ(rb->getHeader().readTail(), 0);
    EXPECT_EQ(rb->getHeader().usedSizeWeak(), 0);
    ssize_t ret = p.write(&ch, sizeof(ch));
    EXPECT_EQ(ret, sizeof(ch));
    EXPECT_EQ(rb->getHeader().readHead(), 1);
    EXPECT_EQ(rb->getHeader().readTail(), 0);
    EXPECT_EQ(rb->getHeader().usedSizeWeak(), 1);
  }

  // Next 8 bytes won't fit.
  {
    ssize_t ret = p.write(&n, sizeof(n));
    EXPECT_EQ(ret, -ENOSPC)
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
    ret = c1.copyInTx(sizeof(rch), &rch);
    EXPECT_EQ(ret, sizeof(uint8_t));
    EXPECT_EQ(rch, ch);
    EXPECT_EQ(rb->getHeader().readHead(), 1);
    EXPECT_EQ(rb->getHeader().readTail(), 0);
    EXPECT_TRUE(c1.inTx());
  }

  {
    // Complete c1's Tx.
    ssize_t ret = c1.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(rb->getHeader().readHead(), 1);
    EXPECT_EQ(rb->getHeader().readTail(), 1);
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
    EXPECT_EQ(rb->getHeader().readHead(), 9);
    EXPECT_EQ(rb->getHeader().readTail(), 1);
  }

  {
    // Start c1 read Tx again.
    ssize_t ret;
    ret = c1.startTx();
    EXPECT_EQ(ret, 0);

    uint64_t rn;
    ret = c1.copyInTx(sizeof(rn), &rn);
    EXPECT_EQ(ret, sizeof(uint64_t));
    EXPECT_EQ(rn, n);
    EXPECT_EQ(rb->getHeader().readHead(), 9);
    EXPECT_EQ(rb->getHeader().readTail(), 1);
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
    EXPECT_EQ(rb->getHeader().readHead(), 17);
    EXPECT_EQ(rb->getHeader().readTail(), 9);
  }
  {
    ssize_t ret;
    ret = c1.startTx();
    EXPECT_EQ(ret, 0);

    uint64_t rn;
    ret = c1.copyInTx(sizeof(rn), &rn);
    EXPECT_EQ(ret, sizeof(uint64_t));
    EXPECT_EQ(rn, n);
    EXPECT_EQ(rb->getHeader().readHead(), 17);
    EXPECT_EQ(rb->getHeader().readTail(), 9);
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
    ret = c1.copyInTx(sizeof(rn), &rn);
    EXPECT_EQ(ret, sizeof(uint64_t));
    EXPECT_EQ(rn, n);
    EXPECT_EQ(rb->getHeader().readHead(), 17);
    EXPECT_EQ(rb->getHeader().readTail(), 9);
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

  auto rb = makeRingBuffer(size);
  // Make a producer.
  Producer p{rb};
  // Make consumers.
  Consumer c1{rb};
  Consumer c2{rb};

  EXPECT_EQ(rb->getHeader().usedSizeWeak(), 0);

  uint8_t ch = 0xA7;
  uint64_t n = 0x3333333333333333;

  // Put one byte.
  {
    EXPECT_EQ(rb->getHeader().readHead(), 0);
    EXPECT_EQ(rb->getHeader().readTail(), 0);
    EXPECT_EQ(rb->getHeader().usedSizeWeak(), 0);
    ssize_t ret = p.write(&ch, sizeof(ch));
    EXPECT_EQ(ret, sizeof(ch));
    EXPECT_EQ(rb->getHeader().readHead(), 1);
    EXPECT_EQ(rb->getHeader().readTail(), 0);
    EXPECT_EQ(rb->getHeader().usedSizeWeak(), 1);
  }

  // Next 8 bytes won't fit.
  {
    ssize_t ret = p.write(&n, sizeof(n));
    EXPECT_EQ(ret, -ENOSPC)
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
    ret = c1.copyInTx(sizeof(rch), &rch);
    EXPECT_EQ(ret, sizeof(uint8_t));
    EXPECT_EQ(rch, ch);
    EXPECT_EQ(rb->getHeader().readHead(), 1);
    EXPECT_EQ(rb->getHeader().readTail(), 0);
    EXPECT_TRUE(c1.inTx());
  }

  {
    // Complete c1's Tx.
    ssize_t ret = c1.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(rb->getHeader().readHead(), 1);
    EXPECT_EQ(rb->getHeader().readTail(), 1);
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
    EXPECT_EQ(rb->getHeader().readHead(), 9);
    EXPECT_EQ(rb->getHeader().readTail(), 1);
  }

  {
    // Start c1 read Tx again.
    ssize_t ret;
    ret = c1.startTx();
    EXPECT_EQ(ret, 0);

    uint64_t rn;
    ret = c1.copyInTx(sizeof(rn), &rn);
    EXPECT_EQ(ret, sizeof(uint64_t));
    EXPECT_EQ(rn, n);
    EXPECT_EQ(rb->getHeader().readHead(), 9);
    EXPECT_EQ(rb->getHeader().readTail(), 1);
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
    EXPECT_EQ(rb->getHeader().readHead(), 17);
    EXPECT_EQ(rb->getHeader().readTail(), 9);
  }
  {
    ssize_t ret;
    ret = c2.startTx();
    EXPECT_EQ(ret, 0);

    uint64_t rn;
    ret = c2.copyInTx(sizeof(rn), &rn);
    EXPECT_EQ(ret, sizeof(uint64_t));
    EXPECT_EQ(rn, n);
    EXPECT_EQ(rb->getHeader().readHead(), 17);
    EXPECT_EQ(rb->getHeader().readTail(), 9);
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
    ret = c1.copyInTx(sizeof(rn), &rn);
    EXPECT_EQ(ret, sizeof(uint64_t));
    EXPECT_EQ(rn, n);
    EXPECT_EQ(rb->getHeader().readHead(), 17);
    EXPECT_EQ(rb->getHeader().readTail(), 9);
  }

  {
    // Commit succeds.
    ssize_t ret = c1.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_FALSE(c1.inTx());
    EXPECT_FALSE(c2.inTx());
  }
}

TEST(RingBuffer, readContiguousAtMostInTx) {
  // 256 bytes buffer.
  size_t size = 1u << 8u;

  auto rb = makeRingBuffer(size);
  // Make a producer.
  Producer p{rb};
  // Make a consumer.
  Consumer c{rb};

  EXPECT_EQ(rb->getHeader().usedSizeWeak(), 0);

  uint16_t n = 0xACAC; // fits 128 times

  {
    for (int i = 0; i < 128; ++i) {
      ssize_t ret = p.write(&n, sizeof(n));
      EXPECT_EQ(ret, sizeof(n));
    }

    // It must be full by now.
    EXPECT_EQ(rb->getHeader().usedSizeWeak(), 256);

    uint8_t b = 0xEE;
    ssize_t ret = p.write(&b, sizeof(b));
    EXPECT_EQ(ret, -ENOSPC);
  }

  {
    // Read the first 200 bytes at once.
    ssize_t ret;
    ret = c.startTx();
    EXPECT_EQ(ret, 0);

    const uint8_t* ptr;
    std::tie(ret, ptr) = c.readContiguousAtMostInTx(200);
    EXPECT_EQ(ret, 200);
    for (int i = 0; i < 200; ++i) {
      EXPECT_EQ(ptr[i], 0xAC);
    }
    ret = c.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(rb->getHeader().usedSizeWeak(), 56);
  }

  {
    for (int i = 0; i < 100; ++i) {
      ssize_t ret = p.write(&n, sizeof(n));
      EXPECT_EQ(ret, sizeof(n));
    }

    // It must be full again by now.
    EXPECT_EQ(rb->getHeader().usedSizeWeak(), 256);
  }

  {
    // Attempt reading the next 200 bytes, but only 56 available contiguously.
    ssize_t ret;
    ret = c.startTx();
    EXPECT_EQ(ret, 0);

    const uint8_t* ptr;
    std::tie(ret, ptr) = c.readContiguousAtMostInTx(200);
    EXPECT_EQ(ret, 56);
    for (int i = 0; i < 56; ++i) {
      EXPECT_EQ(ptr[i], 0xAC);
    }
    ret = c.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(rb->getHeader().usedSizeWeak(), 200);
  }

  {
    for (int i = 0; i < 28; ++i) {
      ssize_t ret = p.write(&n, sizeof(n));
      EXPECT_EQ(ret, sizeof(n));
    }

    // It must be full again by now.
    EXPECT_EQ(rb->getHeader().usedSizeWeak(), 256);
  }

  {
    // Reading the whole 256 bytes.
    ssize_t ret;
    ret = c.startTx();
    EXPECT_EQ(ret, 0);

    const uint8_t* ptr;
    std::tie(ret, ptr) = c.readContiguousAtMostInTx(256);
    EXPECT_EQ(ret, 256);
    for (int i = 0; i < 256; ++i) {
      EXPECT_EQ(ptr[i], 0xAC);
    }
    ret = c.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(rb->getHeader().usedSizeWeak(), 0);
  }

  {
    // Attempt reading from empty buffer.
    ssize_t ret;
    ret = c.startTx();
    EXPECT_EQ(ret, 0);

    const uint8_t* ptr;
    std::tie(ret, ptr) = c.readContiguousAtMostInTx(200);
    EXPECT_EQ(ret, 0);
    ret = c.commitTx();
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(rb->getHeader().usedSizeWeak(), 0);
  }
}
