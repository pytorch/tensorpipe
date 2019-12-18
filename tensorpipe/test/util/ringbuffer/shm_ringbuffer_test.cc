#include <tensorpipe/util/ringbuffer/consumer.h>
#include <tensorpipe/util/ringbuffer/producer.h>
#include <tensorpipe/util/ringbuffer/ringbuffer.h>
#include <tensorpipe/util/ringbuffer/shm.h>
#include <tensorpipe/util/shm/segment.h>

#include <sys/types.h>

#include <thread>

#include <gtest/gtest.h>

using namespace tensorpipe;
using namespace tensorpipe::util::ringbuffer;

struct MockExtraData {
  int dummy_data;
};

using TRingBuffer = RingBuffer<MockExtraData>;
using TProducer = Producer<MockExtraData>;
using TConsumer = Consumer<MockExtraData>;

// Same process produces and consumes share memory through different mappings.
TEST(ShmRingBuffer, SameProducerConsumer) {
  // Make unique file name for shared memory
  auto base_name =
      tensorpipe::util::shm::Segment::createTempDir("tensorpipe_test");
  std::string name_id = base_name + "SameProducerConsumer__test";

  {
    // Producer part.
    // Buffer large enough to fit all data and persistent
    // (needs to be unlinked up manually).
    auto rb = shm::create<TRingBuffer>(name_id, 256 * 1024, true);
    TProducer prod{rb};

    // Producer loop. It all fits in buffer.
    int i = 0;
    while (i < 2000) {
      ssize_t ret = prod.write(i);
      EXPECT_EQ(ret, sizeof(i));
      ++i;
    }
  }

  {
    // Consumer part.
    // Map file again (to a different address) and consume it.
    auto rb = shm::load<TRingBuffer>(name_id);
    TConsumer cons{rb};

    int i = 0;
    while (i < 2000) {
      int value;
      ssize_t ret = cons.copy(value);
      EXPECT_EQ(ret, sizeof(value));
      EXPECT_EQ(value, i);
      ++i;
    }
  }

  // All done. Unlink to cleanup.
  shm::unlink<TRingBuffer>(name_id);
};

TEST(ShmRingBuffer, SingleProducer_SingleConsumer) {
  // Make unique file name for shared memory
  auto base_name =
      tensorpipe::util::shm::Segment::createTempDir("tensorpipe_test");
  std::string name_id = base_name + "SingleProducer_SingleConsumer__test";

  int pid = fork();
  if (pid < 0) {
    TP_THROW_SYSTEM(errno) << "Failed to fork";
  }

  if (pid == 0) {
    // child, the producer
    {
      // Make a scope so shared_ptr's are released even on exit(0).
      //
      // Shared memory segment is set to be non-persistent. It will be
      // removed when rb goes out of scope.
      //
      auto rb = shm::create<TRingBuffer>(name_id, 1024, false);
      TProducer prod{rb};

      int i = 0;
      while (i < 2000) {
        ssize_t ret = prod.write(i);
        if (ret == -ENOSPC) {
          std::this_thread::yield();
          continue;
        }
        EXPECT_EQ(ret, sizeof(i));
        ++i;
      }
      // Because of buffer size smaller than amount of data written,
      // producer cannot have completed the loop before consumer
      // started consuming the data. This guarantees that children can
      // complete (and unlink before, since it's set to be
      // non-persistent) safely.
    }

    // Children exits. Careful when calling exit() directly, because
    // it does not call destructors. We ensured shared_ptrs were
    // destroyed before by calling exit(0).
    exit(0);
  } else {
    // parent, the consumer
    {
      // Wait for other process to create buffer, one second wait is
      // long enough to reasonably guarantee that the buffer has been
      // created. XXX: this should use semaphores to remove sleep.
      sleep(1);
      auto rb = shm::load<TRingBuffer>(name_id);
      TConsumer cons{rb};

      int i = 0;
      while (i < 2000) {
        int value;
        ssize_t ret = cons.copy(value);
        if (ret == -ENODATA) {
          std::this_thread::yield();
          continue;
        }
        EXPECT_EQ(ret, sizeof(value));
        EXPECT_EQ(value, i);
        ++i;
      }
    }

    // Wait for children to make gtest happy.
    wait(nullptr);
  }
};
