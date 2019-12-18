#include <tensorpipe/util/shm/segment.h>

#include <thread>

#include <gtest/gtest.h>

using namespace tensorpipe::util::shm;

// Same process produces and consumes share memory through different mappings.
TEST(Segment, SameProducerConsumer_Scalar) {
  // Set affinity of producer to CPU zero so that consumer only has to read from
  // that one CPU's buffer.
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);
  sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

  // Make unique file name for shared memory
  auto base_name = Segment::createTempDir("tensorpipe_test");
  std::string name_id =
      base_name + "afolder/b/SameProducerConsumer_Scalar__test";

  {
    // Producer part.
    std::shared_ptr<int> my_int_ptr;
    std::tie(my_int_ptr, std::ignore) = Segment::create<int>(
        name_id, true, PageType::Default, CreationMode::LinkOnCreation);
    int& my_int = *my_int_ptr;
    my_int = 1000;
  }

  {
    // Consumer part.
    // Map file again (to a different address) and consume it.
    std::shared_ptr<int> my_int_ptr;
    std::shared_ptr<Segment> segment;
    std::tie(my_int_ptr, segment) =
        Segment::load<int>(name_id, true, PageType::Default);
    EXPECT_EQ(segment->getSize(), sizeof(int));
    EXPECT_EQ(*my_int_ptr, 1000);
  }

  // All done. Unlink to cleanup.
  Segment::unlink(name_id);
};

TEST(SegmentManager, SingleProducer_SingleConsumer_Array) {
  // Make unique file name for shared memory
  auto base_name = Segment::createTempDir("tensorpipe_test");
  std::string name_id = base_name + "SingleProducer_SingleConsumer_Array__test";

  size_t num_floats = 330000;

  int pid = fork();

  EXPECT_GE(pid, 0) << "Fork failed";

  if (pid == 0) {
    // children, the producer

    // use huge pages in creation and not in loading. This should only affects
    // TLB overhead.
    std::shared_ptr<float[]> my_floats;
    std::shared_ptr<Segment> segment;
    std::tie(my_floats, segment) = Segment::create<float[]>(
        num_floats,
        name_id,
        true,
        PageType::HugeTLB_2MB,
        CreationMode::LinkOnCreation);

    for (int i = 0; i < num_floats; ++i) {
      my_floats[i] = i;
    }

    sleep(1); // Wait for children to find and open file before unlinking it.
    Segment::unlink(name_id);
    // Children exits, releases shm segment shared_ptr first because exit()
    // won't do so.
    my_floats = nullptr;
    exit(0);
  }

  // parent

  // wait for other process to create buffer.
  sleep(1);
  std::shared_ptr<float[]> my_floats;
  std::shared_ptr<Segment> segment;
  std::tie(my_floats, segment) =
      Segment::load<float[]>(name_id, false, PageType::Default);
  EXPECT_EQ(num_floats * sizeof(float), segment->getSize());
  for (int i = 0; i < num_floats; ++i) {
    EXPECT_EQ(my_floats[i], i);
  }
};
