#include <tensorpipe/common/system.h>

#include <gtest/gtest.h>

using namespace tensorpipe;

TEST(Defs, CpuSet) {
  {
    auto c = CpuSet::makeAllOnline();
    EXPECT_TRUE(c.hasCpu(0));
  }
  {
    auto c = CpuSet::makeFromCont(std::vector<unsigned>{});
    EXPECT_FALSE(c.hasCpu(1));
  }

  {
    auto c = CpuSet::makeFromCont(std::initializer_list<CpuId>{1});
    EXPECT_FALSE(c.hasCpu(0));
    EXPECT_TRUE(c.hasCpu(1));
    EXPECT_FALSE(c.hasCpu(11));
  }

  {
    auto c = CpuSet::makeFromCont(std::set<int>{0, 2});
    EXPECT_TRUE(c.hasCpu(0));
    EXPECT_FALSE(c.hasCpu(1));
    EXPECT_FALSE(c.hasCpu(3));
    EXPECT_TRUE(c.hasCpu(2));
    EXPECT_FALSE(c.hasCpu(21));
  }
}

TEST(ParseCpu, CpuRange) {
  {
    auto cpus = parseCpusRange("");
    cpu_set_t exp;
    CPU_ZERO(&exp);
    EXPECT_TRUE(CPU_EQUAL(&cpus, &exp));
  }

  {
    auto cpus = parseCpusRange(" \n ");
    cpu_set_t exp;
    CPU_ZERO(&exp);
    EXPECT_TRUE(CPU_EQUAL(&cpus, &exp));
  }

  {
    auto cpus = parseCpusRange(" ");
    cpu_set_t exp;
    CPU_ZERO(&exp);
    EXPECT_TRUE(CPU_EQUAL(&cpus, &exp));
  }

  {
    auto cpus = parseCpusRange("5");
    cpu_set_t exp;
    CPU_ZERO(&exp);
    CPU_SET(5, &exp);
    EXPECT_TRUE(CPU_EQUAL(&cpus, &exp));
  }

  {
    auto cpus = parseCpusRange("0-1");
    cpu_set_t exp;
    CPU_ZERO(&exp);
    CPU_SET(0, &exp);
    CPU_SET(1, &exp);
    EXPECT_TRUE(CPU_EQUAL(&cpus, &exp));
  }
}

TEST(ParseCpu, CpuList) {
  {
    auto cpus = parseCpusList("");
    cpu_set_t exp;
    CPU_ZERO(&exp);
    EXPECT_TRUE(CPU_EQUAL(&cpus, &exp));
  }

  {
    auto cpus = parseCpusList("\n");
    cpu_set_t exp;
    CPU_ZERO(&exp);
    EXPECT_TRUE(CPU_EQUAL(&cpus, &exp));
  }

  {
    auto cpus = parseCpusList(" \n ");
    cpu_set_t exp;
    CPU_ZERO(&exp);
    EXPECT_TRUE(CPU_EQUAL(&cpus, &exp));
  }
  {
    auto cpus = parseCpusList("5");
    cpu_set_t exp;
    CPU_ZERO(&exp);
    CPU_SET(5, &exp);
    EXPECT_TRUE(CPU_EQUAL(&cpus, &exp));
  }

  {
    auto cpus = parseCpusList("0-1");
    cpu_set_t exp;
    CPU_ZERO(&exp);
    CPU_SET(0, &exp);
    CPU_SET(1, &exp);
    EXPECT_TRUE(CPU_EQUAL(&cpus, &exp));
  }

  {
    auto cpus = parseCpusList("5, 0-1");
    cpu_set_t exp;
    CPU_ZERO(&exp);
    CPU_SET(0, &exp);
    CPU_SET(1, &exp);
    CPU_SET(5, &exp);
    EXPECT_TRUE(CPU_EQUAL(&cpus, &exp));
  }
  {
    auto cpus = parseCpusList("5-6, 0-1,9");
    cpu_set_t exp;
    CPU_ZERO(&exp);
    CPU_SET(0, &exp);
    CPU_SET(1, &exp);
    CPU_SET(5, &exp);
    CPU_SET(6, &exp);
    CPU_SET(9, &exp);
    EXPECT_TRUE(CPU_EQUAL(&cpus, &exp));
  }
  {
    auto cpus = parseCpusList("49-55");
    cpu_set_t exp;
    CPU_ZERO(&exp);
    CPU_SET(49, &exp);
    CPU_SET(50, &exp);
    CPU_SET(51, &exp);
    CPU_SET(52, &exp);
    CPU_SET(53, &exp);
    CPU_SET(54, &exp);
    CPU_SET(55, &exp);
    EXPECT_TRUE(CPU_EQUAL(&cpus, &exp));
  }
}

TEST(Pow2, isPow2) {
  for (uint64_t i = 0; i < 63; ++i) {
    EXPECT_TRUE(isPow2(1ull << i));
  }

  EXPECT_FALSE(isPow2(3));
  EXPECT_FALSE(isPow2(5));
  EXPECT_FALSE(isPow2(10));
  EXPECT_FALSE(isPow2(15));
  EXPECT_TRUE(isPow2(16));
  EXPECT_FALSE(isPow2(17));
  EXPECT_FALSE(isPow2(18));
  EXPECT_FALSE(isPow2(25));
  EXPECT_FALSE(isPow2(1028));
}

TEST(Pow2, nextPow2) {
  for (uint64_t i = 0; i < 63; ++i) {
    uint64_t p2 = 1ull << i;
    uint64_t next_p2 = 1ull << (i + 1);
    EXPECT_EQ(nextPow2(p2), p2);
    EXPECT_EQ(nextPow2(p2 + 1), next_p2);
  }
}
