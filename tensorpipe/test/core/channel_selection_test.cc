/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/channel_selection.h>

#include <gtest/gtest.h>

using namespace tensorpipe;

namespace {

class MockChannelContext : public channel::Context {
 public:
  static std::shared_ptr<channel::Context> create(
      std::unordered_map<Device, std::string> deviceDescriptors) {
    return std::make_shared<MockChannelContext>(std::move(deviceDescriptors));
  }

  explicit MockChannelContext(
      std::unordered_map<Device, std::string> deviceDescriptors)
      : deviceDescriptors_(std::move(deviceDescriptors)) {}

  bool isViable() const override {
    return true;
  }

  size_t numConnectionsNeeded() const override {
    return 1;
  }

  const std::unordered_map<Device, std::string>& deviceDescriptors()
      const override {
    return deviceDescriptors_;
  }

  bool canCommunicateWithRemote(
      const std::string& localDeviceDescriptor,
      const std::string& remoteDeviceDescriptor) const override {
    return localDeviceDescriptor == remoteDeviceDescriptor;
  }

  std::shared_ptr<channel::Channel> createChannel(
      std::vector<std::shared_ptr<transport::Connection>> /* unused */,
      channel::Endpoint /* unused */) override {
    return nullptr;
  }

  void setId(std::string /* unused */) override {}

  void close() override {}

  void join() override {}

 private:
  std::unordered_map<Device, std::string> deviceDescriptors_;
};

} // namespace

TEST(ChannelSelection, NoCompatibleChannel) {
  auto channelContext = MockChannelContext::create({
      {Device{kCpuDeviceType, 0}, "any"},
  });

  ChannelSelection channelSelection = selectChannels(
      {
          {0, std::make_tuple("foo_channel", channelContext)},
      },
      {
          {
              "bar_channel",
              {
                  {Device{kCpuDeviceType, 0}, "any"},
              },
          },
      });

  auto it = channelSelection.channelForDevicePair.find(
      {Device{kCpuDeviceType, 0}, Device{kCpuDeviceType, 0}});
  EXPECT_EQ(it, channelSelection.channelForDevicePair.end());
  EXPECT_TRUE(channelSelection.descriptorsMap.empty());
}

TEST(ChannelSelection, OneCompatibleChannel) {
  auto channelContext = MockChannelContext::create({
      {Device{kCpuDeviceType, 0}, "any"},
  });

  ChannelSelection channelSelection = selectChannels(
      {
          {0, std::make_tuple("foo_channel", channelContext)},
      },
      {
          {
              "foo_channel",
              {
                  {Device{kCpuDeviceType, 0}, "any"},
              },
          },
      });

  auto it = channelSelection.channelForDevicePair.find(
      {Device{kCpuDeviceType, 0}, Device{kCpuDeviceType, 0}});
  EXPECT_NE(it, channelSelection.channelForDevicePair.end());
  EXPECT_EQ(it->second, "foo_channel");
  EXPECT_EQ(
      channelSelection.descriptorsMap["foo_channel"],
      channelContext->deviceDescriptors());
}

TEST(ChannelSelection, SelectsHighestPriorityChannel) {
  auto fooChannelContext = MockChannelContext::create({
      {Device{kCpuDeviceType, 0}, "any"},
  });
  auto barChannelContext = MockChannelContext::create({
      {Device{kCpuDeviceType, 0}, "any"},
  });

  ChannelSelection channelSelection = selectChannels(
      {
          {0, std::make_tuple("foo_channel", fooChannelContext)},
          {-100, std::make_tuple("bar_channel", barChannelContext)},
      },
      {
          {
              "foo_channel",
              {
                  {Device{kCpuDeviceType, 0}, "any"},
              },
          },
          {
              "bar_channel",
              {
                  {Device{kCpuDeviceType, 0}, "any"},
              },
          },
      });

  auto it = channelSelection.channelForDevicePair.find(
      {Device{kCpuDeviceType, 0}, Device{kCpuDeviceType, 0}});
  EXPECT_NE(it, channelSelection.channelForDevicePair.end());
  EXPECT_EQ(it->second, "bar_channel");
  EXPECT_EQ(
      channelSelection.descriptorsMap["bar_channel"],
      barChannelContext->deviceDescriptors());
}
