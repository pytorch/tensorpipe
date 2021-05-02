/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/channel_selection.h>

namespace tensorpipe {

ChannelSelection selectChannels(
    const ContextImpl::TOrderedChannels& orderedChannels,
    const std::unordered_map<
        std::string,
        std::unordered_map<Device, std::string>>& remoteDescriptorsMap) {
  ChannelSelection result;

  for (const auto& channelIter : orderedChannels) {
    const std::string& channelName = std::get<0>(channelIter.second);
    const channel::Context& channelContext = *std::get<1>(channelIter.second);

    const auto& remoteDescriptorsMapIter =
        remoteDescriptorsMap.find(channelName);
    if (remoteDescriptorsMapIter == remoteDescriptorsMap.end()) {
      continue;
    }

    const std::unordered_map<Device, std::string>& localDeviceDescriptors =
        channelContext.deviceDescriptors();
    const std::unordered_map<Device, std::string>& remoteDeviceDescriptors =
        remoteDescriptorsMapIter->second;

    bool selected = false;
    for (const auto& localDescIter : localDeviceDescriptors) {
      const Device& localDevice = localDescIter.first;
      const std::string& localDeviceDescriptor = localDescIter.second;
      for (const auto& remoteDescIter : remoteDeviceDescriptors) {
        const Device& remoteDevice = remoteDescIter.first;
        const std::string& remoteDeviceDescriptor = remoteDescIter.second;

        if (!channelContext.canCommunicateWithRemote(
                localDeviceDescriptor, remoteDeviceDescriptor)) {
          continue;
        }

        if (result.channelForDevicePair.count({localDevice, remoteDevice}) !=
            0) {
          // A channel with higher priority has already been selected for this
          // device pair.
          continue;
        }

        selected = true;
        result.channelForDevicePair[{localDevice, remoteDevice}] = channelName;
      }
    }

    if (selected) {
      result.descriptorsMap[channelName] = localDeviceDescriptors;
    }
  }

  return result;
}

} // namespace tensorpipe
