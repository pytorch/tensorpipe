/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <unordered_map>
#include <utility>

#include <tensorpipe/common/device.h>
#include <tensorpipe/core/context_impl.h>

namespace tensorpipe {

struct ChannelSelection {
  std::unordered_map<std::string, std::unordered_map<Device, std::string>>
      descriptorsMap;
  std::unordered_map<std::pair<Device, Device>, std::string>
      channelForDevicePair;

  std::string toString() const;
};

ChannelSelection selectChannels(
    const ContextImpl::TOrderedChannels& orderedChannels,
    const std::unordered_map<
        std::string,
        std::unordered_map<Device, std::string>>& remoteDescriptorsMap);

} // namespace tensorpipe
