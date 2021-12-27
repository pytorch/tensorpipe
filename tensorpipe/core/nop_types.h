/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include <nop/serializer.h>
#include <nop/structure.h>
#include <nop/types/optional.h>
#include <nop/types/variant.h>

#include <tensorpipe/common/device.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/core/message.h>

namespace tensorpipe {

struct SpontaneousConnection {
  std::string contextName;
  NOP_STRUCTURE(SpontaneousConnection, contextName);
};

struct RequestedConnection {
  uint64_t registrationId;
  NOP_STRUCTURE(RequestedConnection, registrationId);
};

NOP_EXTERNAL_STRUCTURE(Device, type, index);

struct Brochure {
  std::unordered_map<std::string, std::string> transportDomainDescriptors;
  std::unordered_map<std::string, std::unordered_map<Device, std::string>>
      channelDeviceDescriptors;
  NOP_STRUCTURE(Brochure, transportDomainDescriptors, channelDeviceDescriptors);
};

struct BrochureAnswer {
  std::string transport;
  std::string address;
  std::unordered_map<uint64_t, uint64_t> transportRegistrationIds;
  std::string transportDomainDescriptor;
  std::unordered_map<std::string, std::vector<uint64_t>> channelRegistrationIds;
  std::unordered_map<std::string, std::unordered_map<Device, std::string>>
      channelDeviceDescriptors;
  std::unordered_map<std::pair<Device, Device>, std::string>
      channelForDevicePair;
  NOP_STRUCTURE(
      BrochureAnswer,
      transport,
      address,
      transportRegistrationIds,
      transportDomainDescriptor,
      channelRegistrationIds,
      channelDeviceDescriptors,
      channelForDevicePair);
};

NOP_EXTERNAL_STRUCTURE(Descriptor::Payload, length, metadata);
NOP_EXTERNAL_STRUCTURE(
    Descriptor::Tensor,
    length,
    sourceDevice,
    targetDevice,
    metadata);
NOP_EXTERNAL_STRUCTURE(Descriptor, metadata, payloads, tensors);

struct DescriptorReply {
  std::vector<Device> targetDevices;
  NOP_STRUCTURE(DescriptorReply, targetDevices);
};

using Packet = nop::Variant<SpontaneousConnection, RequestedConnection>;

} // namespace tensorpipe
