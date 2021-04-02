/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#include <nop/types/variant.h>

#include <tensorpipe/common/device.h>

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
  uint64_t transportRegistrationId;
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
      transportRegistrationId,
      transportDomainDescriptor,
      channelRegistrationIds,
      channelDeviceDescriptors,
      channelForDevicePair);
};

struct MessageDescriptor {
  struct PayloadDescriptor {
    // This pointless constructor is needed to work around a bug in GCC 5.5 (and
    // possibly other versions). It appears to be needed in the nop types that
    // are used inside std::vectors.
    PayloadDescriptor() {}

    int64_t sizeInBytes;
    std::string metadata;
    NOP_STRUCTURE(PayloadDescriptor, sizeInBytes, metadata);
  };

  struct TensorDescriptor {
    // This pointless constructor is needed to work around a bug in GCC 5.5 (and
    // possibly other versions). It appears to be needed in the nop types that
    // are used inside std::vectors.
    TensorDescriptor() {}

    int64_t sizeInBytes;
    std::string metadata;

    Device sourceDevice;
    NOP_STRUCTURE(TensorDescriptor, sizeInBytes, metadata, sourceDevice);
  };

  std::string metadata;
  std::vector<PayloadDescriptor> payloadDescriptors;
  std::vector<TensorDescriptor> tensorDescriptors;
  NOP_STRUCTURE(
      MessageDescriptor,
      metadata,
      payloadDescriptors,
      tensorDescriptors);
};

using Packet = nop::Variant<
    SpontaneousConnection,
    RequestedConnection,
    Brochure,
    BrochureAnswer,
    MessageDescriptor>;

} // namespace tensorpipe
