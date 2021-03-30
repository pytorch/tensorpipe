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

#include <tensorpipe/common/buffer.h>
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

struct TransportAdvertisement {
  std::string domainDescriptor;
  NOP_STRUCTURE(TransportAdvertisement, domainDescriptor);
};

NOP_EXTERNAL_STRUCTURE(Device, type, index);

struct ChannelAdvertisement {
  std::unordered_map<Device, std::string> deviceDescriptors;
  NOP_STRUCTURE(ChannelAdvertisement, deviceDescriptors);
};

struct Brochure {
  std::unordered_map<std::string, TransportAdvertisement>
      transportAdvertisement;
  std::unordered_map<std::string, ChannelAdvertisement> channelAdvertisement;
  NOP_STRUCTURE(Brochure, transportAdvertisement, channelAdvertisement);
};

struct ChannelSelection {
  std::vector<uint64_t> registrationIds;
  std::unordered_map<Device, std::string> deviceDescriptors;
  NOP_STRUCTURE(ChannelSelection, registrationIds, deviceDescriptors);
};

struct BrochureAnswer {
  std::string transport;
  std::string address;
  uint64_t transportRegistrationId;
  std::string transportDomainDescriptor;
  std::unordered_map<std::string, ChannelSelection> channelSelection;
  NOP_STRUCTURE(
      BrochureAnswer,
      transport,
      address,
      transportRegistrationId,
      transportDomainDescriptor,
      channelSelection);
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

    DeviceType deviceType;
    std::string channelName;
    NOP_STRUCTURE(
        TensorDescriptor,
        sizeInBytes,
        metadata,
        deviceType,
        channelName);
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
