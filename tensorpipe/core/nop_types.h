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

#include <tensorpipe/core/buffer.h>

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

struct ChannelAdvertisement {
  std::string domainDescriptor;
  NOP_STRUCTURE(ChannelAdvertisement, domainDescriptor);
};

struct Brochure {
  std::unordered_map<std::string, TransportAdvertisement>
      transportAdvertisement;
  std::unordered_map<std::string, ChannelAdvertisement> cpuChannelAdvertisement;
  std::unordered_map<std::string, ChannelAdvertisement>
      cudaChannelAdvertisement;
  NOP_STRUCTURE(
      Brochure,
      transportAdvertisement,
      cpuChannelAdvertisement,
      cudaChannelAdvertisement);
};

struct ChannelSelection {
  uint64_t registrationId;
  NOP_STRUCTURE(ChannelSelection, registrationId);
};

struct BrochureAnswer {
  std::string transport;
  std::string address;
  uint64_t registrationId;
  std::unordered_map<std::string, ChannelSelection> cpuChannelSelection;
  std::unordered_map<std::string, ChannelSelection> cudaChannelSelection;
  NOP_STRUCTURE(
      BrochureAnswer,
      transport,
      address,
      registrationId,
      cpuChannelSelection,
      cudaChannelSelection);
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
    std::string channelDescriptor;
    NOP_STRUCTURE(
        TensorDescriptor,
        sizeInBytes,
        metadata,
        deviceType,
        channelName,
        channelDescriptor);
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
