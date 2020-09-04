/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <vector>

#include <nop/serializer.h>
#include <nop/structure.h>
#include <nop/types/variant.h>

namespace tensorpipe {
namespace channel {
namespace mpt {

struct LaneAdvertisement {
  std::string address;
  uint64_t registrationId;
  NOP_STRUCTURE(LaneAdvertisement, address, registrationId);
};

struct ServerHello {
  std::vector<LaneAdvertisement> laneAdvertisements;
  NOP_STRUCTURE(ServerHello, laneAdvertisements);
};

struct ClientHello {
  uint64_t registrationId;
  NOP_STRUCTURE(ClientHello, registrationId);
};

using Packet = nop::Variant<ServerHello, ClientHello>;

} // namespace mpt
} // namespace channel
} // namespace tensorpipe
