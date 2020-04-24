/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/helpers.h>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace channel {

Channel::TDescriptor saveDescriptor(const google::protobuf::MessageLite& pb) {
  Channel::TDescriptor out;
  const auto success = pb.SerializeToString(&out);
  TP_DCHECK(success) << "Failed to serialize protobuf message";
  return out;
}

void loadDescriptor(
    google::protobuf::MessageLite& pb,
    const Channel::TDescriptor& in) {
  const auto success = pb.ParseFromString(in);
  TP_DCHECK(success) << "Failed to parse protobuf message";
}

} // namespace channel
} // namespace tensorpipe
