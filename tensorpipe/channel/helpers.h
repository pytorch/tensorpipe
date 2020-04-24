/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

// Note: never include this file from headers!

#include <google/protobuf/message_lite.h>

#include <tensorpipe/channel/channel.h>

namespace tensorpipe {
namespace channel {

Channel::TDescriptor saveDescriptor(const google::protobuf::MessageLite& pb);

void loadDescriptor(
    google::protobuf::MessageLite& pb,
    const Channel::TDescriptor& in);

} // namespace channel
} // namespace tensorpipe
