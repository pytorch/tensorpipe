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

#include <tensorpipe/common/defs.h>
#include <tensorpipe/core/channel.h>

namespace tensorpipe {
namespace {

template <
    typename T,
    typename std::enable_if<
        std::is_base_of<google::protobuf::MessageLite, T>::value,
        bool>::type = false>
Channel::TDescriptor saveDescriptor(const T& pb) {
  Channel::TDescriptor out;
  const auto len = pb.ByteSize();
  out.resize(len);
  const auto end = pb.SerializeWithCachedSizesToArray(out.data());
  TP_DCHECK_EQ(end, out.data() + len);
  return out;
}

template <
    typename T,
    typename std::enable_if<
        std::is_base_of<google::protobuf::MessageLite, T>::value,
        bool>::type = false>
T loadDescriptor(const Channel::TDescriptor& in) {
  T pb;
  const auto success = pb.ParseFromArray(in.data(), in.size());
  TP_THROW_ASSERT_IF(!success) << "Failed to parse protobuf message.";
  return pb;
}

} // namespace
} // namespace tensorpipe
