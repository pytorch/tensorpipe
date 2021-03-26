/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sstream>
#include <stdexcept>
#include <string>

namespace tensorpipe {

enum class DeviceType {
  kCpu,
  kCuda,
};

const std::string kCpuDeviceType{"cpu"};
const std::string kCudaDeviceType{"cuda"};

struct Device {
  std::string type;
  int index;

  // FIXME: This method will disappear once XDTT channel selection is
  // implemented.
  DeviceType deviceType() const {
    if (type == "cpu") {
      return DeviceType::kCpu;
    } else if (type == "cuda") {
      return DeviceType::kCuda;
    } else {
      throw std::runtime_error("Invalid device type " + type);
    }
  }

  std::string toString() const {
    std::stringstream ss;
    ss << type << ":" << index;
    return ss.str();
  }

  bool operator==(const Device& other) const {
    return type == other.type && index == other.index;
  }
};

} // namespace tensorpipe

namespace std {

template <>
struct hash<::tensorpipe::Device> {
  size_t operator()(const ::tensorpipe::Device& device) const noexcept {
    return std::hash<std::string>{}(device.toString());
  }
};

} // namespace std
