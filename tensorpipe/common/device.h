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

  // This pointless constructor is needed to work around a bug in GCC 5.5 (and
  // possibly other versions). It appears to be needed in the nop types that
  // are used inside nop::Optional.
  Device() {}

  Device(std::string type, int index) : type(std::move(type)), index(index) {}

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

template <>
struct hash<std::pair<::tensorpipe::Device, ::tensorpipe::Device>> {
  size_t operator()(const std::pair<::tensorpipe::Device, ::tensorpipe::Device>&
                        p) const noexcept {
    size_t h1 = std::hash<::tensorpipe::Device>{}(p.first);
    size_t h2 = std::hash<::tensorpipe::Device>{}(p.second);
    // Shifting one hash to avoid collisions between (a, b) and (b, a).
    return h1 ^ (h2 << 1);
  }
};

} // namespace std
