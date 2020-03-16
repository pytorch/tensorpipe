/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

namespace tensorpipe {
namespace benchmark {

struct Options {
  std::string mode; // server or client
  std::string transport; // shm or uv
  std::string channel; // basic
  std::string address; // address for listen or connect
  int numRoundTrips{0}; // number of write/read pairs
  size_t payloadSize{0};
  size_t tensorSize{0};
  size_t metadataSize{0};
};

struct Options parseOptions(int argc, char** argv);

} // namespace benchmark
} // namespace tensorpipe
