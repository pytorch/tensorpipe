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
  std::string address; // address for listen or connect
  int io_num{0}; // number of write/read pairs
  int chunk_bytes{8};
};

struct Options parseOptions(int argc, char** argv);

} // namespace benchmark
} // namespace tensorpipe
