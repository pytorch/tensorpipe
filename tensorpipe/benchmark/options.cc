/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "tensorpipe/benchmark/options.h"

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <tensorpipe/channel/registry.h>
#include <tensorpipe/transport/registry.h>

namespace tensorpipe {
namespace benchmark {

void validateTransportContext(std::shared_ptr<transport::Context> context) {
  if (!context) {
    auto keys = TensorpipeTransportRegistry().keys();
    std::cout
        << "The transport you passed in is not supported. The following transports are valid: ";
    for (const auto& key : keys) {
      std::cout << key << ", ";
    }
    std::cout << "\n";
    exit(EXIT_FAILURE);
  }
}

void validateChannelContext(std::shared_ptr<channel::Context> context) {
  if (!context) {
    auto keys = TensorpipeChannelRegistry().keys();
    std::cout
        << "The channel you passed in is not supported. The following channels are valid: ";
    for (const auto& key : keys) {
      std::cout << key << ", ";
    }
    std::cout << "\n";
    exit(EXIT_FAILURE);
  }
}

static void usage(int status, const char* argv0) {
  if (status != EXIT_SUCCESS) {
    fprintf(stderr, "`%s --help' for more information.\n", argv0);
    exit(status);
  }

  fprintf(stderr, "Usage: %s [OPTIONS]\n", argv0);
#define X(x) fputs(x "\n", stderr);
  X("");
  X("--mode=MODE                     Running mode [listen|connect]");
  X("--transport=TRANSPORT           Transport backend [shm|uv]");
  X("--channel=CHANNEL               Channel backend [basic]");
  X("--address=ADDRESS               Address to listen or connect to");
  X("--num-round-trips=NUM           Number of write/read pairs to perform");
  X("--payload-size=SIZE [optional]  Size of payload of each write/read pair");
  X("--tensor-size=SIZE [optional]   Size of tensor of each write/read pair");
  X("--metadata-size=SIZE [optional] Size of metadata of each write/read pair");

  exit(status);
}

static void validateOptions(Options options, const char* argv0) {
  int status = EXIT_SUCCESS;
  if (options.mode.empty()) {
    fprintf(stderr, "Missing argument: --mode must be set\n");
    status = EXIT_FAILURE;
  }
  if (options.transport.empty()) {
    fprintf(stderr, "Missing argument: --transport must be set\n");
    status = EXIT_FAILURE;
  }
  if (options.address.empty()) {
    fprintf(stderr, "Missing argument: --address must be set\n");
    status = EXIT_FAILURE;
  }
  if (options.numRoundTrips <= 0) {
    fprintf(stderr, "Missing argument: --io-num must be set\n");
    status = EXIT_FAILURE;
  }
  if (status != EXIT_SUCCESS) {
    usage(status, argv0);
  }
}

struct Options parseOptions(int argc, char** argv) {
  struct Options options;
  int opt;
  int flag = -1;

  enum Flags : int {
    MODE,
    TRANSPORT,
    CHANNEL,
    ADDRESS,
    NUM_ROUND_TRIPS,
    PAYLOAD_SIZE,
    TENSOR_SIZE,
    METADATA_SIZE,
    HELP,
  };

  static struct option long_options[] = {
      {"mode", required_argument, &flag, MODE},
      {"transport", required_argument, &flag, TRANSPORT},
      {"channel", required_argument, &flag, CHANNEL},
      {"address", required_argument, &flag, ADDRESS},
      {"num-round-trips", required_argument, &flag, NUM_ROUND_TRIPS},
      {"payload-size", required_argument, &flag, PAYLOAD_SIZE},
      {"tensor-size", required_argument, &flag, TENSOR_SIZE},
      {"metadata-size", required_argument, &flag, METADATA_SIZE},
      {"help", no_argument, &flag, HELP},
      {nullptr, 0, nullptr, 0}};

  while (1) {
    opt = getopt_long(argc, argv, "", long_options, nullptr);
    if (opt == -1) {
      break;
    }
    if (opt != 0) {
      usage(EXIT_FAILURE, argv[0]);
      break;
    }
    switch (flag) {
      case MODE:
        options.mode = std::string(optarg, strlen(optarg));
        if (options.mode != "listen" && options.mode != "connect") {
          fprintf(stderr, "Error:\n");
          fprintf(stderr, "  --mode must be [listen|connect]\n");
          exit(EXIT_FAILURE);
        }
        break;
      case TRANSPORT:
        options.transport = std::string(optarg, strlen(optarg));
        break;
      case CHANNEL:
        options.channel = std::string(optarg, strlen(optarg));
        break;
      case ADDRESS:
        options.address = std::string(optarg, strlen(optarg));
        break;
      case NUM_ROUND_TRIPS:
        options.numRoundTrips = atoi(optarg);
        break;
      case PAYLOAD_SIZE:
        options.payloadSize = atoi(optarg);
        break;
      case TENSOR_SIZE:
        options.tensorSize = atoi(optarg);
        break;
      case METADATA_SIZE:
        options.metadataSize = atoi(optarg);
        break;
      case HELP:
        usage(EXIT_SUCCESS, argv[0]);
        break;
      default:
        usage(EXIT_FAILURE, argv[0]);
        break;
    }
  }

  validateOptions(options, argv[0]);

  return options;
}

} // namespace benchmark
} // namespace tensorpipe
