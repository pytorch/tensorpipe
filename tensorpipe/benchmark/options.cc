/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/benchmark/options.h>

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>

namespace tensorpipe {
namespace benchmark {

static void usage(int status, const char* argv0) {
  if (status != EXIT_SUCCESS) {
    fprintf(stderr, "`%s --help' for more information.\n", argv0);
    exit(status);
  }

  fprintf(stderr, "Usage: %s [OPTIONS]\n", argv0);
#define X(x) fputs(x "\n", stderr);
  X("");
  X("--mode=MODE                      Running mode [listen|connect]");
  X("--transport=TRANSPORT            Transport backend [shm|uv]");
  X("--channel=CHANNEL                Channel backend [basic]");
  X("--address=ADDRESS                Address to listen or connect to");
  X("--num-round-trips=NUM            Number of write/read pairs to perform");
  X("--num-payloads=NUM [optional]    Number of payloads of each write/read pair");
  X("--payload-size=SIZE [optional]   Size of payload of each write/read pair");
  X("--num-tensors=NUM [optional]     Number of tensors of each write/read pair");
  X("--tensor-size=SIZE [optional]    Size of tensor of each write/read pair");
  X("--tensor-type=TYPE [optional]    Type of tensor (cpu or cuda)");
  X("--metadata-size=SIZE [optional]  Size of metadata of each write/read pair");
  X("--cuda-sync-period=NUM [optiona] Number of round-trips between two stream syncs");

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
    fprintf(stderr, "Missing argument: --num-round-trips must be set\n");
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
    NUM_PAYLOADS,
    PAYLOAD_SIZE,
    NUM_TENSORS,
    TENSOR_SIZE,
    TENSOR_TYPE,
    METADATA_SIZE,
    CUDA_SYNC_PERIOD,
    HELP,
  };

  static struct option longOptions[] = {
      {"mode", required_argument, &flag, MODE},
      {"transport", required_argument, &flag, TRANSPORT},
      {"channel", required_argument, &flag, CHANNEL},
      {"address", required_argument, &flag, ADDRESS},
      {"num-round-trips", required_argument, &flag, NUM_ROUND_TRIPS},
      {"num-payloads", required_argument, &flag, NUM_PAYLOADS},
      {"payload-size", required_argument, &flag, PAYLOAD_SIZE},
      {"num-tensors", required_argument, &flag, NUM_TENSORS},
      {"tensor-size", required_argument, &flag, TENSOR_SIZE},
      {"tensor-type", required_argument, &flag, TENSOR_TYPE},
      {"metadata-size", required_argument, &flag, METADATA_SIZE},
      {"cuda-sync-period", required_argument, &flag, CUDA_SYNC_PERIOD},
      {"help", no_argument, &flag, HELP},
      {nullptr, 0, nullptr, 0}};

  while (1) {
    opt = getopt_long(argc, argv, "", longOptions, nullptr);
    if (opt == -1) {
      break;
    }
    if (opt != 0) {
      usage(EXIT_FAILURE, argv[0]);
      break;
    }
    switch (flag) {
      case MODE:
        options.mode = std::string(optarg);
        if (options.mode != "listen" && options.mode != "connect") {
          fprintf(stderr, "Error:\n");
          fprintf(stderr, "  --mode must be [listen|connect]\n");
          exit(EXIT_FAILURE);
        }
        break;
      case TRANSPORT:
        options.transport = std::string(optarg);
        break;
      case CHANNEL:
        options.channel = std::string(optarg);
        break;
      case ADDRESS:
        options.address = std::string(optarg);
        break;
      case NUM_ROUND_TRIPS:
        options.numRoundTrips = std::strtol(optarg, nullptr, 10);
        break;
      case NUM_PAYLOADS:
        options.numPayloads = std::strtoull(optarg, nullptr, 10);
        break;
      case PAYLOAD_SIZE:
        options.payloadSize = std::strtoull(optarg, nullptr, 10);
        break;
      case NUM_TENSORS:
        options.numTensors = std::strtoull(optarg, nullptr, 10);
        break;
      case TENSOR_SIZE:
        options.tensorSize = std::strtoull(optarg, nullptr, 10);
        break;
      case TENSOR_TYPE:
        if (std::string(optarg) == "cpu") {
          options.tensorType = TensorType::kCpu;
        } else if (std::string(optarg) == "cuda") {
          options.tensorType = TensorType::kCuda;
        } else {
          fprintf(stderr, "Error:\n");
          fprintf(stderr, "  --tensor-type must be [cpu|cuda]\n");
          exit(EXIT_FAILURE);
        }
        break;
      case METADATA_SIZE:
        options.metadataSize = std::strtoull(optarg, nullptr, 10);
        break;
      case CUDA_SYNC_PERIOD:
        options.cudaSyncPeriod = std::strtoull(optarg, nullptr, 10);
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
