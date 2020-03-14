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
  X("--mode=MODE                    Running mode [listen|connect]");
  X("--transport=TRANSPORT          Transport backend [shm|uv]");
  X("--address=ADDRESS              Address to listen or connect to");
  X("--io-num=NUM                   Number of write/read pairs to perform");
  X("--chunk-bytes=SIZE [Optional]  Chunk bytes of each write/read pair");

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
  if (options.ioNum <= 0) {
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
    ADDRESS,
    IO_NUM,
    CHUNK_BYTES,
    HELP,
  };

  static struct option long_options[] = {
      {"mode", required_argument, &flag, MODE},
      {"transport", required_argument, &flag, TRANSPORT},
      {"address", required_argument, &flag, ADDRESS},
      {"io-num", required_argument, &flag, IO_NUM},
      {"chunk-bytes", required_argument, &flag, CHUNK_BYTES},
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
        if (options.transport != "shm" && options.transport != "uv") {
          fprintf(stderr, "Error:\n");
          fprintf(stderr, "  --transport must be [shm|uv]\n");
          exit(EXIT_FAILURE);
        }
        break;
      case ADDRESS:
        options.address = std::string(optarg, strlen(optarg));
        break;
      case IO_NUM:
        options.ioNum = atoi(optarg);
        break;
      case CHUNK_BYTES:
        options.chunkBytes = atoi(optarg);
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
