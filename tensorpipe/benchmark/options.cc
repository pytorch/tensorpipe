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
  X("-m, --mode=MODE                    Running mode [listen|connect]");
  X("-t, --transport=TRANSPORT          Transport backend [shm|uv]");
  X("-a, --address=ADDRESS              Address to listen or connect to");
  X("-n, --io-num=NUM                   Number of write/read pairs to perform");
  X("-c, --chunk-bytes=SIZE [Optional]  Chunk bytes of each write/read pair");

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

  static struct option long_options[] = {
      {"mode", required_argument, nullptr, 'm'},
      {"transport", required_argument, nullptr, 't'},
      {"address", required_argument, nullptr, 'a'},
      {"io-num", required_argument, nullptr, 'n'},
      {"chunk-bytes", optional_argument, nullptr, 'c'},
      {"help", no_argument, nullptr, 'h'},
      {0, 0, 0, 0}};

  int opt;
  while (1) {
    int option_index = 0;
    opt = getopt_long(argc, argv, "m:t:a:n:c:", long_options, &option_index);
    if (opt == -1) {
      break;
    }
    switch (opt) {
      case 'm':
        options.mode = std::string(optarg, strlen(optarg));
        if (options.mode != "listen" && options.mode != "connect") {
          fprintf(stderr, "Error:\n");
          fprintf(stderr, "  --mode must be [listen|connect]\n");
          exit(EXIT_FAILURE);
        }
        break;
      case 't':
        options.transport = std::string(optarg, strlen(optarg));
        if (options.transport != "shm" && options.transport != "uv") {
          fprintf(stderr, "Error:\n");
          fprintf(stderr, "  --transport must be [shm|uv]\n");
          exit(EXIT_FAILURE);
        }
        break;
      case 'a':
        options.address = std::string(optarg, strlen(optarg));
        break;
      case 'n':
        options.ioNum = atoi(optarg);
        break;
      case 'c':
        options.chunkBytes = atoi(optarg);
        break;
      case 'h':
        // help
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
