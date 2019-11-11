#pragma once

#include <cstdio>
#include <cstdlib>

#include <uv.h>

// Note: this file must only be included from source files!

#define UV_ASSERT(rv, prefix) \
  do {                        \
    if ((rv) != 0) {          \
      fprintf(                \
          stderr,             \
          "[%s:%d] %s: %s\n", \
          __FILE__,           \
          __LINE__,           \
          prefix,             \
          uv_strerror(rv));   \
      abort();                \
    }                         \
  } while (0);
