/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/sockaddr.h>

#include <array>
#include <cstring>
#include <sstream>
#include <utility>

#include <uv.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

Sockaddr Sockaddr::createInetSockAddr(const std::string& str) {
  int port = 0;
  std::string addrStr;
  std::string portStr;

  // If the input string is an IPv6 address with port, the address
  // itself must be wrapped with brackets.
  if (addrStr.empty()) {
    auto start = str.find("[");
    auto stop = str.find("]");
    if (start < stop && start != std::string::npos &&
        stop != std::string::npos) {
      addrStr = str.substr(start + 1, stop - (start + 1));
      if (stop + 1 < str.size() && str[stop + 1] == ':') {
        portStr = str.substr(stop + 2);
      }
    }
  }

  // If the input string is an IPv4 address with port, we expect
  // at least a single period and a single colon in the string.
  if (addrStr.empty()) {
    auto period = str.find(".");
    auto colon = str.find(":");
    if (period != std::string::npos && colon != std::string::npos) {
      addrStr = str.substr(0, colon);
      portStr = str.substr(colon + 1);
    }
  }

  // Fallback to using entire input string as address without port.
  if (addrStr.empty()) {
    addrStr = str;
  }

  // Parse port number if specified.
  if (!portStr.empty()) {
    port = std::stoi(portStr);
    if (port < 0 || port > std::numeric_limits<uint16_t>::max()) {
      TP_THROW_EINVAL() << str;
    }
  }

  // Try to convert an IPv4 address.
  {
    struct sockaddr_in addr;
    auto rv = uv_ip4_addr(addrStr.c_str(), port, &addr);
    if (rv == 0) {
      return Sockaddr(reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    }
  }

  // Try to convert an IPv6 address.
  {
    struct sockaddr_in6 addr;
    auto rv = uv_ip6_addr(addrStr.c_str(), port, &addr);
    if (rv == 0) {
      return Sockaddr(reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    }
  }

  // Invalid address.
  TP_THROW_EINVAL() << str;

  // Return bogus to silence "return from non-void function" warning.
  // Note: we don't reach this point per the throw above.
  return Sockaddr(nullptr, 0);
}

std::string Sockaddr::str() const {
  std::ostringstream oss;

  if (addr_.ss_family == AF_INET) {
    std::array<char, 64> buf;
    auto in = reinterpret_cast<const struct sockaddr_in*>(&addr_);
    auto rv = uv_ip4_name(in, buf.data(), buf.size());
    TP_THROW_UV_IF(rv < 0, rv);
    oss << buf.data() << ":" << htons(in->sin_port);
  } else if (addr_.ss_family == AF_INET6) {
    std::array<char, 64> buf;
    auto in6 = reinterpret_cast<const struct sockaddr_in6*>(&addr_);
    auto rv = uv_ip6_name(in6, buf.data(), buf.size());
    TP_THROW_UV_IF(rv < 0, rv);
    oss << "[" << buf.data();
    if (in6->sin6_scope_id > 0) {
      std::array<char, UV_IF_NAMESIZE> scopeBuf;
      size_t size = sizeof(scopeBuf);
      rv = uv_if_indextoiid(in6->sin6_scope_id, scopeBuf.data(), &size);
      TP_THROW_UV_IF(rv < 0, rv);
      oss << "%" << scopeBuf.data();
    }
    oss << "]:" << htons(in6->sin6_port);
  } else {
    TP_THROW_EINVAL() << "invalid address family: " << addr_.ss_family;
  }

  return oss.str();
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
