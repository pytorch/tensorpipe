/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/ibv/sockaddr.h>

#include <array>
#include <cstring>
#include <sstream>
#include <utility>

#include <arpa/inet.h>
#include <net/if.h>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

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
    std::memset(&addr, 0, sizeof(addr));
    auto rv = inet_pton(AF_INET, addrStr.c_str(), &addr.sin_addr);
    TP_THROW_SYSTEM_IF(rv < 0, errno);
    if (rv == 1) {
      addr.sin_family = AF_INET;
      addr.sin_port = ntohs(port);
      return Sockaddr(reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    }
  }

  // Try to convert an IPv6 address.
  {
    struct sockaddr_in6 addr;
    std::memset(&addr, 0, sizeof(addr));

    auto interfacePos = addrStr.find('%');
    if (interfacePos != std::string::npos) {
      addr.sin6_scope_id =
          if_nametoindex(addrStr.substr(interfacePos + 1).c_str());
      addrStr = addrStr.substr(0, interfacePos);
    }

    auto rv = inet_pton(AF_INET6, addrStr.c_str(), &addr.sin6_addr);
    TP_THROW_SYSTEM_IF(rv < 0, errno);
    if (rv == 1) {
      addr.sin6_family = AF_INET6;
      addr.sin6_port = ntohs(port);
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
    auto rv = inet_ntop(AF_INET, &in->sin_addr, buf.data(), buf.size());
    TP_THROW_SYSTEM_IF(rv == nullptr, errno);
    oss << buf.data() << ":" << htons(in->sin_port);
  } else if (addr_.ss_family == AF_INET6) {
    std::array<char, 64> buf;
    auto in6 = reinterpret_cast<const struct sockaddr_in6*>(&addr_);
    auto rv = inet_ntop(AF_INET6, &in6->sin6_addr, buf.data(), buf.size());
    TP_THROW_SYSTEM_IF(rv == nullptr, errno);
    oss << "[" << buf.data();
    if (in6->sin6_scope_id > 0) {
      std::array<char, IF_NAMESIZE> scopeBuf;
      rv = if_indextoname(in6->sin6_scope_id, scopeBuf.data());
      TP_THROW_SYSTEM_IF(rv == nullptr, errno);
      oss << "%" << scopeBuf.data();
    }
    oss << "]:" << htons(in6->sin6_port);

  } else {
    TP_THROW_EINVAL() << "invalid address family: " << addr_.ss_family;
  }

  return oss.str();
}

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
