/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/sockaddr.h>

#include <netinet/in.h>

#include <gtest/gtest.h>

using namespace tensorpipe::transport;

namespace {

int family(const uv::Sockaddr& addr) {
  auto sockaddr = addr.addr();
  return sockaddr->sa_family;
}

int port(const uv::Sockaddr& addr) {
  auto sockaddr = addr.addr();
  if (sockaddr->sa_family == AF_INET) {
    auto in = reinterpret_cast<const struct sockaddr_in*>(sockaddr);
    return in->sin_port;
  }
  if (sockaddr->sa_family == AF_INET6) {
    auto in6 = reinterpret_cast<const struct sockaddr_in6*>(sockaddr);
    return in6->sin6_port;
  }
  return -1;
}

} // namespace

TEST(UvSockaddr, InetBadPort) {
  ASSERT_THROW(
      uv::Sockaddr::createInetSockAddr("1.2.3.4:-1"), std::invalid_argument);
  ASSERT_THROW(
      uv::Sockaddr::createInetSockAddr("1.2.3.4:65536"), std::invalid_argument);
}

TEST(UvSockaddr, Inet) {
  {
    auto sa = uv::Sockaddr::createInetSockAddr("1.2.3.4:5");
    ASSERT_EQ(family(sa), AF_INET);
    ASSERT_EQ(port(sa), ntohs(5));
    ASSERT_EQ(sa.str(), "1.2.3.4:5");
  }

  {
    auto sa = uv::Sockaddr::createInetSockAddr("1.2.3.4:0");
    ASSERT_EQ(family(sa), AF_INET);
    ASSERT_EQ(port(sa), 0);
    ASSERT_EQ(sa.str(), "1.2.3.4:0");
  }

  {
    auto sa = uv::Sockaddr::createInetSockAddr("1.2.3.4");
    ASSERT_EQ(family(sa), AF_INET);
    ASSERT_EQ(port(sa), 0);
    ASSERT_EQ(sa.str(), "1.2.3.4:0");
  }
}

TEST(UvSockaddr, Inet6BadPort) {
  ASSERT_THROW(
      uv::Sockaddr::createInetSockAddr("[::1]:-1"), std::invalid_argument);
  ASSERT_THROW(
      uv::Sockaddr::createInetSockAddr("[::1]:65536"), std::invalid_argument);
  ASSERT_THROW(
      uv::Sockaddr::createInetSockAddr("]::1["), std::invalid_argument);
}

// Interface name conventions change based on platform. Linux uses "lo", OSX
// uses lo0, Windows uses integers.
#ifdef __linux__
#define LOOPBACK_INTERFACE "lo"
#elif __APPLE__
#define LOOPBACK_INTERFACE "lo0"
#endif

TEST(UvSockaddr, Inet6) {
  {
    auto sa = uv::Sockaddr::createInetSockAddr("[::1]:5");
    ASSERT_EQ(family(sa), AF_INET6);
    ASSERT_EQ(port(sa), ntohs(5));
    ASSERT_EQ(sa.str(), "[::1]:5");
  }

  {
    auto sa = uv::Sockaddr::createInetSockAddr("[::1]:0");
    ASSERT_EQ(family(sa), AF_INET6);
    ASSERT_EQ(port(sa), 0);
    ASSERT_EQ(sa.str(), "[::1]:0");
  }

  {
    auto sa = uv::Sockaddr::createInetSockAddr("::1");
    ASSERT_EQ(family(sa), AF_INET6);
    ASSERT_EQ(port(sa), 0);
    ASSERT_EQ(sa.str(), "[::1]:0");
  }

#ifdef LOOPBACK_INTERFACE
  {
    auto sa = uv::Sockaddr::createInetSockAddr("::1%" LOOPBACK_INTERFACE);
    ASSERT_EQ(family(sa), AF_INET6);
    ASSERT_EQ(port(sa), 0);
    ASSERT_EQ(sa.str(), "[::1%" LOOPBACK_INTERFACE "]:0");
  }

  {
    sockaddr_in6 sa;
    std::memset(&sa, 0, sizeof(sa));
    sa.sin6_family = AF_INET6;
    sa.sin6_port = ntohs(42);
    sa.sin6_flowinfo = 0;
    sa.sin6_addr.s6_addr[15] = 1;
    // Implicitly assuming that the loopback interface is the first one.
    sa.sin6_scope_id = 1;
    uv::Sockaddr tpSa(reinterpret_cast<sockaddr*>(&sa), sizeof(sa));
    ASSERT_EQ(tpSa.str(), "[::1%" LOOPBACK_INTERFACE "]:42");
  }
#endif
}
