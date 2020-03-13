/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

TEST(Sockaddr, InetBadPort) {
  ASSERT_THROW(
      uv::Sockaddr::createInetSockAddr("1.2.3.4:-1"), std::invalid_argument);
  ASSERT_THROW(
      uv::Sockaddr::createInetSockAddr("1.2.3.4:65536"), std::invalid_argument);
}

TEST(Sockaddr, Inet) {
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

TEST(Sockaddr, Inet6BadPort) {
  ASSERT_THROW(
      uv::Sockaddr::createInetSockAddr("[::1]:-1"), std::invalid_argument);
  ASSERT_THROW(
      uv::Sockaddr::createInetSockAddr("[::1]:65536"), std::invalid_argument);
  ASSERT_THROW(
      uv::Sockaddr::createInetSockAddr("]::1["), std::invalid_argument);
}

TEST(Sockaddr, Inet6) {
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
}
