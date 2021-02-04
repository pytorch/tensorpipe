/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/utility.h>

#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/transport/uv/error.h>
#include <tensorpipe/transport/uv/sockaddr.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

std::tuple<Error, std::string> lookupAddrForIface(std::string iface) {
  int rv;
  InterfaceAddresses addresses;
  int count;
  std::tie(rv, addresses, count) = getInterfaceAddresses();
  if (rv < 0) {
    return std::make_tuple(TP_CREATE_ERROR(UVError, rv), std::string());
  }

  for (auto i = 0; i < count; i++) {
    const uv_interface_address_t& interface = addresses[i];
    if (iface != interface.name) {
      continue;
    }

    const auto& address = interface.address;
    const struct sockaddr* sockaddr =
        reinterpret_cast<const struct sockaddr*>(&address);
    switch (sockaddr->sa_family) {
      case AF_INET:
        return std::make_tuple(
            Error::kSuccess,
            Sockaddr(sockaddr, sizeof(address.address4)).str());
      case AF_INET6:
        return std::make_tuple(
            Error::kSuccess,
            Sockaddr(sockaddr, sizeof(address.address6)).str());
    }
  }

  return std::make_tuple(TP_CREATE_ERROR(NoAddrFoundError), std::string());
}

std::tuple<Error, std::string> lookupAddrForHostname() {
  // For some operations we need a libuv event loop. We create a fresh one, just
  // for this purpose, which we'll drive inline from this thread. This way we
  // avoid misusing the main event loop in the context impl.
  struct InlineLoop {
    uv_loop_t loop;

    InlineLoop() {
      auto rv = uv_loop_init(&loop);
      TP_THROW_UV_IF(rv < 0, rv);
    }

    ~InlineLoop() {
      auto rv = uv_loop_close(&loop);
      TP_THROW_UV_IF(rv < 0, rv);
    }
  };
  InlineLoop loop;

  struct InlineDeferredExecutor : public DeferredExecutor {
    std::thread::id threadId = std::this_thread::get_id();

    void deferToLoop(TTask fn) override {
      TP_THROW_ASSERT()
          << "How could this be called?! This class is supposed to be "
          << "instantiated as const, and this method isn't const-qualified";
    }

    bool inLoop() const override {
      return std::this_thread::get_id() == threadId;
    }
  };
  const InlineDeferredExecutor executor;

  int rv;
  std::string hostname;
  std::tie(rv, hostname) = getHostname();
  if (rv < 0) {
    return std::make_tuple(TP_CREATE_ERROR(UVError, rv), std::string());
  }

  Addrinfo info;
  std::tie(rv, info) = getAddrinfoFromLoop(&loop.loop, std::move(hostname));
  if (rv < 0) {
    return std::make_tuple(TP_CREATE_ERROR(UVError, rv), std::string());
  }

  Error error;
  for (struct addrinfo* rp = info.get(); rp != nullptr; rp = rp->ai_next) {
    TP_DCHECK(rp->ai_family == AF_INET || rp->ai_family == AF_INET6);
    TP_DCHECK_EQ(rp->ai_socktype, SOCK_STREAM);
    TP_DCHECK_EQ(rp->ai_protocol, IPPROTO_TCP);

    Sockaddr addr = Sockaddr(rp->ai_addr, rp->ai_addrlen);

    TCPHandle handle(&loop.loop, executor);
    handle.initFromLoop();
    rv = handle.bindFromLoop(addr);
    handle.closeFromLoop();

    // The handle will only be closed at the next loop iteration, so run it.
    {
      auto rv = uv_run(&loop.loop, UV_RUN_DEFAULT);
      TP_THROW_ASSERT_IF(rv > 0);
    }

    if (rv < 0) {
      // Record the first binding error we encounter and return that in the end
      // if no working address is found, in order to help with debugging.
      if (!error) {
        error = TP_CREATE_ERROR(UVError, rv);
      }
      continue;
    }

    return std::make_tuple(Error::kSuccess, addr.str());
  }

  if (error) {
    return std::make_tuple(std::move(error), std::string());
  } else {
    return std::make_tuple(TP_CREATE_ERROR(NoAddrFoundError), std::string());
  }
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
