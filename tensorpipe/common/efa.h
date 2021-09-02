/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#ifndef TENSORPIPE_COMMON_EFA_H_
#define TENSORPIPE_COMMON_EFA_H_


#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <rdma/fabric.h>
#include <rdma/fi_eq.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/nop.h>

static const int FABRIC_VERSION = FI_VERSION(1, 10);
static const int kMaxConcurrentWorkRequest = 4224;

namespace tensorpipe {

#define TP_CHECK_EFA_RET(ret, msg)                           \
  do {                                                       \
    if (ret < 0) {                                          \
      TP_THROW_ASSERT() << msg << ". Return Code: " << ret   \
                        << ". ERROR: " << fi_strerror(-ret); \
    }                                                        \
  } while (false)

struct FabricDeleter {
  void operator()(fi_info* info) {
    if (info)
      fi_freeinfo(info);
  }
  void operator()(fid* fid) {
    if (fid)
      fi_close(fid);
  }
  void operator()(fid_domain* fid) {
    if (fid)
      fi_close((fid_t)fid);
  }
  void operator()(fid_fabric* fid) {
    if (fid)
      fi_close((fid_t)fid);
  }
  void operator()(fid_cq* fid) {
    if (fid)
      fi_close((fid_t)fid);
  }
  void operator()(fid_av* fid) {
    if (fid)
      fi_close((fid_t)fid);
  }
  void operator()(fid_ep* fid) {
    if (fid)
      fi_close((fid_t)fid);
  }
  void operator()(fid_eq* fid) {
    if (fid)
      fi_close((fid_t)fid);
  }
};

template <typename T>
using UniqueFabricPtr = std::unique_ptr<T, FabricDeleter>;

struct FabricAddr {
  // endpoint name
  char name[64] = {};
  // length of endpoint name
  size_t len = sizeof(name);

  std::string DebugStr() const {
    std::stringstream ss;
    ss << "[";
    for (size_t i = 0; i < len; i++) {
      ss << std::to_string(name[i]) << ",";
    }
    ss << "]";
    return ss.str();
  }

  std::string str() const {
    return std::string(name, len);
  }

  void CopyFrom(void* ep_name, const size_t ep_name_len) {
    len = ep_name_len;
    memcpy(name, ep_name, sizeof(name));
  }

  void CopyTo(char* ep_name, size_t* ep_name_len) {
    *(ep_name_len) = len;
    memcpy(ep_name, name, sizeof(name));
  }
};

class FabricContext {
 public:
  // fabric top-level object
  UniqueFabricPtr<struct fid_fabric> fabric;
  // domains which maps to a specific local network interface adapter
  UniqueFabricPtr<struct fid_domain> domain;
  // completion queue
  UniqueFabricPtr<struct fid_cq> cq;
  // address vector
  UniqueFabricPtr<struct fid_av> av;
  // the endpoint
  UniqueFabricPtr<struct fid_ep> ep;
  // endpoint name
  struct FabricAddr addr;
  // readable endpoint name
  struct FabricAddr readable_addr;

 public:
  explicit FabricContext();

 private:
  UniqueFabricPtr<fi_info> getFabricInfo();
};

class FabricEndpoint {
 public:
  FabricEndpoint();

  fi_addr_t addPeerAddr(FabricAddr* addr);
  void removePeerAddr(fi_addr_t peer_addr);

  int post_send(void* buffer, size_t size, uint64_t tag, fi_addr_t dst_addr, void* context = nullptr);
  int post_recv(void* buffer, size_t size, uint64_t tag, fi_addr_t src_addr, uint64_t ignore, void* context = nullptr);

  int poll_cq(struct fi_cq_tagged_entry* cq_entries, fi_addr_t* src_addrs, size_t count);

  static bool isEfaAvailable();

  // Fabric Context contains everything
  std::unique_ptr<FabricContext> fabric_ctx;
};
} // namespace tensorpipe


#endif // TENSORPIPE_COMMON_EFA_H_
