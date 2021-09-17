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

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/efa_lib.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/nop.h>

namespace tensorpipe {

static const int FABRIC_VERSION = FI_VERSION(1, 10);

#define TP_CHECK_EFA_RET(ret, msg)                          \
  do {                                                      \
    if (ret < 0) {                                          \
      TP_THROW_ASSERT() << msg << ". Return Code: " << ret; \
    }                                                       \
  } while (false)

struct FabricDeleter {
  void operator()(fi_info* info) {
    if (info)
      efaLib->fi_freeinfo_op(info);
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

  EfaLib* efaLib;
};

template <typename T>
using UniqueFabricPtr = std::unique_ptr<T, FabricDeleter>;

struct EfaAddress {
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

inline EfaLib::device* getEfaDevices(EfaLib& efaLib) {
  EfaLib::device* hints = efaLib.fi_dupinfo_op((const fi_info*)NULL);
  hints->mode = FI_CONTEXT;
  hints->ep_attr->type = FI_EP_RDM; // Reliable Datagram
  hints->caps = FI_TAGGED | FI_MSG | FI_REMOTE_COMM | FI_DIRECTED_RECV |
      FI_LOCAL_COMM | FI_SOURCE;
  hints->tx_attr->msg_order = FI_ORDER_SAS;
  hints->rx_attr->msg_order = FI_ORDER_SAS;
  hints->domain_attr->control_progress = FI_PROGRESS_AUTO;
  hints->domain_attr->data_progress = FI_PROGRESS_AUTO;
  hints->domain_attr->caps =
      FI_LOCAL_COMM | FI_REMOTE_COMM; // Enable local loopback
  hints->domain_attr->av_type = FI_AV_TABLE;
  hints->fabric_attr->prov_name = strdup("efa");
  // info.
  struct fi_info* info_;
  int ret =
      efaLib.fi_getinfo_op(FABRIC_VERSION, nullptr, nullptr, 0, hints, &info_);
  return info_;
}

using EfaFabric = UniqueFabricPtr<struct fid_fabric>;
inline EfaFabric createEfaFabric(EfaLib& efaLib, EfaLib::device* info) {
  struct fid_fabric* fabric_;
  int ret = efaLib.fi_fabric_op(info->fabric_attr, &fabric_, nullptr);
  TP_CHECK_EFA_RET(ret, "Couldn't open a fabric provider");
  return EfaFabric(fabric_, FabricDeleter{&efaLib});
}

using EfaDomain = UniqueFabricPtr<struct fid_domain>;
inline EfaDomain createEfaDomain(
    EfaLib& efaLib,
    EfaFabric& fabric,
    EfaLib::device* info) {
  struct fid_domain* domain_;
  int ret = fi_domain(fabric.get(), info, &domain_, nullptr);
  TP_CHECK_EFA_RET(ret, "Couldn't open a fabric access domain");
  return EfaDomain(domain_, FabricDeleter{&efaLib});
}

using EfaEndpoint = UniqueFabricPtr<struct fid_ep>;
inline EfaEndpoint createEfaEndpoint(
    EfaLib& efaLib,
    EfaDomain& domain,
    EfaLib::device* info) {
  struct fid_ep* ep_;
  int ret = fi_endpoint(domain.get(), info, &ep_, nullptr);
  TP_CHECK_EFA_RET(ret, "Couldn't allocate endpoint");
  return EfaEndpoint(ep_, FabricDeleter{&efaLib});
}

using EfaCompletionQueue = UniqueFabricPtr<struct fid_cq>;
inline EfaCompletionQueue createEfaCompletionQueue(
    EfaLib& efaLib,
    EfaDomain& domain,
    EfaLib::device* info) {
  struct fid_cq* cq_;
  struct fi_cq_attr cq_attr = {};
  cq_attr.format = FI_CQ_FORMAT_TAGGED;
  cq_attr.size = info->rx_attr->size;
  int ret = fi_cq_open(domain.get(), &cq_attr, &cq_, nullptr);
  TP_CHECK_EFA_RET(ret, "Couldn't open CQ");
  return EfaCompletionQueue(cq_, FabricDeleter{&efaLib});
}

using EfaAdressVector = UniqueFabricPtr<struct fid_av>;
inline EfaAdressVector createEfaAdressVector(
    EfaLib& efaLib,
    EfaDomain& domain) {
  struct fi_av_attr av_attr = {};
  struct fid_av* av_;
  int ret = fi_av_open(domain.get(), &av_attr, &av_, nullptr);
  TP_CHECK_EFA_RET(ret, "Couldn't open AV");
  return EfaAdressVector(av_, FabricDeleter{&efaLib});
}

inline EfaAddress enableEndpoint(
    EfaLib& efaLib,
    EfaEndpoint& ep,
    EfaAdressVector& av,
    EfaCompletionQueue& cq) {
  // fi_ep_bind: bind CQ and AV to the endpoint
  int ret;
  ret = fi_ep_bind(ep.get(), (fid_t)cq.get(), FI_RECV | FI_TRANSMIT);
  TP_CHECK_EFA_RET(ret, "Couldn't bind EP-CQ");
  ret = fi_ep_bind(ep.get(), (fid_t)av.get(), 0);
  TP_CHECK_EFA_RET(ret, "Couldn't bind EP-AV");

  // fi_enable: enable endpoint for communication
  ret = fi_enable(ep.get());
  TP_CHECK_EFA_RET(ret, "Couldn't enable endpoint");

  // fi_getname: get endpoint name
  EfaAddress addr;
  ret = fi_getname((fid_t)ep.get(), addr.name, &addr.len);
  TP_CHECK_EFA_RET(ret, "Call to fi_getname() failed");
  return addr;
}

class EfaDeviceList {
 private:
  EfaDeviceList(EfaLib& efaLib, EfaLib::device* ptr, int size)
      : deviceList_(ptr, Deleter{&efaLib}), size_(size) {}

 public:
  EfaDeviceList() = default;

  static std::tuple<Error, EfaDeviceList> create(EfaLib& efaLib) {
    int size;
    EfaLib::device* ptr = getEfaDevices(efaLib);
    EfaLib::device* first_ptr = ptr;
    if (ptr == nullptr) {
      return std::make_tuple(
          TP_CREATE_ERROR(SystemError, "fi_getinfo", -1), EfaDeviceList());
    }
    size = 1;
    while (ptr->next != nullptr) {
      ptr = ptr->next;
      size++;
    };
    return std::make_tuple(
        Error::kSuccess, EfaDeviceList(efaLib, first_ptr, size));
  }

  int size() {
    return size_;
  }

  EfaLib::device& operator[](int i) {
    EfaLib::device* ptr = deviceList_.get();
    for (int j = 0; j < i; j++) {
      ptr = ptr->next;
    }
    return *ptr;
  }

  void reset() {
    deviceList_.reset();
  }

 private:
  struct Deleter {
    void operator()(EfaLib::device* ptr) {
      efaLib->fi_freeinfo_op(ptr);
    }

    EfaLib* efaLib;
  };
  std::unique_ptr<EfaLib::device, Deleter> deviceList_;
  int size_;
};

} // namespace tensorpipe

#endif // TENSORPIPE_COMMON_EFA_H_
