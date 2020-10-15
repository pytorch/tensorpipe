/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <infiniband/verbs.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/dl.h>

namespace tensorpipe {

// Master list of all symbols we care about from libibverbs.

#define TP_FORALL_IBV_SYMBOLS(_)                                      \
  _(ack_async_event, void, (IbvLib::async_event*))                    \
  _(alloc_pd, IbvLib::pd*, (IbvLib::context*))                        \
  _(close_device, int, (IbvLib::context*))                            \
  _(create_cq,                                                        \
    IbvLib::cq*,                                                      \
    (IbvLib::context*, int, void*, IbvLib::comp_channel*, int))       \
  _(create_qp, IbvLib::qp*, (IbvLib::pd*, IbvLib::qp_init_attr*))     \
  _(create_srq, IbvLib::srq*, (IbvLib::pd*, IbvLib::srq_init_attr*))  \
  _(dealloc_pd, int, (IbvLib::pd*))                                   \
  _(dereg_mr, int, (IbvLib::mr*))                                     \
  _(destroy_cq, int, (IbvLib::cq*))                                   \
  _(destroy_qp, int, (IbvLib::qp*))                                   \
  _(destroy_srq, int, (IbvLib::srq*))                                 \
  _(event_type_str, const char*, (IbvLib::event_type))                \
  _(free_device_list, void, (IbvLib::device**))                       \
  _(get_async_event, int, (IbvLib::context*, IbvLib::async_event*))   \
  _(get_device_list, IbvLib::device**, (int*))                        \
  _(modify_qp, int, (IbvLib::qp*, IbvLib::qp_attr*, int))             \
  _(open_device, IbvLib::context*, (IbvLib::device*))                 \
  _(query_gid, int, (IbvLib::context*, uint8_t, int, IbvLib::gid*))   \
  _(query_port, int, (IbvLib::context*, uint8_t, IbvLib::port_attr*)) \
  _(reg_mr, IbvLib::mr*, (IbvLib::pd*, void*, size_t, int))           \
  _(wc_status_str, const char*, (IbvLib::wc_status))

// Wrapper for libibverbs.

class IbvLib {
 public:
  using async_event = struct ibv_async_event;
  using comp_channel = struct ibv_comp_channel;
  using context = struct ibv_context;
  using cq = struct ibv_cq;
  using device = struct ibv_device;
  using event_type = enum ibv_event_type;
  using gid = union ibv_gid;
  using mr = struct ibv_mr;
  using mtu = enum ibv_mtu;
  using pd = struct ibv_pd;
  using port_attr = struct ibv_port_attr;
  using qp = struct ibv_qp;
  using qp_attr = struct ibv_qp_attr;
  using qp_init_attr = struct ibv_qp_init_attr;
  using recv_wr = struct ibv_recv_wr;
  using send_wr = struct ibv_send_wr;
  using sge = struct ibv_sge;
  using srq = struct ibv_srq;
  using srq_init_attr = struct ibv_srq_init_attr;
  using wc = struct ibv_wc;
  using wc_opcode = enum ibv_wc_opcode;
  using wc_status = enum ibv_wc_status;

  constexpr static auto ACCESS_LOCAL_WRITE = IBV_ACCESS_LOCAL_WRITE;
  constexpr static auto ACCESS_REMOTE_WRITE = IBV_ACCESS_REMOTE_WRITE;
  constexpr static auto QP_ACCESS_FLAGS = IBV_QP_ACCESS_FLAGS;
  constexpr static auto QP_AV = IBV_QP_AV;
  constexpr static auto QP_DEST_QPN = IBV_QP_DEST_QPN;
  constexpr static auto QP_MAX_DEST_RD_ATOMIC = IBV_QP_MAX_DEST_RD_ATOMIC;
  constexpr static auto QP_MAX_QP_RD_ATOMIC = IBV_QP_MAX_QP_RD_ATOMIC;
  constexpr static auto QP_MIN_RNR_TIMER = IBV_QP_MIN_RNR_TIMER;
  constexpr static auto QP_PATH_MTU = IBV_QP_PATH_MTU;
  constexpr static auto QP_PKEY_INDEX = IBV_QP_PKEY_INDEX;
  constexpr static auto QP_PORT = IBV_QP_PORT;
  constexpr static auto QP_RETRY_CNT = IBV_QP_RETRY_CNT;
  constexpr static auto QP_RNR_RETRY = IBV_QP_RNR_RETRY;
  constexpr static auto QP_RQ_PSN = IBV_QP_RQ_PSN;
  constexpr static auto QPS_ERR = IBV_QPS_ERR;
  constexpr static auto QPS_INIT = IBV_QPS_INIT;
  constexpr static auto QP_SQ_PSN = IBV_QP_SQ_PSN;
  constexpr static auto QPS_RTR = IBV_QPS_RTR;
  constexpr static auto QPS_RTS = IBV_QPS_RTS;
  constexpr static auto QP_STATE = IBV_QP_STATE;
  constexpr static auto QP_TIMEOUT = IBV_QP_TIMEOUT;
  constexpr static auto WC_BIND_MW = IBV_WC_BIND_MW;
  constexpr static auto WC_COMP_SWAP = IBV_WC_COMP_SWAP;
  constexpr static auto WC_FETCH_ADD = IBV_WC_FETCH_ADD;
  constexpr static auto WC_RDMA_READ = IBV_WC_RDMA_READ;
  constexpr static auto WC_RDMA_WRITE = IBV_WC_RDMA_WRITE;
  constexpr static auto WC_RECV = IBV_WC_RECV;
  constexpr static auto WC_RECV_RDMA_WITH_IMM = IBV_WC_RECV_RDMA_WITH_IMM;
  constexpr static auto WC_SEND = IBV_WC_SEND;

 private:
  IbvLib(DynamicLibraryHandle dlhandle) : dlhandle_(std::move(dlhandle)) {}

  DynamicLibraryHandle dlhandle_;

#define TP_DECLARE_FIELD(function_name, return_type, args_types) \
  return_type(*function_name##_ptr_) args_types = nullptr;
  TP_FORALL_IBV_SYMBOLS(TP_DECLARE_FIELD)
#undef TP_DECLARE_FIELD

 public:
  IbvLib() = default;

  static std::tuple<Error, IbvLib> create() {
    Error error;
    DynamicLibraryHandle dlhandle;
    std::tie(error, dlhandle) =
        createDynamicLibraryHandle("libibverbs.so.1", RTLD_LOCAL | RTLD_LAZY);
    if (error) {
      return {error, IbvLib()};
    }
    IbvLib lib(std::move(dlhandle));
#define TP_LOAD_SYMBOL(function_name, return_type, args_types)               \
  {                                                                          \
    void* ptr;                                                               \
    std::tie(error, ptr) = loadSymbol(lib.dlhandle_, "ibv_" #function_name); \
    if (error) {                                                             \
      return {error, IbvLib()};                                              \
    }                                                                        \
    TP_THROW_ASSERT_IF(ptr == nullptr);                                      \
    lib.function_name##_ptr_ =                                               \
        reinterpret_cast<decltype(function_name##_ptr_)>(ptr);               \
  }
    TP_FORALL_IBV_SYMBOLS(TP_LOAD_SYMBOL)
#undef TP_LOAD_SYMBOL
    return {Error::kSuccess, std::move(lib)};
  }

#define TP_FORWARD_CALL(function_name, return_type, args_types)  \
  template <typename... Args>                                    \
  auto function_name(Args&&... args) const {                     \
    return (*function_name##_ptr_)(std::forward<Args>(args)...); \
  }
  TP_FORALL_IBV_SYMBOLS(TP_FORWARD_CALL)
#undef TP_FORWARD_CALL

  // These functions (which, it would seem, are the ones that are used in the
  // critical control path, and which thus must have the lowest latency and
  // avoid any syscall/kernel overhead) are not exposed as symbols of
  // libibverbs.so: they are defined inline in the header and, in fact, they
  // access a function pointer stored on the ibv_context and execute it.

  int poll_cq(IbvLib::cq* cq, int num_entries, IbvLib::wc* wc) {
    return cq->context->ops.poll_cq(cq, num_entries, wc);
  }

  int post_send(IbvLib::qp* qp, IbvLib::send_wr* wr, IbvLib::send_wr** bad_wr) {
    return qp->context->ops.post_send(qp, wr, bad_wr);
  }

  int post_srq_recv(
      IbvLib::srq* srq,
      IbvLib::recv_wr* recv_wr,
      IbvLib::recv_wr** bad_recv_wr) {
    return srq->context->ops.post_srq_recv(srq, recv_wr, bad_recv_wr);
  }
};

#undef TP_FORALL_IBV_SYMBOLS

} // namespace tensorpipe
