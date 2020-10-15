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

#define TP_FORALL_IBV_SYMBOLS(_)                                              \
  _(ack_async_event, void, (struct ibv_async_event*))                         \
  _(alloc_pd, struct ibv_pd*, (struct ibv_context*))                          \
  _(close_device, int, (struct ibv_context*))                                 \
  _(create_cq,                                                                \
    struct ibv_cq*,                                                           \
    (struct ibv_context*, int, void*, struct ibv_comp_channel*, int))         \
  _(create_qp, struct ibv_qp*, (struct ibv_pd*, struct ibv_qp_init_attr*))    \
  _(create_srq, struct ibv_srq*, (struct ibv_pd*, struct ibv_srq_init_attr*)) \
  _(dealloc_pd, int, (struct ibv_pd*))                                        \
  _(dereg_mr, int, (struct ibv_mr*))                                          \
  _(destroy_cq, int, (struct ibv_cq*))                                        \
  _(destroy_qp, int, (struct ibv_qp*))                                        \
  _(destroy_srq, int, (struct ibv_srq*))                                      \
  _(event_type_str, const char*, (enum ibv_event_type))                       \
  _(free_device_list, void, (struct ibv_device**))                            \
  _(get_async_event, int, (struct ibv_context*, struct ibv_async_event*))     \
  _(get_device_list, struct ibv_device**, (int*))                             \
  _(modify_qp, int, (struct ibv_qp*, struct ibv_qp_attr*, int))               \
  _(open_device, struct ibv_context*, (struct ibv_device*))                   \
  _(query_gid, int, (struct ibv_context*, uint8_t, int, union ibv_gid*))      \
  _(query_port, int, (struct ibv_context*, uint8_t, struct ibv_port_attr*))   \
  _(reg_mr, struct ibv_mr*, (struct ibv_pd*, void*, size_t, int))             \
  _(wc_status_str, const char*, (enum ibv_wc_status))
// These functions (which, it would seem, are the ones that are used in the
// critical control path, and which thus must have the lowest latency and avoid
// any syscall/kernel overhead) are not exposed as symbols of libibverbs.so:
// they are defined inline in the header and, in fact, they access a function
// pointer stored on the ibv_context and execute it.
//  _(poll_cq)
//  _(post_send)
//  _(post_srq_recv)

// Wrapper for libibverbs.

class IbvLib {
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
        createDynamicLibraryHandle("libibverbs.so", RTLD_LOCAL | RTLD_LAZY);
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
};

#undef TP_FORALL_IBV_SYMBOLS

} // namespace tensorpipe
