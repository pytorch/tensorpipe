/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

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
  _(get_device_name, const char*, (IbvLib::device*))                  \
  _(modify_qp, int, (IbvLib::qp*, IbvLib::qp_attr*, int))             \
  _(open_device, IbvLib::context*, (IbvLib::device*))                 \
  _(query_gid, int, (IbvLib::context*, uint8_t, int, IbvLib::gid*))   \
  _(query_port, int, (IbvLib::context*, uint8_t, IbvLib::port_attr*)) \
  _(reg_mr, IbvLib::mr*, (IbvLib::pd*, void*, size_t, int))           \
  _(wc_status_str, const char*, (IbvLib::wc_status))

// Wrapper for libibverbs.

class IbvLib {
 public:
  // Constants

  enum { SYSFS_NAME_MAX = 64, SYSFS_PATH_MAX = 256 };
  enum { WC_IP_CSUM_OK_SHIFT = 2 };

  // Enums

  enum access_flags {
    ACCESS_LOCAL_WRITE = 1,
    ACCESS_REMOTE_WRITE = (1 << 1),
    ACCESS_REMOTE_READ = (1 << 2),
    ACCESS_REMOTE_ATOMIC = (1 << 3),
    ACCESS_MW_BIND = (1 << 4),
    ACCESS_ZERO_BASED = (1 << 5),
    ACCESS_ON_DEMAND = (1 << 6),
    ACCESS_HUGETLB = (1 << 7),
    ACCESS_RELAXED_ORDERING = (1 << 20),
  };

  enum event_type {
    EVENT_CQ_ERR,
    EVENT_QP_FATAL,
    EVENT_QP_REQ_ERR,
    EVENT_QP_ACCESS_ERR,
    EVENT_COMM_EST,
    EVENT_SQ_DRAINED,
    EVENT_PATH_MIG,
    EVENT_PATH_MIG_ERR,
    EVENT_DEVICE_FATAL,
    EVENT_PORT_ACTIVE,
    EVENT_PORT_ERR,
    EVENT_LID_CHANGE,
    EVENT_PKEY_CHANGE,
    EVENT_SM_CHANGE,
    EVENT_SRQ_ERR,
    EVENT_SRQ_LIMIT_REACHED,
    EVENT_QP_LAST_WQE_REACHED,
    EVENT_CLIENT_REREGISTER,
    EVENT_GID_CHANGE,
    EVENT_WQ_FATAL,
  };

  enum mig_state { MIG_MIGRATED, MIG_REARM, MIG_ARMED };

  enum mtu {
    MTU_256 = 1,
    MTU_512 = 2,
    MTU_1024 = 3,
    MTU_2048 = 4,
    MTU_4096 = 5
  };

  enum mw_type { MW_TYPE_1 = 1, MW_TYPE_2 = 2 };

  enum node_type {
    NODE_UNKNOWN = -1,
    NODE_CA = 1,
    NODE_SWITCH,
    NODE_ROUTER,
    NODE_RNIC,
    NODE_USNIC,
    NODE_USNIC_UDP,
    NODE_UNSPECIFIED,
  };

  enum port_state {
    PORT_NOP = 0,
    PORT_DOWN = 1,
    PORT_INIT = 2,
    PORT_ARMED = 3,
    PORT_ACTIVE = 4,
    PORT_ACTIVE_DEFER = 5
  };

  enum qp_attr_mask {
    QP_STATE = 1 << 0,
    QP_CUR_STATE = 1 << 1,
    QP_EN_SQD_ASYNC_NOTIFY = 1 << 2,
    QP_ACCESS_FLAGS = 1 << 3,
    QP_PKEY_INDEX = 1 << 4,
    QP_PORT = 1 << 5,
    QP_QKEY = 1 << 6,
    QP_AV = 1 << 7,
    QP_PATH_MTU = 1 << 8,
    QP_TIMEOUT = 1 << 9,
    QP_RETRY_CNT = 1 << 10,
    QP_RNR_RETRY = 1 << 11,
    QP_RQ_PSN = 1 << 12,
    QP_MAX_QP_RD_ATOMIC = 1 << 13,
    QP_ALT_PATH = 1 << 14,
    QP_MIN_RNR_TIMER = 1 << 15,
    QP_SQ_PSN = 1 << 16,
    QP_MAX_DEST_RD_ATOMIC = 1 << 17,
    QP_PATH_MIG_STATE = 1 << 18,
    QP_CAP = 1 << 19,
    QP_DEST_QPN = 1 << 20,
    QP_RATE_LIMIT = 1 << 25,
  };

  enum qp_state {
    QPS_RESET,
    QPS_INIT,
    QPS_RTR,
    QPS_RTS,
    QPS_SQD,
    QPS_SQE,
    QPS_ERR,
    QPS_UNKNOWN
  };

  enum qp_type {
    QPT_RC = 2,
    QPT_UC,
    QPT_UD,
    QPT_RAW_PACKET = 8,
    QPT_XRC_SEND = 9,
    QPT_XRC_RECV,
    QPT_DRIVER = 0xff,
  };

  enum transport_type {
    TRANSPORT_UNKNOWN = -1,
    TRANSPORT_IB = 0,
    TRANSPORT_IWARP,
    TRANSPORT_USNIC,
    TRANSPORT_USNIC_UDP,
    TRANSPORT_UNSPECIFIED,
  };

  enum wc_flags {
    WC_GRH = 1 << 0,
    WC_WITH_IMM = 1 << 1,
    WC_IP_CSUM_OK = 1 << WC_IP_CSUM_OK_SHIFT,
    WC_WITH_INV = 1 << 3,
    WC_TM_SYNC_REQ = 1 << 4,
    WC_TM_MATCH = 1 << 5,
    WC_TM_DATA_VALID = 1 << 6,
  };

  enum wc_opcode {
    WC_SEND,
    WC_RDMA_WRITE,
    WC_RDMA_READ,
    WC_COMP_SWAP,
    WC_FETCH_ADD,
    WC_BIND_MW,
    WC_LOCAL_INV,
    WC_TSO,
    WC_RECV = 1 << 7,
    WC_RECV_RDMA_WITH_IMM,

    WC_TM_ADD,
    WC_TM_DEL,
    WC_TM_SYNC,
    WC_TM_RECV,
    WC_TM_NO_TAG,
    WC_DRIVER1,
  };

  enum wc_status {
    WC_SUCCESS,
    WC_LOC_LEN_ERR,
    WC_LOC_QP_OP_ERR,
    WC_LOC_EEC_OP_ERR,
    WC_LOC_PROT_ERR,
    WC_WR_FLUSH_ERR,
    WC_MW_BIND_ERR,
    WC_BAD_RESP_ERR,
    WC_LOC_ACCESS_ERR,
    WC_REM_INV_REQ_ERR,
    WC_REM_ACCESS_ERR,
    WC_REM_OP_ERR,
    WC_RETRY_EXC_ERR,
    WC_RNR_RETRY_EXC_ERR,
    WC_LOC_RDD_VIOL_ERR,
    WC_REM_INV_RD_REQ_ERR,
    WC_REM_ABORT_ERR,
    WC_INV_EECN_ERR,
    WC_INV_EEC_STATE_ERR,
    WC_FATAL_ERR,
    WC_RESP_TIMEOUT_ERR,
    WC_GENERAL_ERR,
    WC_TM_ERR,
    WC_TM_RNDV_INCOMPLETE,
  };

  enum wr_opcode {
    WR_RDMA_WRITE,
    WR_RDMA_WRITE_WITH_IMM,
    WR_SEND,
    WR_SEND_WITH_IMM,
    WR_RDMA_READ,
    WR_ATOMIC_CMP_AND_SWP,
    WR_ATOMIC_FETCH_AND_ADD,
    WR_LOCAL_INV,
    WR_BIND_MW,
    WR_SEND_WITH_INV,
    WR_TSO,
    WR_DRIVER1,
  };

  // Structs and unions

  // Forward declarations

  struct _compat_port_attr;
  struct ah;
  struct context;
  struct cq;
  struct device;
  struct mr;
  struct mw_bind;
  struct mw;
  struct pd;
  struct qp;
  struct srq;
  struct wq;

  // Attributes

  struct port_attr {
    IbvLib::port_state state;
    IbvLib::mtu max_mtu;
    IbvLib::mtu active_mtu;
    int gid_tbl_len;
    uint32_t port_cap_flags;
    uint32_t max_msg_sz;
    uint32_t bad_pkey_cntr;
    uint32_t qkey_viol_cntr;
    uint16_t pkey_tbl_len;
    uint16_t lid;
    uint16_t sm_lid;
    uint8_t lmc;
    uint8_t max_vl_num;
    uint8_t sm_sl;
    uint8_t subnet_timeout;
    uint8_t init_type_reply;
    uint8_t active_width;
    uint8_t active_speed;
    uint8_t phys_state;
    uint8_t link_layer;
    uint8_t flags;
    uint16_t port_cap_flags2;
  };

  struct qp_cap {
    uint32_t max_send_wr;
    uint32_t max_recv_wr;
    uint32_t max_send_sge;
    uint32_t max_recv_sge;
    uint32_t max_inline_data;
  };

  union gid {
    uint8_t raw[16];
    struct {
      uint64_t subnet_prefix;
      uint64_t interface_id;
    } global;
  };

  struct global_route {
    IbvLib::gid dgid;
    uint32_t flow_label;
    uint8_t sgid_index;
    uint8_t hop_limit;
    uint8_t traffic_class;
  };

  struct ah_attr {
    IbvLib::global_route grh;
    uint16_t dlid;
    uint8_t sl;
    uint8_t src_path_bits;
    uint8_t static_rate;
    uint8_t is_global;
    uint8_t port_num;
  };

  struct qp_attr {
    IbvLib::qp_state qp_state;
    IbvLib::qp_state cur_qp_state;
    IbvLib::mtu path_mtu;
    IbvLib::mig_state path_mig_state;
    uint32_t qkey;
    uint32_t rq_psn;
    uint32_t sq_psn;
    uint32_t dest_qp_num;
    unsigned int qp_access_flags;
    IbvLib::qp_cap cap;
    IbvLib::ah_attr ah_attr;
    IbvLib::ah_attr alt_ah_attr;
    uint16_t pkey_index;
    uint16_t alt_pkey_index;
    uint8_t en_sqd_async_notify;
    uint8_t sq_draining;
    uint8_t max_rd_atomic;
    uint8_t max_dest_rd_atomic;
    uint8_t min_rnr_timer;
    uint8_t port_num;
    uint8_t timeout;
    uint8_t retry_cnt;
    uint8_t rnr_retry;
    uint8_t alt_port_num;
    uint8_t alt_timeout;
    uint32_t rate_limit;
  };

  struct qp_init_attr {
    void* qp_context;
    IbvLib::cq* send_cq;
    IbvLib::cq* recv_cq;
    IbvLib::srq* srq;
    IbvLib::qp_cap cap;
    IbvLib::qp_type qp_type;
    int sq_sig_all;
  };

  struct srq_attr {
    uint32_t max_wr;
    uint32_t max_sge;
    uint32_t srq_limit;
  };

  struct srq_init_attr {
    void* srq_context;
    IbvLib::srq_attr attr;
  };

  // Work requests and completions

  struct sge {
    uint64_t addr;
    uint32_t length;
    uint32_t lkey;
  };

  struct recv_wr {
    uint64_t wr_id;
    IbvLib::recv_wr* next;
    IbvLib::sge* sg_list;
    int num_sge;
  };

  struct mw_bind_info {
    IbvLib::mr* mr;
    uint64_t addr;
    uint64_t length;
    unsigned int mw_access_flags;
  };

  struct send_wr {
    uint64_t wr_id;
    IbvLib::send_wr* next;
    IbvLib::sge* sg_list;
    int num_sge;
    IbvLib::wr_opcode opcode;
    unsigned int send_flags;
    union {
      uint32_t imm_data;
      uint32_t invalidate_rkey;
    };
    union {
      struct {
        uint64_t remote_addr;
        uint32_t rkey;
      } rdma;
      struct {
        uint64_t remote_addr;
        uint64_t compare_add;
        uint64_t swap;
        uint32_t rkey;
      } atomic;
      struct {
        IbvLib::ah* ah;
        uint32_t remote_qpn;
        uint32_t remote_qkey;
      } ud;
    } wr;
    union {
      struct {
        uint32_t remote_srqn;
      } xrc;
    } qp_type;
    union {
      struct {
        IbvLib::mw* mw;
        uint32_t rkey;
        IbvLib::mw_bind_info bind_info;
      } bind_mw;
      struct {
        void* hdr;
        uint16_t hdr_sz;
        uint16_t mss;
      } tso;
    };
  };

  struct wc {
    uint64_t wr_id;
    IbvLib::wc_status status;
    IbvLib::wc_opcode opcode;
    uint32_t vendor_err;
    uint32_t byte_len;
    union {
      uint32_t imm_data;
      uint32_t invalidated_rkey;
    };
    uint32_t qp_num;
    uint32_t src_qp;
    unsigned int wc_flags;
    uint16_t pkey_index;
    uint16_t slid;
    uint8_t sl;
    uint8_t dlid_path_bits;
  };

  // Main structs

  struct async_event {
    union {
      IbvLib::cq* cq;
      IbvLib::qp* qp;
      IbvLib::srq* srq;
      IbvLib::wq* wq;
      int port_num;
    } element;
    IbvLib::event_type event_type;
  };

  struct comp_channel {
    IbvLib::context* context;
    int fd;
    int refcnt;
  };

  struct context_ops {
    void* (*_compat_query_device)(void);
    int (*_compat_query_port)(
        IbvLib::context* context,
        uint8_t port_num,
        struct IbvLib::_compat_port_attr* port_attr);
    void* (*_compat_alloc_pd)(void);
    void* (*_compat_dealloc_pd)(void);
    void* (*_compat_reg_mr)(void);
    void* (*_compat_rereg_mr)(void);
    void* (*_compat_dereg_mr)(void);
    IbvLib::mw* (*alloc_mw)(IbvLib::pd* pd, IbvLib::mw_type type);
    int (*bind_mw)(IbvLib::qp* qp, IbvLib::mw* mw, IbvLib::mw_bind* mw_bind);
    int (*dealloc_mw)(IbvLib::mw* mw);
    void* (*_compat_create_cq)(void);
    int (*poll_cq)(IbvLib::cq* cq, int num_entries, IbvLib::wc* wc);
    int (*req_notify_cq)(IbvLib::cq* cq, int solicited_only);
    void* (*_compat_cq_event)(void);
    void* (*_compat_resize_cq)(void);
    void* (*_compat_destroy_cq)(void);
    void* (*_compat_create_srq)(void);
    void* (*_compat_modify_srq)(void);
    void* (*_compat_query_srq)(void);
    void* (*_compat_destroy_srq)(void);
    int (*post_srq_recv)(
        IbvLib::srq* srq,
        IbvLib::recv_wr* recv_wr,
        IbvLib::recv_wr** bad_recv_wr);
    void* (*_compat_create_qp)(void);
    void* (*_compat_query_qp)(void);
    void* (*_compat_modify_qp)(void);
    void* (*_compat_destroy_qp)(void);
    int (*post_send)(
        IbvLib::qp* qp,
        IbvLib::send_wr* wr,
        IbvLib::send_wr** bad_wr);
    int (*post_recv)(
        IbvLib::qp* qp,
        IbvLib::recv_wr* wr,
        IbvLib::recv_wr** bad_wr);
    void* (*_compat_create_ah)(void);
    void* (*_compat_destroy_ah)(void);
    void* (*_compat_attach_mcast)(void);
    void* (*_compat_detach_mcast)(void);
    void* (*_compat_async_event)(void);
  };

  struct context {
    IbvLib::device* device;
    IbvLib::context_ops ops;
    int cmd_fd;
    int async_fd;
    int num_comp_vectors;
    pthread_mutex_t mutex;
    void* abi_compat;
  };

  struct cq {
    IbvLib::context* context;
    IbvLib::comp_channel* channel;
    void* cq_context;
    uint32_t handle;
    int cqe;

    pthread_mutex_t mutex;
    pthread_cond_t cond;
    uint32_t comp_events_completed;
    uint32_t async_events_completed;
  };

  struct _device_ops {
    IbvLib::context* (*_dummy1)(IbvLib::device* device, int cmd_fd);
    void (*_dummy2)(IbvLib::context* context);
  };

  struct device {
    IbvLib::_device_ops _ops;
    IbvLib::node_type node_type;
    IbvLib::transport_type transport_type;
    char name[IbvLib::SYSFS_NAME_MAX];
    char dev_name[IbvLib::SYSFS_NAME_MAX];
    char dev_path[IbvLib::SYSFS_PATH_MAX];
    char ibdev_path[IbvLib::SYSFS_PATH_MAX];
  };

  struct mr {
    IbvLib::context* context;
    IbvLib::pd* pd;
    void* addr;
    size_t length;
    uint32_t handle;
    uint32_t lkey;
    uint32_t rkey;
  };

  struct pd {
    IbvLib::context* context;
    uint32_t handle;
  };

  struct qp {
    IbvLib::context* context;
    void* qp_context;
    IbvLib::pd* pd;
    IbvLib::cq* send_cq;
    IbvLib::cq* recv_cq;
    IbvLib::srq* srq;
    uint32_t handle;
    uint32_t qp_num;
    IbvLib::qp_state state;
    IbvLib::qp_type qp_type;

    pthread_mutex_t mutex;
    pthread_cond_t cond;
    uint32_t events_completed;
  };

  struct srq {
    IbvLib::context* context;
    void* srq_context;
    IbvLib::pd* pd;
    uint32_t handle;

    pthread_mutex_t mutex;
    pthread_cond_t cond;
    uint32_t events_completed;
  };

 private:
  explicit IbvLib(DynamicLibraryHandle dlhandle)
      : dlhandle_(std::move(dlhandle)) {}

  DynamicLibraryHandle dlhandle_;

#define TP_DECLARE_FIELD(function_name, return_type, args_types) \
  return_type(*function_name##_ptr_) args_types = nullptr;
  TP_FORALL_IBV_SYMBOLS(TP_DECLARE_FIELD)
#undef TP_DECLARE_FIELD

 public:
  IbvLib() = default;

#define TP_FORWARD_CALL(function_name, return_type, args_types)  \
  template <typename... Args>                                    \
  auto function_name(Args&&... args) const {                     \
    return (*function_name##_ptr_)(std::forward<Args>(args)...); \
  }
  TP_FORALL_IBV_SYMBOLS(TP_FORWARD_CALL)
#undef TP_FORWARD_CALL

  static std::tuple<Error, IbvLib> create() {
    Error error;
    DynamicLibraryHandle dlhandle;
    // To keep things "neat" and contained, we open in "local" mode (as opposed
    // to global) so that the ibverbs symbols can only be resolved through this
    // handle and are not exposed (a.k.a., "leaded") to other shared objects.
    std::tie(error, dlhandle) =
        DynamicLibraryHandle::create("libibverbs.so.1", RTLD_LOCAL | RTLD_LAZY);
    if (error) {
      return std::make_tuple(std::move(error), IbvLib());
    }
    // Log at level 9 as we can't know whether this will be used in a transport
    // or channel, thus err on the side of this being as low-level as possible
    // because we don't expect this to be of interest that often.
    TP_VLOG(9) << [&]() -> std::string {
      std::string filename;
      std::tie(error, filename) = dlhandle.getFilename();
      if (error) {
        return "Couldn't determine location of shared library libibverbs.so.1: " +
            error.what();
      }
      return "Found shared library libibverbs.so.1 at " + filename;
    }();
    IbvLib lib(std::move(dlhandle));
#define TP_LOAD_SYMBOL(function_name, return_type, args_types)              \
  {                                                                         \
    void* ptr;                                                              \
    std::tie(error, ptr) = lib.dlhandle_.loadSymbol("ibv_" #function_name); \
    if (error) {                                                            \
      return std::make_tuple(std::move(error), IbvLib());                   \
    }                                                                       \
    TP_THROW_ASSERT_IF(ptr == nullptr);                                     \
    lib.function_name##_ptr_ =                                              \
        reinterpret_cast<decltype(function_name##_ptr_)>(ptr);              \
  }
    TP_FORALL_IBV_SYMBOLS(TP_LOAD_SYMBOL)
#undef TP_LOAD_SYMBOL
    return std::make_tuple(Error::kSuccess, std::move(lib));
  }

  // These functions (which, it would seem, are the ones that are used in the
  // critical control path, and which thus must have the lowest latency and
  // avoid any syscall/kernel overhead) are not exposed as symbols of
  // libibverbs.so: they are defined inline in the header and, in fact, they
  // access a function pointer stored on the ibv_context and execute it.

  int poll_cq(IbvLib::cq* cq, int num_entries, IbvLib::wc* wc) const {
    return cq->context->ops.poll_cq(cq, num_entries, wc);
  }

  int post_send(IbvLib::qp* qp, IbvLib::send_wr* wr, IbvLib::send_wr** bad_wr)
      const {
    return qp->context->ops.post_send(qp, wr, bad_wr);
  }

  int post_recv(IbvLib::qp* qp, IbvLib::recv_wr* wr, IbvLib::recv_wr** bad_wr)
      const {
    return qp->context->ops.post_recv(qp, wr, bad_wr);
  }

  int post_srq_recv(
      IbvLib::srq* srq,
      IbvLib::recv_wr* recv_wr,
      IbvLib::recv_wr** bad_recv_wr) const {
    return srq->context->ops.post_srq_recv(srq, recv_wr, bad_recv_wr);
  }
};

#undef TP_FORALL_IBV_SYMBOLS

} // namespace tensorpipe
