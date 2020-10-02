/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/ibv/ibv.h>

#include <cstdlib>
#include <cstring>

namespace tensorpipe {
namespace transport {
namespace ibv {

std::string opcodeToStr(enum ibv_wc_opcode opcode) {
  switch (opcode) {
    case IBV_WC_SEND:
      return "IBV_WC_SEND";
    case IBV_WC_RDMA_WRITE:
      return "IBV_WC_RDMA_WRITE";
    case IBV_WC_RDMA_READ:
      return "IBV_WC_RDMA_READ";
    case IBV_WC_COMP_SWAP:
      return "IBV_WC_COMP_SWAP";
    case IBV_WC_FETCH_ADD:
      return "IBV_WC_FETCH_ADD";
    case IBV_WC_BIND_MW:
      return "IBV_WC_BIND_MW";
    case IBV_WC_RECV:
      return "IBV_WC_RECV";
    case IBV_WC_RECV_RDMA_WITH_IMM:
      return "IBV_WC_RECV_RDMA_WITH_IMM";
  }
}

struct IbvAddress makeIbvAddress(
    IbvContext& context,
    uint8_t portNum,
    uint8_t globalIdentifierIndex) {
  struct IbvAddress addr;
  std::memset(&addr, 0, sizeof(addr));

  addr.portNum = portNum;
  addr.globalIdentifierIndex = globalIdentifierIndex;

  struct ibv_port_attr portAttr;
  std::memset(&portAttr, 0, sizeof(portAttr));
  TP_CHECK_IBV_INT(ibv_query_port(context.ptr(), portNum, &portAttr));
  addr.localIdentifier = portAttr.lid;

  TP_CHECK_IBV_INT(ibv_query_gid(
      context.ptr(), portNum, globalIdentifierIndex, &addr.globalIdentifier));

  return addr;
}

struct IbvSetupInformation makeIbvSetupInformation(
    IbvAddress& addr,
    IbvQueuePair& qp) {
  struct IbvSetupInformation info;
  std::memset(&info, 0, sizeof(info));

  info.localIdentifier = addr.localIdentifier;
  info.globalIdentifier = addr.globalIdentifier;
  info.queuePairNumber = qp->qp_num;
  info.packetSequenceNumber = std::rand() & 0xffffff;

  return info;
}

namespace {

// The number of simultaneous outgoing operations we require the queue pair to
// be able to handle. (The QP might actually decide to support more than this).
// Right now this is set to an arbitrary "large enough" number, but nothing is
// done to deal with the case of this many operations being reached (except
// failing).
constexpr auto kMaxSendWorkRequests = 1000;

} // namespace

IbvQueuePair createIbvQueuePair(
    IbvProtectionDomain& pd,
    IbvCompletionQueue& cq,
    IbvSharedReceiveQueue& srq) {
  struct ibv_qp_init_attr initAttr;
  std::memset(&initAttr, 0, sizeof(initAttr));
  initAttr.qp_type = IBV_QPT_RC;
  initAttr.send_cq = cq.ptr();
  initAttr.recv_cq = cq.ptr();
  initAttr.cap.max_send_wr = kMaxSendWorkRequests;
  initAttr.cap.max_send_sge = 1;
  initAttr.srq = srq.ptr();
  initAttr.sq_sig_all = 1;
  return IbvQueuePair(pd, initAttr);
}

void transitionIbvQueuePairToInit(IbvQueuePair& qp, IbvAddress& selfAddr) {
  struct ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  int attrMask = 0;

  attrMask |= IBV_QP_STATE;
  attr.qp_state = IBV_QPS_INIT;

  attrMask |= IBV_QP_PKEY_INDEX;
  attr.pkey_index = 0;

  attrMask |= IBV_QP_PORT;
  attr.port_num = selfAddr.portNum;

  attrMask |= IBV_QP_ACCESS_FLAGS;
  attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;

  TP_CHECK_IBV_INT(ibv_modify_qp(qp.ptr(), &attr, attrMask));
}

void transitionIbvQueuePairToReadyToReceive(
    IbvQueuePair& qp,
    IbvAddress& selfAddr,
    IbvSetupInformation& destinationInfo) {
  struct ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  int attrMask = 0;

  attrMask |= IBV_QP_STATE;
  attr.qp_state = IBV_QPS_RTR;

  attrMask |= IBV_QP_AV;
  if (destinationInfo.localIdentifier != 0) {
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = destinationInfo.localIdentifier;
  } else {
    attr.ah_attr.is_global = 1;
    attr.ah_attr.grh.dgid = destinationInfo.globalIdentifier;
    attr.ah_attr.grh.sgid_index = selfAddr.globalIdentifierIndex;
    attr.ah_attr.grh.hop_limit = 1;
  }
  attr.ah_attr.port_num = selfAddr.portNum;

  attrMask |= IBV_QP_PATH_MTU;
  attr.path_mtu = IBV_MTU_1024;

  attrMask |= IBV_QP_DEST_QPN;
  attr.dest_qp_num = destinationInfo.queuePairNumber;

  attrMask |= IBV_QP_RQ_PSN;
  attr.rq_psn = destinationInfo.packetSequenceNumber;

  attrMask |= IBV_QP_MAX_DEST_RD_ATOMIC;
  attr.max_dest_rd_atomic = 1;

  attrMask |= IBV_QP_MIN_RNR_TIMER;
  attr.min_rnr_timer = 20;

  TP_CHECK_IBV_INT(ibv_modify_qp(qp.ptr(), &attr, attrMask));
}

void transitionIbvQueuePairToReadyToSend(
    IbvQueuePair& qp,
    IbvSetupInformation& selfInfo) {
  struct ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  int attrMask = 0;

  attrMask |= IBV_QP_STATE;
  attr.qp_state = IBV_QPS_RTS;

  attrMask |= IBV_QP_SQ_PSN;
  attr.sq_psn = selfInfo.packetSequenceNumber;

  attrMask |= IBV_QP_TIMEOUT;
  attr.timeout = 14;

  attrMask |= IBV_QP_RETRY_CNT;
  attr.retry_cnt = 7;

  attrMask |= IBV_QP_RNR_RETRY;
  attr.rnr_retry = 7;

  attrMask |= IBV_QP_MAX_QP_RD_ATOMIC;
  attr.max_rd_atomic = 1;

  TP_CHECK_IBV_INT(ibv_modify_qp(qp.ptr(), &attr, attrMask));
}

void transitionIbvQueuePairToError(IbvQueuePair& qp) {
  struct ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  int attrMask = 0;

  attrMask |= IBV_QP_STATE;
  attr.qp_state = IBV_QPS_ERR;

  TP_CHECK_IBV_INT(ibv_modify_qp(qp.ptr(), &attr, attrMask));
}

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
