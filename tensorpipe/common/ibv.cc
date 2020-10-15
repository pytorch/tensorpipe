/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/ibv.h>

#include <cstdlib>
#include <cstring>

namespace tensorpipe {

std::string ibvWorkCompletionOpcodeToStr(enum ibv_wc_opcode opcode) {
  switch (opcode) {
    case IBV_WC_SEND:
      return "SEND";
    case IBV_WC_RDMA_WRITE:
      return "RDMA_WRITE";
    case IBV_WC_RDMA_READ:
      return "RDMA_READ";
    case IBV_WC_COMP_SWAP:
      return "COMP_SWAP";
    case IBV_WC_FETCH_ADD:
      return "FETCH_ADD";
    case IBV_WC_BIND_MW:
      return "BIND_MW";
    case IBV_WC_RECV:
      return "RECV";
    case IBV_WC_RECV_RDMA_WITH_IMM:
      return "RECV_RDMA_WITH_IMM";
    default:
      return "UNKNOWN (" + std::to_string(opcode) + ")";
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
  TP_CHECK_IBV_INT(ibv_query_port(context.get(), portNum, &portAttr));
  addr.localIdentifier = portAttr.lid;
  addr.maximumTransmissionUnit = portAttr.active_mtu;

  TP_CHECK_IBV_INT(ibv_query_gid(
      context.get(), portNum, globalIdentifierIndex, &addr.globalIdentifier));

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
  info.maximumTransmissionUnit = addr.maximumTransmissionUnit;

  return info;
}

void transitionIbvQueuePairToInit(IbvQueuePair& qp, IbvAddress& selfAddr) {
  struct ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  int attrMask = 0;

  attrMask |= IBV_QP_STATE;
  attr.qp_state = IBV_QPS_INIT;

  // Hardcode the use of the first entry of the partition key table, as it will
  // always be valid. FIXME Make thus configurable similarly to the port number.
  attrMask |= IBV_QP_PKEY_INDEX;
  attr.pkey_index = 0;

  attrMask |= IBV_QP_PORT;
  attr.port_num = selfAddr.portNum;

  attrMask |= IBV_QP_ACCESS_FLAGS;
  attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;

  TP_CHECK_IBV_INT(ibv_modify_qp(qp.get(), &attr, attrMask));
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

  // Global routing is only set up as far as needed to support RoCE.
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
  attr.path_mtu = std::min(
      selfAddr.maximumTransmissionUnit,
      destinationInfo.maximumTransmissionUnit);

  attrMask |= IBV_QP_DEST_QPN;
  attr.dest_qp_num = destinationInfo.queuePairNumber;

  // The packet sequence numbers of the local send and of the remote receive
  // queues (and vice versa) only need to match. Thus we set them all to zero.
  attrMask |= IBV_QP_RQ_PSN;
  attr.rq_psn = 0;

  attrMask |= IBV_QP_MAX_DEST_RD_ATOMIC;
  attr.max_dest_rd_atomic = 1;

  attrMask |= IBV_QP_MIN_RNR_TIMER;
  attr.min_rnr_timer = 20; // 10.24 milliseconds

  TP_CHECK_IBV_INT(ibv_modify_qp(qp.get(), &attr, attrMask));
}

void transitionIbvQueuePairToReadyToSend(
    IbvQueuePair& qp,
    IbvSetupInformation& selfInfo) {
  struct ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  int attrMask = 0;

  attrMask |= IBV_QP_STATE;
  attr.qp_state = IBV_QPS_RTS;

  // The packet sequence numbers of the local send and of the remote receive
  // queues (and vice versa) only need to match. Thus we set them all to zero.
  attrMask |= IBV_QP_SQ_PSN;
  attr.sq_psn = 0;

  attrMask |= IBV_QP_TIMEOUT;
  attr.timeout = 14; // 67.1 milliseconds

  attrMask |= IBV_QP_RETRY_CNT;
  attr.retry_cnt = 7;

  attrMask |= IBV_QP_RNR_RETRY;
  attr.rnr_retry = 7; // infinite

  attrMask |= IBV_QP_MAX_QP_RD_ATOMIC;
  attr.max_rd_atomic = 1;

  TP_CHECK_IBV_INT(ibv_modify_qp(qp.get(), &attr, attrMask));
}

void transitionIbvQueuePairToError(IbvQueuePair& qp) {
  struct ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  int attrMask = 0;

  attrMask |= IBV_QP_STATE;
  attr.qp_state = IBV_QPS_ERR;

  TP_CHECK_IBV_INT(ibv_modify_qp(qp.get(), &attr, attrMask));
}

} // namespace tensorpipe
