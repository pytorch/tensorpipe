/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/ibv.h>

#include <cstdlib>
#include <cstring>

namespace tensorpipe {

std::string ibvWorkCompletionOpcodeToStr(IbvLib::wc_opcode opcode) {
  switch (opcode) {
    case IbvLib::WC_SEND:
      return "SEND";
    case IbvLib::WC_RDMA_WRITE:
      return "RDMA_WRITE";
    case IbvLib::WC_RDMA_READ:
      return "RDMA_READ";
    case IbvLib::WC_COMP_SWAP:
      return "COMP_SWAP";
    case IbvLib::WC_FETCH_ADD:
      return "FETCH_ADD";
    case IbvLib::WC_BIND_MW:
      return "BIND_MW";
    case IbvLib::WC_RECV:
      return "RECV";
    case IbvLib::WC_RECV_RDMA_WITH_IMM:
      return "RECV_RDMA_WITH_IMM";
    default:
      return "UNKNOWN (" + std::to_string(opcode) + ")";
  }
}

struct IbvAddress makeIbvAddress(
    const IbvLib& ibvLib,
    const IbvContext& context,
    uint8_t portNum,
    uint8_t globalIdentifierIndex) {
  struct IbvAddress addr;
  std::memset(&addr, 0, sizeof(addr));

  addr.portNum = portNum;
  addr.globalIdentifierIndex = globalIdentifierIndex;

  IbvLib::port_attr portAttr;
  std::memset(&portAttr, 0, sizeof(portAttr));
  TP_CHECK_IBV_INT(ibvLib.query_port(context.get(), portNum, &portAttr));
  addr.localIdentifier = portAttr.lid;
  addr.maximumTransmissionUnit = portAttr.active_mtu;
  addr.maximumMessageSize = portAttr.max_msg_sz;

  TP_CHECK_IBV_INT(ibvLib.query_gid(
      context.get(), portNum, globalIdentifierIndex, &addr.globalIdentifier));

  return addr;
}

struct IbvSetupInformation makeIbvSetupInformation(
    const IbvAddress& addr,
    const IbvQueuePair& qp) {
  struct IbvSetupInformation info;
  std::memset(&info, 0, sizeof(info));

  info.localIdentifier = addr.localIdentifier;
  info.globalIdentifier = addr.globalIdentifier;
  info.queuePairNumber = qp->qp_num;
  info.maximumTransmissionUnit = addr.maximumTransmissionUnit;
  info.maximumMessageSize = addr.maximumMessageSize;

  return info;
}

void transitionIbvQueuePairToInit(
    const IbvLib& ibvLib,
    IbvQueuePair& qp,
    const IbvAddress& selfAddr) {
  IbvLib::qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  int attrMask = 0;

  attrMask |= IbvLib::QP_STATE;
  attr.qp_state = IbvLib::QPS_INIT;

  // Hardcode the use of the first entry of the partition key table, as it will
  // always be valid.
  // FIXME: Make this configurable similarly to the port number.
  attrMask |= IbvLib::QP_PKEY_INDEX;
  attr.pkey_index = 0;

  attrMask |= IbvLib::QP_PORT;
  attr.port_num = selfAddr.portNum;

  attrMask |= IbvLib::QP_ACCESS_FLAGS;
  attr.qp_access_flags =
      IbvLib::ACCESS_LOCAL_WRITE | IbvLib::ACCESS_REMOTE_WRITE;

  TP_CHECK_IBV_INT(ibvLib.modify_qp(qp.get(), &attr, attrMask));
}

void transitionIbvQueuePairToReadyToReceive(
    const IbvLib& ibvLib,
    IbvQueuePair& qp,
    const IbvAddress& selfAddr,
    const IbvSetupInformation& destinationInfo) {
  IbvLib::qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  int attrMask = 0;

  attrMask |= IbvLib::QP_STATE;
  attr.qp_state = IbvLib::QPS_RTR;

  // Global routing is only set up as far as needed to support RoCE.
  attrMask |= IbvLib::QP_AV;
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

  attrMask |= IbvLib::QP_PATH_MTU;
  attr.path_mtu = std::min(
      selfAddr.maximumTransmissionUnit,
      destinationInfo.maximumTransmissionUnit);

  attrMask |= IbvLib::QP_DEST_QPN;
  attr.dest_qp_num = destinationInfo.queuePairNumber;

  // The packet sequence numbers of the local send and of the remote receive
  // queues (and vice versa) only need to match. Thus we set them all to zero.
  attrMask |= IbvLib::QP_RQ_PSN;
  attr.rq_psn = 0;

  attrMask |= IbvLib::QP_MAX_DEST_RD_ATOMIC;
  attr.max_dest_rd_atomic = 1;

  attrMask |= IbvLib::QP_MIN_RNR_TIMER;
  attr.min_rnr_timer = 20; // 10.24 milliseconds

  TP_CHECK_IBV_INT(ibvLib.modify_qp(qp.get(), &attr, attrMask));
}

void transitionIbvQueuePairToReadyToSend(
    const IbvLib& ibvLib,
    IbvQueuePair& qp) {
  IbvLib::qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  int attrMask = 0;

  attrMask |= IbvLib::QP_STATE;
  attr.qp_state = IbvLib::QPS_RTS;

  // The packet sequence numbers of the local send and of the remote receive
  // queues (and vice versa) only need to match. Thus we set them all to zero.
  attrMask |= IbvLib::QP_SQ_PSN;
  attr.sq_psn = 0;

  attrMask |= IbvLib::QP_TIMEOUT;
  attr.timeout = 14; // 67.1 milliseconds

  attrMask |= IbvLib::QP_RETRY_CNT;
  attr.retry_cnt = 7;

  attrMask |= IbvLib::QP_RNR_RETRY;
  attr.rnr_retry = 7; // infinite

  attrMask |= IbvLib::QP_MAX_QP_RD_ATOMIC;
  attr.max_rd_atomic = 1;

  TP_CHECK_IBV_INT(ibvLib.modify_qp(qp.get(), &attr, attrMask));
}

void transitionIbvQueuePairToError(const IbvLib& ibvLib, IbvQueuePair& qp) {
  IbvLib::qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  int attrMask = 0;

  attrMask |= IbvLib::QP_STATE;
  attr.qp_state = IbvLib::QPS_ERR;

  TP_CHECK_IBV_INT(ibvLib.modify_qp(qp.get(), &attr, attrMask));
}

} // namespace tensorpipe
