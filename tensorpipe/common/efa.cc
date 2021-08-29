/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/efa.h>
#include <tensorpipe/common/defs.h>
#include <rdma/fabric.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_tagged.h>

namespace tensorpipe{

FabricContext::FabricContext(){
    UniqueFabricPtr<fi_info> fabinfo = getFabricInfo();
    struct fi_info *info = fabinfo.get();
    struct fi_av_attr av_attr = {};

    // fi_fabric: create fabric
    struct fid_fabric *fabric_;
    int ret = fi_fabric(info->fabric_attr, &fabric_, nullptr);
    TP_CHECK_EFA_RET(ret, "Couldn't open a fabric provider");
    fabric.reset(fabric_);

    // fi_domain: create domain
    struct fid_domain *domain_;
    ret = fi_domain(fabric.get(), info, &domain_, nullptr);
    // LOG(INFO) << domain_->
    TP_CHECK_EFA_RET(ret, "Couldn't open a fabric access domain");
    domain.reset(domain_);

    // fi_av_open: create address vector
    av_attr.type = FI_AV_TABLE;
    struct fid_av *av_;
    ret = fi_av_open(domain.get(), &av_attr, &av_, nullptr);
    av.reset(av_);
    TP_CHECK_EFA_RET(ret, "Couldn't open AV");

    // fi_cq_open: open completion queue
    struct fid_cq *cq_;
    struct fi_cq_attr cq_attr = {};
    cq_attr.format = FI_CQ_FORMAT_TAGGED;
    cq_attr.size = info->rx_attr->size;
    ret = fi_cq_open(domain.get(), &cq_attr, &cq_, nullptr);
    cq.reset(cq_);
    TP_CHECK_EFA_RET(ret, "Couldn't open CQ");

    // fi_endpoint: create transport level communication endpoint(s)
    struct fid_ep *ep_;
    ret = fi_endpoint(domain.get(), info, &ep_, nullptr);
    ep.reset(ep_);
    TP_CHECK_EFA_RET(ret, "Couldn't allocate endpoint");

    // fi_ep_bind: bind CQ and AV to the endpoint
    ret = fi_ep_bind(ep.get(), (fid_t)cq.get(), FI_RECV | FI_TRANSMIT);
    TP_CHECK_EFA_RET(ret, "Couldn't bind EP-CQ");
    ret = fi_ep_bind(ep.get(), (fid_t)av.get(), 0);
    TP_CHECK_EFA_RET(ret, "Couldn't bind EP-AV");

    // fi_enable: enable endpoint for communication
    ret = fi_enable(ep.get());
    TP_CHECK_EFA_RET(ret, "Couldn't enable endpoint");

    // fi_getname: get endpoint name
    ret = fi_getname((fid_t)ep.get(), addr.name, &addr.len);
    TP_CHECK_EFA_RET(ret, "Call to fi_getname() failed");
    // set readable address name
    fi_av_straddr(av.get(), addr.name, readable_addr.name, &readable_addr.len);
}

UniqueFabricPtr<fi_info> FabricContext::getFabricInfo(){    
    UniqueFabricPtr<struct fi_info> hints(fi_allocinfo());
    hints->mode = FI_CONTEXT;
    hints->ep_attr->type = FI_EP_RDM; // Reliable Datagram
    hints->caps = FI_TAGGED | FI_MSG | FI_REMOTE_COMM | FI_DIRECTED_RECV | FI_LOCAL_COMM | FI_SOURCE;
    hints->tx_attr->msg_order = FI_ORDER_SAS;
    hints->rx_attr->msg_order = FI_ORDER_SAS;
    hints->domain_attr->control_progress = FI_PROGRESS_AUTO;
    hints->domain_attr->data_progress = FI_PROGRESS_AUTO;
    hints->domain_attr->caps =
        FI_LOCAL_COMM | FI_REMOTE_COMM; // Enable local loopback
    hints->domain_attr->av_type = FI_AV_TABLE;
    hints->fabric_attr->prov_name = strdup("efa");

    UniqueFabricPtr<fi_info> info;
    struct fi_info* info_;
    int ret =
        fi_getinfo(FABRIC_VERSION, nullptr, nullptr, 0, hints.get(), &info_);
    info.reset(info_);    
    TP_CHECK_EFA_RET(ret, "fi_getinfo failed");
    // TP_THROW_ASSERT() << "Could not find any optimal provider. Return Code: "
    //                   << ret << ". ERROR: " << fi_strerror(-ret);
    return info;
}

bool FabricEndpoint::isEfaAvailable(){
    UniqueFabricPtr<struct fi_info> hints(fi_allocinfo());
    hints->mode = FI_CONTEXT;
    hints->ep_attr->type = FI_EP_RDM; // Reliable Datagram
    hints->caps = FI_TAGGED | FI_MSG | FI_REMOTE_COMM | FI_DIRECTED_RECV | FI_LOCAL_COMM | FI_SOURCE;
    hints->tx_attr->msg_order = FI_ORDER_SAS;
    hints->rx_attr->msg_order = FI_ORDER_SAS;
    hints->domain_attr->control_progress = FI_PROGRESS_AUTO;
    hints->domain_attr->data_progress = FI_PROGRESS_AUTO;
    hints->domain_attr->caps =
        FI_LOCAL_COMM | FI_REMOTE_COMM; // Enable local loopback
    hints->domain_attr->av_type = FI_AV_TABLE;
    hints->fabric_attr->prov_name = strdup("efa");
    UniqueFabricPtr<fi_info> info;
    struct fi_info* info_;
    int ret =
        fi_getinfo(FABRIC_VERSION, nullptr, nullptr, 0, hints.get(), &info_);
    return info_ == nullptr;
}

FabricEndpoint::FabricEndpoint(){
    fabric_ctx = std::make_unique<FabricContext>();    
}

int FabricEndpoint::PollCQ(struct fi_cq_tagged_entry* cq_entries, fi_addr_t* src_addrs, size_t count){
    int ret = fi_cq_readfrom(fabric_ctx->cq.get(), &cq_entries, count, src_addrs);
    return ret;
}

int FabricEndpoint::PushSendEvent(void* buffer, size_t size, uint64_t tag, fi_addr_t dest_addr, void* context){
    int ret = fi_tsend(fabric_ctx->ep.get(), buffer, size, nullptr, dest_addr, tag, context);
    if (ret < 0 && ret != -FI_EAGAIN) {
        TP_CHECK_EFA_RET(ret, "Unable to do fi_tsend message");
    }
    return ret;
}

int FabricEndpoint::PushRecvEvent(void* buffer, size_t size, uint64_t tag, fi_addr_t dest_addr, uint64_t ignore, void* context){
    int ret = fi_trecv(fabric_ctx->ep.get(), buffer, size, nullptr, dest_addr, tag, ignore, context);
    if (ret < 0 && ret != -FI_EAGAIN) {
        TP_CHECK_EFA_RET(ret, "Unable to do fi_trecv message");
    }
    return ret;
}



} // namespace tensorpipe