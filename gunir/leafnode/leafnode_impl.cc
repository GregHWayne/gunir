// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#include "gunir/leafnode/leafnode_impl.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "toft/system/threading/this_thread.h"

#include "gunir/leafnode/sender_manager.h"
#include "gunir/leafnode/task_container_pool.h"
#include "gunir/leafnode/worker_manager.h"
#include "gunir/master/master_client.h"
#include "gunir/proto/master_rpc.pb.h"
#include "gunir/proto/proto_helper.h"
#include "gunir/proto/task.pb.h"
#include "gunir/stemnode/stemnode_client.h"
#include "gunir/types.h"

DECLARE_int32(gunir_leafnode_thread_min_num);
DECLARE_int32(gunir_leafnode_thread_max_num);

DECLARE_int64(gunir_heartbeat_period);
DECLARE_int64(gunir_heartbeat_retry_period_factor);
DECLARE_int32(gunir_heartbeat_retry_times);

DECLARE_int32(gunir_leafnode_worker_thread_num);
DECLARE_int32(gunir_leafnode_sender_thread_num);

namespace gunir {
namespace leafnode {


LeafNodeImpl::LeafNodeImpl(const ServerInfo& node_info)
    : m_server_info(node_info),
      m_status(kNotInited),
      m_this_sequence_id(kSequenceIDStart),
      m_master_client(new master::MasterClient()),
      m_stemnode_client(new stemnode::StemNodeClient()),
      m_thread_pool(new toft::ThreadPool(FLAGS_gunir_leafnode_thread_min_num,
                                         FLAGS_gunir_leafnode_thread_max_num)) {
    master::MasterClient::SetThreadPool(m_thread_pool.get());
    master::MasterClient::SetOption();

    stemnode::StemNodeClient::SetThreadPool(m_thread_pool.get());
    stemnode::StemNodeClient::SetOption();

    m_task_container_pool.reset(new TaskContainerPool());
    m_worker_manager.reset(new WorkerManager(m_task_container_pool.get(),
                                             FLAGS_gunir_leafnode_worker_thread_num));
    m_sender_manager.reset(new SenderManager(m_task_container_pool.get(),
                                             FLAGS_gunir_leafnode_sender_thread_num));
}

LeafNodeImpl::~LeafNodeImpl() {}

bool LeafNodeImpl::Init() {
    m_worker_manager->Init();
    m_sender_manager->Init();
    return true;
}

bool LeafNodeImpl::Exit() {
    return true;
}

bool LeafNodeImpl::Register() {
    LOG(INFO) << "register()";
    RegisterRequest request;
    RegisterResponse response;

    m_this_sequence_id = kSequenceIDStart;
    request.set_sequence_id(m_this_sequence_id);
    request.mutable_server_info()->CopyFrom(m_server_info);

    if (!m_master_client->Register(&request, &response, NULL)) {
        LOG(ERROR) << "Rpc failed: register, status = "
            << StatusCodeToString(response.status());
        return false;
    }
    if (response.status() == kMasterOk) {
        LOG(INFO) << "register success: " << response.ShortDebugString();
        m_server_info.set_status(kServerIsRunning);
        ++m_this_sequence_id;
        return true;
    }

    LOG(INFO) << "register fail: " << response.ShortDebugString();
    return false;
}

bool LeafNodeImpl::Report() {
    LOG(INFO) << "report()";
    ReportRequest request;
    request.set_sequence_id(m_this_sequence_id);
    request.mutable_server_info()->CopyFrom(m_server_info);
    m_worker_manager->FillReportRequest(&request);
    m_sender_manager->FillReportRequest(&request);

    int32_t retry = 0;
    while (retry < FLAGS_gunir_heartbeat_retry_times) {
        ReportResponse response;
        if (!m_master_client->Report(&request, &response, NULL)) {
            LOG(ERROR) << "Rpc failed: report, status = "
                << StatusCodeToString(response.status());
        } else if (response.status() == kMasterOk) {
            LOG(INFO) << "report success, response: "
                << response.ShortDebugString();
            if (response.sequence_id() != m_this_sequence_id) {
                LOG(WARNING) << "report is lost";
                m_this_sequence_id = response.sequence_id() + 1;
            } else {
                ++m_this_sequence_id;
            }
            return true;
        } else if (response.status() == kServerNotRegistered) {
            if (!Register()) {
                return false;
            }
            return true;
        } else {
            LOG(ERROR) << "report failed: " << response.DebugString();
            toft::ThisThread::Sleep(retry * FLAGS_gunir_heartbeat_period *
                                    FLAGS_gunir_heartbeat_retry_period_factor);
        }
        ++retry;
    }
    return false;
    return true;
}

bool LeafNodeImpl::PushLeafTask(const PushLeafTaskRequest* request,
                                PushLeafTaskResponse* response) {
    LOG(INFO) << "rpc (PushLeafTask): " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());
    for (int32_t i = 0; i < request->leaf_spec_size(); ++i) {
        m_worker_manager->PushTask(new LeafTaskSpec(request->leaf_spec(i)));
    }
    response->set_status(kServerOk);
    return true;
}


} // namespace leafnode
} // namespace gunir
