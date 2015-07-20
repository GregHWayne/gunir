// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#include "gunir/stemnode/stemnode_impl.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "toft/system/threading/this_thread.h"

#include "gunir/master/master_client.h"
#include "gunir/proto/master_rpc.pb.h"
#include "gunir/proto/proto_helper.h"
#include "gunir/stemnode/worker_manager.h"
#include "gunir/types.h"


DECLARE_int32(gunir_stemnode_thread_min_num);
DECLARE_int32(gunir_stemnode_thread_max_num);

DECLARE_int64(gunir_heartbeat_period);
DECLARE_int64(gunir_heartbeat_retry_period_factor);
DECLARE_int32(gunir_heartbeat_retry_times);

namespace gunir {
namespace stemnode {


StemNodeImpl::StemNodeImpl(const ServerInfo& node_info)
    : m_server_info(node_info),
      m_status(kNotInited),
      m_this_sequence_id(kSequenceIDStart),
      m_master_client(new master::MasterClient()),
      m_thread_pool(new toft::ThreadPool(FLAGS_gunir_stemnode_thread_min_num,
                                         FLAGS_gunir_stemnode_thread_max_num)) {
    master::MasterClient::SetThreadPool(m_thread_pool.get());
    master::MasterClient::SetOption();

    m_worker_manager.reset(new WorkerManager(m_server_info.slot_number()));
}

StemNodeImpl::~StemNodeImpl() {}

bool StemNodeImpl::Init() {
    return true;
}

bool StemNodeImpl::Exit() {
    return true;
}

bool StemNodeImpl::Register() {
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

bool StemNodeImpl::Report() {
    LOG(INFO) << "report()";
    ReportRequest request;
    request.set_sequence_id(m_this_sequence_id);
    request.mutable_server_info()->CopyFrom(m_server_info);
    m_worker_manager->FillReportRequest(&request);

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

bool StemNodeImpl::PushInterTask(const PushInterTaskRequest* request,
                                 PushInterTaskResponse* response) {
    LOG(INFO) << "rpc (PushInterTask): " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());
    m_worker_manager->AddTask(request->inter_spec());
    response->set_status(kServerOk);
    return true;
}

bool StemNodeImpl::ReportTaskResult(const ReportTaskResultRequest* request,
                                    ReportTaskResultResponse* response) {
    LOG(INFO) << "rpc (ReportTaskResult): " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());
    m_worker_manager->ReportTaskResult(request, response);
    response->set_status(kServerOk);
    return true;
}

bool StemNodeImpl::ReportResultSize(const ReportResultSizeRequest* request,
                                    ReportResultSizeResponse* response) {
    LOG(INFO) << "rpc (ReportResultSize): " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());
    m_worker_manager->ReportResultSize(request, response);
    response->set_status(kServerOk);
    return true;
}

} // namespace stemnode
} // namespace gunir
