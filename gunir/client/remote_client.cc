// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#include "gunir/client/remote_client.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "toft/system/threading/this_thread.h"

#include "gunir/master/master_client.h"


DEFINE_int32(gunir_client_thread_min_num, 1, "");
DEFINE_int32(gunir_client_thread_max_num, 1, "");

namespace gunir {
namespace client {


RemoteClient::RemoteClient()
    : m_master_client(new master::MasterClient()),
      m_thread_pool(new toft::ThreadPool(FLAGS_gunir_client_thread_min_num,
                                         FLAGS_gunir_client_thread_max_num)) {
    master::MasterClient::SetThreadPool(m_thread_pool.get());
    master::MasterClient::SetOption();
}

RemoteClient::~RemoteClient() {}

bool RemoteClient::SubmitJob(const SubmitJobRequest* request,
                             SubmitJobResponse* response) {
    LOG(INFO) << "send request (SubmitJob) to master: "
        << request->ShortDebugString();
    return m_master_client->SubmitJob(request, response, NULL);
}

bool RemoteClient::GetJobResult(const GetJobResultRequest* request,
                                GetJobResultResponse* response) {
    LOG(INFO) << "send request (GetJobResult) to master: "
        << request->ShortDebugString();
    return m_master_client->GetJobResult(request, response, NULL);
}

bool RemoteClient::GetMetaInfo(const GetMetaInfoRequest* request,
                               GetMetaInfoResponse* response) {
    LOG(INFO) << "send request (GetMetaInfo) to master: "
        << request->ShortDebugString();
    return m_master_client->GetMetaInfo(request, response, NULL);
}

bool RemoteClient::AddTable(const AddTableRequest* request,
                            AddTableResponse* response) {
    LOG(INFO) << "send request (AddTable) to master: "
        << request->ShortDebugString();
    return m_master_client->AddTable(request, response, NULL);
}

} // namespace client
} // namespace gunir
