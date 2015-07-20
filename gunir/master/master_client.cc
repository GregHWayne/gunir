// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "gunir/master/master_client.h"

namespace gunir {
namespace master {

toft::ThreadPool* MasterClient::m_thread_pool = NULL;

void MasterClient::SetThreadPool(toft::ThreadPool* thread_pool) {
    m_thread_pool = thread_pool;
}

void MasterClient::SetRpcOption(int32_t max_inflow, int32_t max_outflow,
                                int32_t pending_buffer_size, int32_t thread_num) {
    RpcClientBase::SetOption(max_inflow, max_outflow,
                             pending_buffer_size, thread_num);
}

MasterClient::MasterClient(const std::string& server_addr,
                           int32_t rpc_timeout)
    : RpcClient<MasterServer::Stub>(server_addr),
      m_rpc_timeout(rpc_timeout) {}

MasterClient::~MasterClient() {}

bool MasterClient::SubmitJob(const SubmitJobRequest* request,
                            SubmitJobResponse* response,
                            toft::Closure<void (SubmitJobRequest*, SubmitJobResponse*, bool, int)>* done) {
    return SendMessageWithRetry(&MasterServer::Stub::SubmitJob,
                                request, response, done, "SubmitJob",
                                m_rpc_timeout, m_thread_pool);
}

bool MasterClient::GetJobResult(const GetJobResultRequest* request,
                                GetJobResultResponse* response,
                                toft::Closure<void (GetJobResultRequest*, GetJobResultResponse*, bool, int)>* done) {
    return SendMessageWithRetry(&MasterServer::Stub::GetJobResult,
                                request, response, done, "GetJobResult",
                                m_rpc_timeout, m_thread_pool);
}

bool MasterClient::GetMetaInfo(const GetMetaInfoRequest* request,
                               GetMetaInfoResponse* response,
                               toft::Closure<void (GetMetaInfoRequest*, GetMetaInfoResponse*, bool, int)>* done) {
    return SendMessageWithRetry(&MasterServer::Stub::GetMetaInfo,
                                request, response, done, "GetMetaInfo",
                                m_rpc_timeout, m_thread_pool);
}

bool MasterClient::AddTable(const AddTableRequest* request,
                            AddTableResponse* response,
                            toft::Closure<void (AddTableRequest*, AddTableResponse*, bool, int)>* done) {
    return SendMessageWithRetry(&MasterServer::Stub::AddTable,
                                request, response, done, "AddTable",
                                m_rpc_timeout, m_thread_pool);
}

bool MasterClient::DropTable(const DropTableRequest* request,
                             DropTableResponse* response,
                             toft::Closure<void (DropTableRequest*, DropTableResponse*, bool, int)>* done) {
    return SendMessageWithRetry(&MasterServer::Stub::DropTable,
                                request, response, done, "DropTable",
                                m_rpc_timeout, m_thread_pool);
}

bool MasterClient::Register(const RegisterRequest* request,
                            RegisterResponse* response,
                            toft::Closure<void (RegisterRequest*, RegisterResponse*, bool, int)>* done) {
    return SendMessageWithRetry(&MasterServer::Stub::Register,
                                request, response, done, "Register",
                                m_rpc_timeout, m_thread_pool);
}

bool MasterClient::Report(const ReportRequest* request,
                          ReportResponse* response,
                          toft::Closure<void (ReportRequest*, ReportResponse*, bool, int)>* done) {
    return SendMessageWithRetry(&MasterServer::Stub::Report,
                                request, response, done, "Report",
                                m_rpc_timeout, m_thread_pool);
}

bool MasterClient::IsRetryStatus(const StatusCode& status) {
    return (status == kMasterNotInited
            || status == kMasterIsBusy);
}

} // namespace master
} // namespace gunir
