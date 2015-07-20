// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "gunir/stemnode/stemnode_client.h"

namespace gunir {
namespace stemnode {

toft::ThreadPool* StemNodeClient::m_thread_pool = NULL;

void StemNodeClient::SetThreadPool(toft::ThreadPool* thread_pool) {
    m_thread_pool = thread_pool;
}

void StemNodeClient::SetRpcOption(int32_t max_inflow, int32_t max_outflow,
                                int32_t pending_buffer_size, int32_t thread_num) {
    RpcClientBase::SetOption(max_inflow, max_outflow,
                             pending_buffer_size, thread_num);
}

StemNodeClient::StemNodeClient(const std::string& server_addr,
                           int32_t rpc_timeout)
    : RpcClient<StemNodeServer::Stub>(server_addr),
      m_rpc_timeout(rpc_timeout) {}

StemNodeClient::~StemNodeClient() {}

bool StemNodeClient::PushInterTask(const PushInterTaskRequest* request,
                                   PushInterTaskResponse* response,
                                   toft::Closure<void (PushInterTaskRequest*,
                                                       PushInterTaskResponse*, bool, int)>* done) {
    return SendMessageWithRetry(&StemNodeServer::Stub::PushInterTask,
                                request, response, done, "PushInterTask",
                                m_rpc_timeout, m_thread_pool);
}

bool StemNodeClient::ReportTaskResult(const ReportTaskResultRequest* request,
                                      ReportTaskResultResponse* response,
                                      toft::Closure<void (ReportTaskResultRequest*,
                                                          ReportTaskResultResponse*,
                                                          bool, int)>* done) {
    return SendMessageWithRetry(&StemNodeServer::Stub::ReportTaskResult,
                                request, response, done, "ReportTaskResult",
                                m_rpc_timeout, m_thread_pool);
}

bool StemNodeClient::ReportResultSize(const ReportResultSizeRequest* request,
                                      ReportResultSizeResponse* response,
                                      toft::Closure<void (ReportResultSizeRequest*,
                                                          ReportResultSizeResponse*,
                                                          bool, int)>* done) {
    return SendMessageWithRetry(&StemNodeServer::Stub::ReportResultSize,
                                request, response, done, "ReportResultSize",
                                m_rpc_timeout, m_thread_pool);
}

bool StemNodeClient::IsRetryStatus(const StatusCode& status) {
    return (status == kServerNotInited
            || status == kServerIsBusy);
}

} // namespace stemnode
} // namespace gunir
