// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "gunir/leafnode/leafnode_client.h"

namespace gunir {
namespace leafnode {

toft::ThreadPool* LeafNodeClient::m_thread_pool = NULL;

void LeafNodeClient::SetThreadPool(toft::ThreadPool* thread_pool) {
    m_thread_pool = thread_pool;
}

void LeafNodeClient::SetRpcOption(int32_t max_inflow, int32_t max_outflow,
                                int32_t pending_buffer_size, int32_t thread_num) {
    RpcClientBase::SetOption(max_inflow, max_outflow,
                             pending_buffer_size, thread_num);
}

LeafNodeClient::LeafNodeClient(const std::string& server_addr,
                           int32_t rpc_timeout)
    : RpcClient<LeafNodeServer::Stub>(server_addr),
      m_rpc_timeout(rpc_timeout) {}

LeafNodeClient::~LeafNodeClient() {}

bool LeafNodeClient::PushLeafTask(const PushLeafTaskRequest* request,
                              PushLeafTaskResponse* response,
                              toft::Closure<void (PushLeafTaskRequest*, PushLeafTaskResponse*, bool, int)>* done) {
    return SendMessageWithRetry(&LeafNodeServer::Stub::PushLeafTask,
                                request, response, done, "PushLeafTask",
                                m_rpc_timeout, m_thread_pool);
}

bool LeafNodeClient::IsRetryStatus(const StatusCode& status) {
    return (status == kServerNotInited
            || status == kServerIsBusy);
}

} // namespace leafnode
} // namespace gunir
