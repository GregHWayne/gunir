// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#ifndef GUNIR_LEAF_STEMNODE_CLIENT_H
#define GUNIR_LEAF_STEMNODE_CLIENT_H

#include "trident/pbrpc.h"

#include "gunir/proto/stemnode_rpc.pb.h"
#include "gunir/rpc_client.h"

DECLARE_int32(gunir_stemnode_connect_retry_period);
DECLARE_int32(gunir_stemnode_rpc_timeout_period);

class toft::ThreadPool;

namespace gunir {
namespace stemnode {

class StemNodeClient : public RpcClient<StemNodeServer::Stub> {
public:
    static void SetThreadPool(toft::ThreadPool* thread_pool);

    static void SetRpcOption(int32_t max_inflow, int32_t max_outflow,
                             int32_t pending_buffer_size, int32_t thread_num);

    StemNodeClient(const std::string& addr = "",
                   int32_t rpc_timeout = FLAGS_gunir_stemnode_rpc_timeout_period);

    ~StemNodeClient();

    bool PushInterTask(const PushInterTaskRequest* request,
                  PushInterTaskResponse* response,
                  toft::Closure<void
                                (PushInterTaskRequest*, PushInterTaskResponse*,
                                 bool, int)>* done = NULL);

    bool ReportTaskResult(const ReportTaskResultRequest* request,
                          ReportTaskResultResponse* response,
                          toft::Closure<void
                                        (ReportTaskResultRequest*, ReportTaskResultResponse*,
                                         bool, int)>* done = NULL);

    bool ReportResultSize(const ReportResultSizeRequest* request,
                          ReportResultSizeResponse* response,
                          toft::Closure<void
                                        (ReportResultSizeRequest*, ReportResultSizeResponse*,
                                         bool, int)>* done = NULL);

private:
    bool IsRetryStatus(const StatusCode& status);

    int32_t m_rpc_timeout;
    static toft::ThreadPool* m_thread_pool;
};

} // namespace stemnode
} // namespace gunir

#endif // GUNIR_LEAF_STEMNODE_CLIENT_H
