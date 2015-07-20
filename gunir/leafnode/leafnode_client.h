// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#ifndef GUNIR_LEAF_LEAFNODE_CLIENT_H
#define GUNIR_LEAF_LEAFNODE_CLIENT_H

#include "trident/pbrpc.h"

#include "gunir/proto/leafnode_rpc.pb.h"
#include "gunir/rpc_client.h"

DECLARE_int32(gunir_leafnode_connect_retry_period);
DECLARE_int32(gunir_leafnode_rpc_timeout_period);

class toft::ThreadPool;

namespace gunir {
namespace leafnode {

class LeafNodeClient : public RpcClient<LeafNodeServer::Stub> {
public:
    static void SetThreadPool(toft::ThreadPool* thread_pool);

    static void SetRpcOption(int32_t max_inflow, int32_t max_outflow,
                             int32_t pending_buffer_size, int32_t thread_num);

    LeafNodeClient(const std::string& addr = "",
                   int32_t rpc_timeout = FLAGS_gunir_leafnode_rpc_timeout_period);

    ~LeafNodeClient();

    bool PushLeafTask(const PushLeafTaskRequest* request,
                  PushLeafTaskResponse* response,
                  toft::Closure<void
                                (PushLeafTaskRequest*, PushLeafTaskResponse*,
                                 bool, int)>* done = NULL);


private:
    bool IsRetryStatus(const StatusCode& status);

    int32_t m_rpc_timeout;
    static toft::ThreadPool* m_thread_pool;
};

} // namespace leafnode
} // namespace gunir

#endif // GUNIR_LEAF_LEAFNODE_CLIENT_H
