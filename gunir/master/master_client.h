// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#ifndef GUNIR_MASTER_MASTER_CLIENT_H
#define GUNIR_MASTER_MASTER_CLIENT_H

#include "trident/pbrpc.h"
#include "thirdparty/gflags/gflags.h"

#include "gunir/proto/master_rpc.pb.h"
#include "gunir/rpc_client.h"

DECLARE_string(gunir_master_addr);
DECLARE_string(gunir_master_port);

// DECLARE_int32(gunir_master_connect_retry_period);
DECLARE_int32(gunir_master_rpc_timeout_period);

class toft::ThreadPool;

namespace gunir {
namespace master {

class MasterClient : public RpcClient<MasterServer::Stub> {
public:
    static void SetThreadPool(toft::ThreadPool* thread_pool);

    static void SetRpcOption(int32_t max_inflow, int32_t max_outflow,
                             int32_t pending_buffer_size, int32_t thread_num);

    MasterClient(const std::string& addr = (FLAGS_gunir_master_addr + ":" + FLAGS_gunir_master_port),
                 int32_t rpc_timeout = FLAGS_gunir_master_rpc_timeout_period);

    ~MasterClient();

    bool SubmitJob(const SubmitJobRequest* request,
                   SubmitJobResponse* response,
                   toft::Closure<void
                                (SubmitJobRequest*, SubmitJobResponse*,
                                 bool, int)>* done = NULL);

    bool GetJobResult(const GetJobResultRequest* request,
                      GetJobResultResponse* response,
                      toft::Closure<void
                              (GetJobResultRequest*, GetJobResultResponse*,
                               bool, int)>* done = NULL);

    bool GetMetaInfo(const GetMetaInfoRequest* request,
                     GetMetaInfoResponse* response,
                     toft::Closure<void
                              (GetMetaInfoRequest*, GetMetaInfoResponse*,
                               bool, int)>* done = NULL);

    bool AddTable(const AddTableRequest* request,
                  AddTableResponse* response,
                  toft::Closure<void
                              (AddTableRequest*, AddTableResponse*,
                               bool, int)>* done = NULL);

    bool DropTable(const DropTableRequest* request,
                   DropTableResponse* response,
                   toft::Closure<void
                              (DropTableRequest*, DropTableResponse*,
                               bool, int)>* done = NULL);

    bool Register(const RegisterRequest* request,
                  RegisterResponse* response,
                  toft::Closure<void
                                (RegisterRequest*, RegisterResponse*,
                                 bool, int)>* done = NULL);

    bool Report(const ReportRequest* request,
                ReportResponse* response,
                toft::Closure<void
                              (ReportRequest*, ReportResponse*,
                               bool, int)>* done = NULL);

private:
    bool IsRetryStatus(const StatusCode& status);

    int32_t m_rpc_timeout;
    static toft::ThreadPool* m_thread_pool;
};

} // namespace master
} // namespace gunir

#endif // GUNIR_MASTER_MASTER_CLIENT_H
