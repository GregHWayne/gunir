// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef GUNIR_STEMNODE_STEMNODE_IMPL_H
#define GUNIR_STEMNODE_STEMNODE_IMPL_H

#include "toft/base/scoped_ptr.h"
#include "toft/system/threading/mutex.h"
#include "toft/system/threading/thread_pool.h"

#include "gunir/proto/server_info.pb.h"
#include "gunir/proto/stemnode_rpc.pb.h"
#include "gunir/proto/status_code.pb.h"

namespace gunir {

namespace master {
class MasterClient;
}

namespace stemnode {

class WorkerManager;

class StemNodeImpl {
public:
    enum StemNodeStatus {
        kNotInited = kServerNotInited,
        kIsIniting = kServerIsIniting,
        kIsBusy = kServerIsBusy,
        kRunning = kServerIsRunning
    };

    StemNodeImpl(const ServerInfo& node_info);
    ~StemNodeImpl();

    bool Init();

    bool Exit();

    bool Register();

    bool Report();

    bool PushInterTask(const PushInterTaskRequest* request,
                       PushInterTaskResponse* response);

    bool ReportTaskResult(const ReportTaskResultRequest* request,
                          ReportTaskResultResponse* response);

    bool ReportResultSize(const ReportResultSizeRequest* request,
                          ReportResultSizeResponse* response);

private:
    mutable toft::Mutex m_status_mutex;

    ServerInfo m_server_info;
    StemNodeStatus m_status;
    uint64_t m_this_sequence_id;

    toft::scoped_ptr<WorkerManager> m_worker_manager;

    toft::scoped_ptr<master::MasterClient> m_master_client;
    toft::scoped_ptr<toft::ThreadPool> m_thread_pool;
};


} // namespace stemnode
} // namespace gunir

#endif // GUNIR_STEMNODE_STEMNODE_IMPL_H
