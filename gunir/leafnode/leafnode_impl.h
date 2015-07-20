// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef GUNIR_LEAFNODE_LEAFNODE_IMPL_H
#define GUNIR_LEAFNODE_LEAFNODE_IMPL_H

#include "toft/base/scoped_ptr.h"
#include "toft/system/threading/mutex.h"
#include "toft/system/threading/thread_pool.h"

#include "gunir/proto/server_info.pb.h"
#include "gunir/proto/leafnode_rpc.pb.h"
#include "gunir/proto/status_code.pb.h"

namespace gunir {

namespace master {
class MasterClient;
}

namespace stemnode {
class StemNodeClient;
}

namespace leafnode {

class TaskContainerPool;
class WorkerManager;
class SenderManager;

class LeafNodeImpl {
public:
    enum LeafNodeStatus {
        kNotInited = kServerNotInited,
        kIsIniting = kServerIsIniting,
        kIsBusy = kServerIsBusy,
        kRunning = kServerIsRunning
    };

    LeafNodeImpl(const ServerInfo& node_info);
    ~LeafNodeImpl();

    bool Init();

    bool Exit();

    bool Register();

    bool Report();

    bool PushLeafTask(const PushLeafTaskRequest* request,
                      PushLeafTaskResponse* response);

private:
    mutable toft::Mutex m_status_mutex;

    ServerInfo m_server_info;
    LeafNodeStatus m_status;
    uint64_t m_this_sequence_id;

    toft::scoped_ptr<TaskContainerPool> m_task_container_pool;
    toft::scoped_ptr<WorkerManager> m_worker_manager;
    toft::scoped_ptr<SenderManager> m_sender_manager;

    toft::scoped_ptr<master::MasterClient> m_master_client;
    toft::scoped_ptr<stemnode::StemNodeClient> m_stemnode_client;
    toft::scoped_ptr<toft::ThreadPool> m_thread_pool;
};


} // namespace leafnode
} // namespace gunir

#endif // GUNIR_LEAFNODE_LEAFNODE_IMPL_H
