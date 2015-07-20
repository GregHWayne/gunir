// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_LEAFNODE_WORKER_MANAGER_H
#define  GUNIR_LEAFNODE_WORKER_MANAGER_H

#include <deque>

#include "toft/base/scoped_ptr.h"
#include "toft/base/uncopyable.h"

#include "gunir/proto/master_rpc.pb.h"
// #include "gunir/leafnode/task_state_cmp.h"

namespace gunir {

class LeafTaskSpec;
class ServerInfo;
class TaskState;

namespace leafnode {
class WorkerThread;
class TaskContainerPool;

class WorkerManager {
    DECLARE_UNCOPYABLE(WorkerManager);

public:
    WorkerManager(TaskContainerPool *container_pool, int compute_thread_number);
    ~WorkerManager();

    void Init();

    void PushTask(LeafTaskSpec* leaf_task_spec);

    bool PopNextTask(LeafTaskSpec** leaf_task_spec);

    void FillReportRequest(ReportRequest *request);

    void CancelTask(const ReportResponse& response);

protected:
    void JoinThread();

    void InitWorkerThread();

protected:
    TaskContainerPool* m_container_pool;
    int32_t m_worker_thread_number;
    int64_t m_container_memory_limit;

    toft::scoped_array<WorkerThread*> m_worker_threads;
    std::deque<LeafTaskSpec*> m_task_queue;
};

}  // namespace leafnode
}  // namespace gunir

#endif  // GUNIR_LEAFNODE_WORKER_MANAGER_H

