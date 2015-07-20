// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#ifndef  GUNIR_LEAFNODE_WORKER_THREAD_H
#define  GUNIR_LEAFNODE_WORKER_THREAD_H

#include "toft/base/scoped_ptr.h"
#include "toft/base/uncopyable.h"
#include "toft/system/memory/mempool.h"

#include "gunir/proto/master_rpc.pb.h"
#include "gunir/leafnode/server_thread.h"

namespace gunir {

class LeafTaskSpec;
class TaskState;

namespace leafnode {

class WorkerManager;
class TaskWorker;

class WorkerThread : public ServerThread {
    DECLARE_UNCOPYABLE(WorkerThread);

public:
    WorkerThread(WorkerManager* compute_scheduler,
                  int32_t thread_index,
                  TaskWorker* task_runner);

    ~WorkerThread();
    void Init();

private:
    void Entry();
    bool RunTask();

private:
    toft::scoped_ptr<toft::MemPool> m_mempool;
    LeafTaskSpec* m_leaf_task_spec;
    WorkerManager *m_worker_manager;
    toft::scoped_ptr<TaskWorker> m_worker;
};

}  // namespace leafnode
}  // namespace gunir

#endif  // GUNIR_LEAFNODE_WORKER_THREAD_H

