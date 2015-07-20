// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/leafnode/worker_thread.h"

#include "thirdparty/glog/logging.h"

#include "gunir/leafnode/worker_manager.h"
#include "gunir/leafnode/task_worker.h"


namespace gunir {
namespace leafnode {

WorkerThread::WorkerThread(WorkerManager* compute_scheduler,
                           int32_t thread_index,
                           TaskWorker* task_worker)
    : ServerThread(thread_index),
      m_mempool(new MemPool(MemPool::MAX_UNIT_SIZE)),
      m_worker_manager(compute_scheduler),
      m_worker(task_worker) {
}

WorkerThread::~WorkerThread() {}

void WorkerThread::Init() {}

void WorkerThread::Entry() {
    while (!IsStopRequested()) {
        m_leaf_task_spec = NULL;
        if (!m_worker_manager->PopNextTask(&m_leaf_task_spec)) {
            continue;
        }
        if (!RunTask()) {
            LOG(ERROR) << "RunTask error";
        } else {
        }
        delete m_leaf_task_spec;
    }
}

bool WorkerThread::RunTask() {
    if (!m_worker->Reset(m_leaf_task_spec, m_mempool.get())) {
        LOG(ERROR) << "TaskWorker Reset error ";
        UpdateTaskState(m_leaf_task_spec->task_info(), kFailedTask);
        return false;
    }

    if (!m_worker->Open()) {
        LOG(ERROR) << "TaskWorker Open error ";
        UpdateTaskState(m_leaf_task_spec->task_info(), kFailedTask);
        return false;
    }

    if (!m_worker->Run()) {
        LOG(ERROR) << "TaskWorker Run error ";
        UpdateTaskState(m_leaf_task_spec->task_info(), kLeafResultOOMTask);
        return false;
    }

    if (!m_worker->Close()) {
        LOG(ERROR) << "TaskWorker Close error ";
        UpdateTaskState(m_leaf_task_spec->task_info(), kFailedComputeTask);
        return false;
    }

//     LOG(INFO) << "TaskWorker Succeed : "
//         << m_leaf_task_spec->task_info().ShortDebugString();

    return true;
}

}  // namespace leafnode
}  // namespace gunir
