// Copyright (C) 2015. The Gunir Authors. All rights reserved
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#include "gunir/leafnode/worker_manager.h"

#include <deque>

#include "gunir/leafnode/task_container_pool.h"
#include "gunir/leafnode/task_worker.h"
#include "gunir/leafnode/worker_thread.h"
#include "gunir/utils/message_utils.h"

DECLARE_int64(gunir_leafnode_container_memory_limit);


namespace gunir {
namespace leafnode {

WorkerManager::WorkerManager(TaskContainerPool* container_pool,
                             int32_t worker_thread_number)
    : m_container_pool(container_pool),
      m_worker_thread_number(worker_thread_number),
      m_container_memory_limit(FLAGS_gunir_leafnode_container_memory_limit) {
}

WorkerManager::~WorkerManager() {
    JoinThread();
    for (int32_t i = 0 ; i < m_worker_thread_number ;  i++) {
        delete m_worker_threads[i];
    }
}

void WorkerManager::JoinThread() {
    for (int32_t i = 0 ; i < m_worker_thread_number ;  i++) {
        m_worker_threads[i]->SendStopRequest();
        m_worker_threads[i]->Join();
    }
}

void WorkerManager::InitWorkerThread() {
    m_worker_threads.reset(new WorkerThread*[m_worker_thread_number]);
    for (int32_t i = 0 ; i < m_worker_thread_number ;  i++) {
        TaskWorker* worker = new TaskWorker(m_container_pool);

        m_worker_threads[i] = new WorkerThread(this, i, worker);
        m_worker_threads[i]->Init();
        m_worker_threads[i]->Start();
    }
}

void WorkerManager::Init() {
    InitWorkerThread();
}

void WorkerManager::PushTask(LeafTaskSpec* leaf_task_spec) {
    LOG(INFO) << "WorkerManager PushTask";
    m_task_queue.push_back(leaf_task_spec);
}

bool WorkerManager::PopNextTask(LeafTaskSpec** leaf_task_spec) {
    int64_t total_memory = m_container_pool->GetContainerSize();
    if (total_memory >= m_container_memory_limit) {
        LOG(WARNING) << "Container pool out of memory : "
            << total_memory << " : " << m_container_memory_limit;
        toft::ThisThread::Sleep(100);
        return false;
    }
    if (m_task_queue.empty()) {
        return false;
    }
    *leaf_task_spec = m_task_queue.front();
    m_task_queue.pop_front();
//     LOG(INFO) << "pop next compute task succeed :"
//         << (*leaf_task_spec)->ShortDebugString();
    return true;
}

void WorkerManager::CancelTask(const ReportResponse& response) {
    std::deque<LeafTaskSpec*> spec_queue(m_task_queue);
    m_task_queue.clear();
    while (!spec_queue.empty()) {
        LeafTaskSpec* spec = spec_queue.front();
        if (ShouldTaskCancel(response, spec->task_info())) {
            LOG(WARNING) << "Cancel compute task "
                << spec->task_info().ShortDebugString();

            // add task states to thread[0]
            m_worker_threads[0]->UpdateTaskState(spec->task_info(),
                                                 kCanceledTask);
            delete spec;
        } else {
            PushTask(spec);
        }
        spec_queue.pop_front();
    }
}

void WorkerManager::FillReportRequest(ReportRequest *request) {
    for (int32_t i = 0; i < m_worker_thread_number; ++i) {
        m_worker_threads[i]->FillReportRequest(request);
    }
}

}  // namespace leafnode
}  // namespace gunir
