// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/leafnode/sender_manager.h"

#include <deque>

#include "gunir/leafnode/task_container_pool.h"
#include "gunir/leafnode/sender_thread.h"
#include "gunir/utils/message_utils.h"

DECLARE_int64(gunir_blocking_queue_time);

namespace gunir {
namespace leafnode {

SenderManager::SenderManager(TaskContainerPool* container_pool,
                             int send_thread_number)
    : m_container_pool(container_pool),
      m_sender_thread_number(send_thread_number) {
}

SenderManager::~SenderManager() {
    JoinThread();
    for (int i = 0 ; i < m_sender_thread_number ;  i++) {
        delete m_sender_threads[i];
    }
}

void SenderManager::JoinThread() {
    for (int i = 0 ; i < m_sender_thread_number ;  i++) {
        m_sender_threads[i]->SendStopRequest();
        m_sender_threads[i]->Join();
    }
}

void SenderManager::Init() {
    m_sender_threads.reset(new SenderThread*[m_sender_thread_number]);
    for (int i = 0 ; i < m_sender_thread_number ;  i++) {
        m_sender_threads[i] = new SenderThread(this, i);
        m_sender_threads[i]->Start();
    }
}

void SenderManager::CancelTask(const ReportResponse& response) {
    std::deque<TaskContainer*> container_queue;
    TaskContainer* container = NULL;
    while (m_container_pool->PopNextContainer(&container)) {
        if (ShouldTaskCancel(response, container->GetTaskInfo())) {
            LOG(WARNING) << "Cancel send task "
                << container->GetTaskInfo().ShortDebugString();
            // add task states to SenderThread[0]
            m_sender_threads[0]->UpdateTaskState(container->GetTaskInfo(),
                                              kCanceledTask);
            delete container;
        } else {
            container_queue.push_back(container);
        }
    }

    while (!container_queue.empty()) {
        m_container_pool->AddContainer(container_queue.front());
        container_queue.pop_front();
    }
}

bool SenderManager::PopNextTask(TaskContainer** container) {
    return m_container_pool->PopNextContainer(container);
}

void SenderManager::FillReportRequest(ReportRequest *request) {
    for (int32_t i = 0; i < m_sender_thread_number; ++i) {
        m_sender_threads[i]->FillReportRequest(request);
    }
}

}  // namespace leafnode
}  // namespace gunir
