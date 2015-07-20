// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#include "gunir/stemnode/worker_manager.h"

#include "gunir/stemnode/worker_thread.h"
#include "gunir/utils/message_utils.h"
#include "gunir/types.h"

namespace gunir {
namespace stemnode {


WorkerManager::WorkerManager(uint32_t slot_num)
    : m_thread_num(slot_num) {
    InitWorkerThreads(slot_num);
}

WorkerManager::~WorkerManager() {
    JoinThreads();
}

void WorkerManager::InitWorkerThreads(uint32_t slot_num) {
    m_worker_threads.resize(slot_num);
    for (uint32_t i = 0; i < slot_num; ++i) {
        m_worker_threads[i] = new WorkerThread();
        m_worker_threads[i]->Init(i);
        m_idle_thread_list.push_back(i);
    }
}

void WorkerManager::JoinThreads() {
    for (uint32_t i = 0; i < m_worker_threads.size(); ++i) {
        delete m_worker_threads[i];
    }
    m_worker_threads.clear();
}

void WorkerManager::AddTask(const InterTaskSpecList& task_spec_list) {
    for (int32_t i = 0; i < task_spec_list.size(); ++i) {
        DCHECK_LT(0U, m_idle_thread_list.size());

        uint32_t thread_index = m_idle_thread_list.front();
        m_idle_thread_list.pop_front();
        const InterTaskSpec& spec = task_spec_list.Get(i);

        LOG(INFO) << "Add task : " << spec.task_info().ShortDebugString()
            << " to thread : " << thread_index;

        m_worker_threads[thread_index]->AddTaskSpec(spec);
        m_worker_threads[thread_index]->SetThreadIdle(false);

        toft::RwLock::WriterLocker locker(&m_rwlock);
        m_running_task_map[spec.task_info()] = thread_index;
    }
}

uint32_t WorkerManager::FindTask(const TaskInfo& task_info) {
    toft::RwLock::ReaderLocker locker(&m_rwlock);
    std::map<TaskInfo, uint32_t>::iterator it =
        m_running_task_map.find(task_info);
    if (m_running_task_map.end() == it) {
        return kUnknownId;
    }
    return it->second;
}

void WorkerManager::CancelTask(const FinishJobIdList& id_list) {
    for (int32_t i = 0; i < id_list.size(); ++i) {
        uint64_t job_id = id_list.Get(i);
        LOG(INFO) << "Job " << job_id << " finished, "
            << "cancel it's task";
        std::map<TaskInfo, uint32_t>::iterator iter;
        for (iter = m_running_task_map.begin();
             iter != m_running_task_map.end();
             ++iter) {
            if ((iter->first).job_id() == job_id
                && !m_worker_threads[iter->second]->IsProcessing()) {
                m_worker_threads[iter->second]->UpdateTaskStatus(kCanceledTask);
            }
        }
    }
}

void WorkerManager::ReportResultSize(const ReportResultSizeRequest* request,
                                     ReportResultSizeResponse* response) {
    const TaskInfo& parent_task_info = request->parent_task_info();
    uint32_t thread_index = FindTask(parent_task_info);
    if (kUnknownId == thread_index) {
        LOG(ERROR) << "Can't find parent task in this server, ParentTaskInfo :"
            << parent_task_info.ShortDebugString();
        response->set_result(ReportResultSizeResponse::kReject);
        return;
    }
    m_worker_threads[thread_index]->ReportResultSize(request, response);
}

void WorkerManager::ReportTaskResult(const ReportTaskResultRequest* request,
                                     ReportTaskResultResponse* response) {
    const TaskInfo& parent_task_info = request->parent_task_info();
    uint32_t thread_index = FindTask(parent_task_info);
    if (kUnknownId == thread_index) {
        LOG(ERROR) << "Can't find parent task in this server, ParentTaskInfo :"
            << parent_task_info.ShortDebugString();
        response->set_result(ReportTaskResultResponse::kReject);
        return;
    }
    m_worker_threads[thread_index]->ReportTaskResult(request, response);
}

void WorkerManager::FillReportRequest(ReportRequest* request) {
    for (uint32_t i = 0; i < m_thread_num; ++i) {
        if (!m_worker_threads[i]->IsIdle()) {
            TaskState* state = request->add_task_state();
            m_worker_threads[i]->FillReportTaskState(state);

            if (IsTaskFinished(state->task_info())) {
                m_worker_threads[i]->SetThreadIdle(true);
                m_idle_thread_list.push_back(i);
            }
        }
    }
}

} // namespace stemnode
} // namespace gunir
