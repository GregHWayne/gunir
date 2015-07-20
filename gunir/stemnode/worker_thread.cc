// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/stemnode/worker_thread.h"

#include <string>
#include <vector>

#include "gunir/stemnode/result_manager.h"
#include "gunir/stemnode/task_worker.h"
#include "gunir/types.h"
#include "gunir/utils/message_utils.h"

DECLARE_int64(gunir_stemnode_result_memory_limit);
DECLARE_int64(gunir_blocking_queue_time);

namespace gunir {
namespace stemnode {

WorkerThread::WorkerThread()
    : m_thread_index(0),
      m_is_idle(true),
      m_is_processing(false),
      m_status(kNewTask),
      m_total_content_bytes(0),
      m_result_manager(new ResultManager()),
      m_task_worker(new TaskWorker()) {
    Start();
}

WorkerThread::~WorkerThread() {
    SendStopRequest();
    Join();
}

void WorkerThread::Init(uint32_t thread_index) {
    m_thread_index = thread_index;
}

void WorkerThread::Entry() {
    InterTaskSpec spec;
    while (!IsStopRequested()) {
        if (!m_task_queue.Pop(
                &spec, FLAGS_gunir_blocking_queue_time)) {
            continue;
        }

        if (m_status == kNewTask) {
            m_is_processing = true;
            if (ProcessTask(spec)) {
            } else {
            }
            m_result_manager->Clear();
            m_task_worker->Clear();
            m_is_processing = false;
        }
    }
}

void WorkerThread::AddTaskSpec(const InterTaskSpec& spec) {
    m_task_spec.CopyFrom(spec);
    UpdateTaskStatus(kNewTask);

    m_result_manager->Reset(spec);
    m_total_content_bytes = 0;
}

void WorkerThread::ReportResultSize(const ReportResultSizeRequest* request,
                                    ReportResultSizeResponse* response) {
    if (kNewTask != m_status) {
        response->set_result(ReportResultSizeResponse::kReject);
        return;
    }

    m_result_manager->ReportResultSize(request, response);
}

void WorkerThread::ReportTaskResult(const ReportTaskResultRequest* request,
                                   ReportTaskResultResponse* response) {
    // when all task result is prepared , we start process task
    if (kNewTask != m_status) {
        response->set_result(ReportTaskResultResponse::kReject);
        return;
    }

    bool is_add = false;
    if (m_result_manager->AddAndJudgeIsFull(request, response, &is_add)) {
        LOG(INFO) << "Finish collect result :";
        m_task_queue.Push(m_task_spec);
        return;
    }

    if (is_add) {
        m_total_content_bytes += request->content().length();
        if (m_total_content_bytes > FLAGS_gunir_stemnode_result_memory_limit) {
            LOG(ERROR) << "Inter collector out of memory : "
                << m_total_content_bytes;
            UpdateTaskStatus(kResultManagerOOMTask);
        }
    }
}

void WorkerThread::FillReportTaskState(TaskState* task_state) {
    task_state->mutable_task_info()->CopyFrom(m_task_spec.task_info());
    task_state->mutable_task_info()->set_task_status(m_status);
    if (IsTaskCompleted(task_state->task_info())
        && IsRootTask(task_state->task_info())) {
        m_task_worker->FillReportTaskState(task_state);
    }
}

void WorkerThread::UpdateTaskStatus(const TaskStatus& status) {
    m_status = status;
}

bool WorkerThread::ProcessTask(const InterTaskSpec& spec) {
    LOG(INFO) << "Start reset task runner "
        << m_task_spec.task_info().ShortDebugString();;
    m_task_worker->Reset(spec);

    LOG(INFO) << "Start open task input ";
    std::vector<StringPiece> results;
    m_result_manager->GetAllResult(&results);
    DCHECK_NE(results.size(), 0);
    m_task_worker->Open(results);

    LOG(INFO) << "Start run task ";
    if (!m_task_worker->Run()) {
        LOG(ERROR) << "TaskWorker Run error ";
        UpdateTaskStatus(kInterResultOOMTask);
        return false;
    }

    LOG(INFO) << "Start close task ";
    if (!m_task_worker->Close()) {
        LOG(ERROR) << "TaskWorker Close error ";
        UpdateTaskStatus(kFailedComputeTask);
        return false;
    }

    UpdateTaskStatus(kCompletedTask);

    LOG(INFO) << "TaskWorker Succeed : "
        << m_task_spec.task_info().ShortDebugString();;
    return true;
}

bool WorkerThread::IsIdle() const {
    return m_is_idle;
}

bool WorkerThread::IsProcessing() const {
    return m_is_processing;
}

void WorkerThread::SetThreadIdle(bool state) {
    m_is_idle = state;
}

} // namespace stemnode
} // namespace gunir
