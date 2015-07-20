// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_STEMNODE_WORKER_THREAD_H
#define  GUNIR_STEMNODE_WORKER_THREAD_H

#include <deque>
#include <string>

#include "toft/base/scoped_ptr.h"
#include "toft/base/uncopyable.h"
#include "toft/container/blocking_queue.hpp"
#include "toft/system/atomic/atomic.h"
#include "toft/system/threading/base_thread.h"
#include "toft/system/threading/rwlock.h"

#include "gunir/proto/stemnode_rpc.pb.h"

namespace gunir {
namespace stemnode {

class ResultManager;
class TaskWorker;

class WorkerThread : public  toft::BaseThread {
    DECLARE_UNCOPYABLE(WorkerThread);

public:
    WorkerThread();

    void Init(uint32_t thread_index);

    ~WorkerThread();

    void AddTaskSpec(const InterTaskSpec& spec);

    void ReportResultSize(const ReportResultSizeRequest* request,
                          ReportResultSizeResponse* response);
    void ReportTaskResult(const ReportTaskResultRequest* request,
                          ReportTaskResultResponse* response);

    void FillReportTaskState(TaskState* task_state);

    bool IsIdle() const;

    bool IsProcessing() const;

    void SetThreadIdle(bool state);

    void UpdateTaskStatus(const TaskStatus& status);

protected:
    void Entry();
    bool ProcessTask(const InterTaskSpec& spec);

private:
    uint32_t m_thread_index;
    toft::Atomic<bool> m_is_idle;
    toft::Atomic<bool> m_is_processing;
    toft::Atomic<TaskStatus> m_status;
    int64_t m_total_content_bytes;

    InterTaskSpec m_task_spec;
    toft::scoped_ptr<ResultManager> m_result_manager;
    toft::scoped_ptr<TaskWorker> m_task_worker;
    toft::BlockingQueue<InterTaskSpec> m_task_queue;
};

}  // namespace stemnode
}  // namespace gunir

#endif  // GUNIR_STEMNODE_WORKER_THREAD_H
