// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#ifndef GUNIR_MASTER_JOB_MANAGER_H
#define GUNIR_MASTER_JOB_MANAGER_H

#include <map>

#include "toft/base/scoped_ptr.h"
#include "toft/container/blocking_queue.hpp"
#include "toft/system/atomic/atomic.h"
#include "toft/system/threading/mutex.h"
#include "toft/system/threading/rwlock.h"
#include "toft/system/threading/thread_pool.h"
#include "toft/system/timer/timer_manager.h"

#include "gunir/master/scheduler_plan.pb.h"
#include "gunir/proto/master_rpc.pb.h"

namespace gunir {
namespace master {

class Job;
class JobAnalyser;
class TableManager;

class JobManager {
public:
    JobManager(TableManager* table_manager,
               toft::TimerManager* timer_manager);
    ~JobManager();

    bool AddJob(const SubmitJobRequest* request,
                SubmitJobResponse* response);

    void GetJobResult(const GetJobResultRequest* request,
                      GetJobResultResponse* response);

    void UpdateJobStatus(Job* job, const JobStatus& job_status,
                         const std::string& failed_reason);

    bool PopJobForScheduler(SchedulerPlan* plan,
                            const uint32_t& waiting_time);
    bool FinishedSchedulerPlan(const SchedulerPlan& plan);
    void SchedulerJobFailed(uint32_t job_id);

    bool ReportTaskState(const TaskState& task_state);
    bool PopJobForEmitter(EmitterPlan* plan,
                          const uint32_t& waiting_time);

    bool EmitTaskSucceed(const TaskInfo& task_info);
    void EmitTaskFailed(const TaskInfo& task_info);

private:
    void AnalyseJob(Job* job);
    void StartAnalyse(Job* job);
    void GenerateSchedulerPlan(const SchedulerPlan::SchedulerType& type,
                               Job* job,
                               const TaskType& task_type,
                               const uint32_t& task_id);
    Job* GetJob(uint64_t job_id) const;
    Job* GetRunningJob(uint64_t job_id) const;
    void AddRemoveJobTimer(uint64_t job_id);
    void RemoveJob(uint64_t job_id, uint64_t timer_id);

    void MarkJobFailed(Job* job, const TaskStatus& status);

private:
    mutable toft::RwLock m_rwlock;
    std::map<uint64_t, Job*> m_jobs;
    TableManager* m_table_manager;
    toft::TimerManager* m_timer_manager;
    toft::scoped_ptr<JobAnalyser> m_job_analyser;

    toft::Atomic<uint64_t> m_cur_job_id;
    toft::scoped_ptr<toft::ThreadPool> m_thread_pool;

    toft::BlockingQueue<SchedulerPlan> m_scheduler_waiting_queue;
    toft::BlockingQueue<EmitterPlan> m_emit_waiting_queue;
};

} // namespace master
} // namespace gunir


#endif // GUNIR_MASTER_JOB_MANAGER_H
