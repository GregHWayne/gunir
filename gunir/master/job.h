// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_MASTER_JOB_H
#define  GUNIR_MASTER_JOB_H

#include <stdint.h>

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/uncopyable.h"
#include "toft/system/threading/rwlock.h"

#include "gunir/master/job_state.h"
#include "gunir/master/scheduler_plan.pb.h"
#include "gunir/proto/job.pb.h"
#include "gunir/proto/master_rpc.pb.h"

namespace gunir {
namespace master {

class TaskManager;

class Job {
    DECLARE_UNCOPYABLE(Job);

public:
    Job(const JobSpecification& request, uint64_t job_id);
    ~Job();

    // for client
    void GetJobResult(const GetJobResultRequest* request,
                      GetJobResultResponse* response) const;

    bool Init(const JobAnalyserResult& analyser_result);

    // for report
    void UpdateTaskState(const TaskState& task_state);

    // for scheduler
    void GenerateSchedulerPlan(SchedulerPlan* plan,
                                const TaskType& type,
                                const uint32_t& task_id);

    // for emitter
    void GenerateEmitterPlan(const SchedulerPlan& scheduler_plan,
                             EmitterPlan* emitter_plan);
    void EmitTaskSucceed(const TaskInfo& task_info);

    const JobSpecification& GetJobSpec() const;
    uint64_t GetJobID() const;

    void SetJobStartTime(int64_t t);
    void SetJobAnalyseFinishTime(int64_t t);
    void SetJobScheduleFinishTime(int64_t t);
    void SetJobFinishTime(int64_t t);
    void SetJobStatus(const JobStatus& job_status,
                      const std::string& failed_reason = "");
    void SetFailedReson(const std::string& reason);
    JobStatus GetJobStatus() const;

private:
    void AddTaskByPlan(const compiler::TaskPlanProto& plan,
                       const TaskType& type,
                       const uint32_t& parent_task_id);
    void SetJobEmitFinishTime(int64_t t);

private:
    mutable toft::RwLock m_rwlock;

    toft::scoped_ptr<JobState> m_job_state;
    toft::scoped_ptr<TaskManager> m_task_manager;
};

}  // namespace master
}  // namespace gunir

#endif  // GUNIR_MASTER_JOB_H
