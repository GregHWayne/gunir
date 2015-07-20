// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "gunir/master/job_manager.h"

#include "toft/system/time/timestamp.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "gunir/master/job.h"
#include "gunir/master/job_analyser.h"
#include "gunir/master/table_manager.h"
#include "gunir/utils/message_utils.h"

DECLARE_int64(gunir_remove_job_waiting_time);

DECLARE_int32(gunir_job_thread_min_num);
DECLARE_int32(gunir_job_thread_max_num);

namespace gunir {
namespace master {


JobManager::JobManager(TableManager* table_manager,
                       toft::TimerManager* timer_manager)
    : m_table_manager(table_manager),
      m_timer_manager(timer_manager),
      m_job_analyser(new JobAnalyser(table_manager)),
      m_cur_job_id(0),
      m_thread_pool(new toft::ThreadPool(FLAGS_gunir_job_thread_min_num,
                                         FLAGS_gunir_job_thread_max_num)) {}

JobManager::~JobManager() {}

bool JobManager::AddJob(const SubmitJobRequest* request,
                        SubmitJobResponse* response) {
    toft::RwLock::WriterLocker locker(&m_rwlock);
    Job* job = m_jobs[m_cur_job_id] =
        new Job(request->job_spec(), m_cur_job_id);
    m_cur_job_id++;
    response->set_job_status(kJobSubmitSucceed);
    response->set_job_id(job->GetJobID());
    response->set_status(kMasterOk);

    AnalyseJob(job);
    return true;
}

void JobManager::AnalyseJob(Job* job) {
    toft::Closure<void ()>* callback =
        toft::NewClosure(this, &JobManager::StartAnalyse, job);
    m_thread_pool->AddTask(callback);
}

void JobManager::StartAnalyse(Job* job) {
    LOG(INFO) << "Start analyse job :" << job->GetJobID();

    JobAnalyserResult result;
    if (m_job_analyser->DoAnalyser(job->GetJobSpec(),
                                   &result)) {
        if (job->Init(result)) {
            // add to schedular waiting queue, waiting schedular to get it
            VLOG(20) << "Analyse job : " << job->GetJobID() << " succeed";
            UpdateJobStatus(job, kJobAnalyseSucceed, "");
            // start from root task , generate full job scheduler
            GenerateSchedulerPlan(SchedulerPlan::kFullJobScheduler,
                                  job, kInterTask, 0);
        } else {
            LOG(ERROR) << "Init job : " << job->GetJobID() << " failed ";
            UpdateJobStatus(job, kJobAnalyseFailed, result.failed_reason());
        }
    } else {
        LOG(ERROR) << "Analyse job : " << job->GetJobID() << " Failed ";
        UpdateJobStatus(job, kJobAnalyseFailed, result.failed_reason());
    }
    job->SetJobAnalyseFinishTime(toft::GetTimestampInMs());

    LOG(INFO) << "Finish analyse job :" << job->GetJobID();
}

void JobManager::GenerateSchedulerPlan(const SchedulerPlan::SchedulerType& type,
                                       Job* job,
                                       const TaskType& task_type,
                                       const uint32_t& task_id) {
    SchedulerPlan plan;
    plan.set_type(type);
    job->GenerateSchedulerPlan(&plan, task_type, task_id);
    if (type == SchedulerPlan::kFullJobScheduler) {
        m_scheduler_waiting_queue.Push(plan);
    } else {
        m_scheduler_waiting_queue.Push(plan);
    }
}

void JobManager::GetJobResult(const GetJobResultRequest* request,
                              GetJobResultResponse* response) {
    Job* job = GetJob(request->job_id());
    if (NULL == job) {
        response->mutable_result()->set_reason("Job not exist");
        response->mutable_result()->set_job_status(kJobNotExist);
        return;
    }

    job->GetJobResult(request, response);
}

Job* JobManager::GetJob(uint64_t job_id) const {
    toft::RwLock::ReaderLocker locker(&m_rwlock);

    std::map<uint64_t, Job*>::const_iterator it = m_jobs.find(job_id);
    if (it == m_jobs.end()) {
        return NULL;
    }

    return it->second;
}

void JobManager::AddRemoveJobTimer(uint64_t job_id) {
    toft::Closure<void (uint64_t)>* closure =
        toft::NewClosure(this, &JobManager::RemoveJob, job_id);
    m_timer_manager->AddOneshotTimer(FLAGS_gunir_remove_job_waiting_time,
                                    closure);
}

void JobManager::RemoveJob(uint64_t job_id, uint64_t timer_id) {
    toft::RwLock::WriterLocker locker(&m_rwlock);
    std::map<uint64_t, Job*>::iterator it = m_jobs.find(job_id);
    if (it != m_jobs.end()) {
        LOG(WARNING) << "Removed Job " << job_id << " not exist";
        return;
    }

    Job* job = it->second;
    m_jobs.erase(it);
    delete job;
    VLOG(10) << "Finish remove job : " << job_id;
}

void JobManager::UpdateJobStatus(Job* job, const JobStatus& job_status,
                                 const std::string& failed_reason) {
    uint64_t job_id = job->GetJobID();
    job->SetJobStatus(job_status, failed_reason);
    if (IsJobFinished(job_status)) {
        AddRemoveJobTimer(job_id);
    }
}

bool JobManager::PopJobForScheduler(SchedulerPlan* plan,
                                    const uint32_t& waiting_time) {
    if (!m_scheduler_waiting_queue.Pop(plan, waiting_time)) {
        return false;
    }
    return true;
}

void JobManager::SchedulerJobFailed(uint32_t job_id) {
    Job* job = GetRunningJob(job_id);
    if (NULL != job) {
        UpdateJobStatus(job, kJobSchedulerFailed,
                        "Job is too big to be schedulered in server now");
    }
}

bool JobManager::FinishedSchedulerPlan(const SchedulerPlan& scheduler_plan) {
    Job* job = GetRunningJob(scheduler_plan.job_id());
    if (NULL != job) {
        job->SetJobScheduleFinishTime(toft::GetTimestampInMs());
        EmitterPlan emitter_plan;
        job->GenerateEmitterPlan(scheduler_plan, &emitter_plan);
        m_emit_waiting_queue.Push(emitter_plan);
        return true;
    }
    LOG(WARNING) << "After scheduler finished, the job : "
        << scheduler_plan.job_id() << " is not exist ";
    return false;
}

Job* JobManager::GetRunningJob(uint64_t job_id) const {
    toft::RwLock::ReaderLocker locker(&m_rwlock);

    std::map<uint64_t, Job*>::const_iterator iter = m_jobs.find(job_id);
    if (m_jobs.end() == iter) {
        return NULL;
    }

    // if job was finished , then return NULL
    Job* job = iter->second;
    if (IsJobFinished(job->GetJobStatus())) {
        return NULL;
    }

    return job;
}

// bool JobManager::PopUpEmitQueue(EmitterPlan* plan) {
//     m_emit_waiting_queue.Push(*plan);
// }

bool JobManager::PopJobForEmitter(EmitterPlan* plan,
                                  const uint32_t& waiting_time) {
    if (!m_emit_waiting_queue.Pop(plan, waiting_time)) {
        return false;
    }
    return true;
}

bool JobManager::EmitTaskSucceed(const TaskInfo& task_info) {
    Job* job = GetRunningJob(task_info.job_id());
    if (NULL != job) {
        job->EmitTaskSucceed(task_info);
        return true;
    }
    LOG(WARNING) << "After emit task succeed, the job : "
        << task_info.job_id() << " is not exist ";
    return false;
}

void JobManager::EmitTaskFailed(const TaskInfo& task_info) {
    Job* job = GetRunningJob(task_info.job_id());
    if (NULL != job) {
        GenerateSchedulerPlan(SchedulerPlan::kFailedEmitTaskScheduler,
                              job, task_info.type(), task_info.task_id());
    }
}

bool JobManager::ReportTaskState(const TaskState& task_state) {
    const TaskInfo& task_info = task_state.task_info();
    VLOG(30) << "Report task state " << task_info.ShortDebugString();

    uint64_t job_id = task_info.job_id();
    Job* job = GetRunningJob(job_id);
    if (NULL == job) {
        LOG(WARNING) << "Receive a task " << task_info.ShortDebugString()
            << ", in not exist job : " << job_id;
        return false;
    }

    // if task unrecoverable , stop job
    if (IsTaskUnRecoverable(task_info.task_status())) {
        MarkJobFailed(job, task_info.task_status());
        return false;
    }

    // if task failed , then rescheduler this task again
    if (IsTaskFailed(task_info.task_status())) {
        LOG(ERROR) << "Job get a failed task " << task_info.ShortDebugString()
            << ", and fault tolerance will rescheduler it.";
        GenerateSchedulerPlan(SchedulerPlan::kFailedTaskScheduler,
                              job, task_info.type(), task_info.task_id());
    } else {
        job->UpdateTaskState(task_state);
        if (IsJobFinished(job->GetJobStatus())) {
            AddRemoveJobTimer(job_id);
        }
    }

    return true;
}

void JobManager::MarkJobFailed(Job* job, const TaskStatus& status) {
    switch (status) {
    case kResultManagerOOMTask :
        UpdateJobStatus(job, kJobRunFailed,
                        "Job failed for inter collector receive data out of memory");
        break;
    case kInterResultOOMTask :
        UpdateJobStatus(job, kJobRunFailed,
                        "Job failed for inter task generate data out of memory");
        break;
    case kLeafResultOOMTask :
        UpdateJobStatus(job, kJobRunFailed,
                        "Job failed for leaf task generate data out of memory");
        break;
    case kTaskFailedAfterRetry:
        UpdateJobStatus(job, kJobRunFailed,
                        "Job failed for task failed too much times.");
        break;
    default :
        break;
    }
}

} // namespace master
} // namespace gunir

