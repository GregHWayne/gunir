// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/master/job_scheduler.h"

#include "gunir/master/default_scheduler.h"
#include "gunir/master/job_manager.h"
#include "gunir/master/server_manager.h"
#include "gunir/master/server_state.h"

DECLARE_int64(gunir_blocking_queue_time);

namespace gunir {
namespace master {

JobScheduler::JobScheduler(JobManager* job_manager,
                           ServerManager* server_manager)
    : m_job_manager(job_manager),
      m_server_manager(server_manager) {
    m_scheduler.reset(new DefaultScheduler());
}

JobScheduler::~JobScheduler() {
    VLOG(10) << "Job scheduler thread start exit now ";

    SendStopRequest();
    if (IsJoinable()) {
        Join();
    } else {
        VLOG(20) << "thread is not started";
    }

    VLOG(10) << "Job scheduler thread exit succeed ";
}

void JobScheduler::Entry() {
    while (!IsStopRequested()) {
        SchedulerPlan plan;
        if (m_job_manager->PopJobForScheduler(&plan, FLAGS_gunir_blocking_queue_time)) {
            SchedulerJob(&plan);
        }
    }
}

void JobScheduler::SchedulerJob(SchedulerPlan* plan) {
    // start scheduler job
    VLOG(10) << "Start Scheduler Job : " << plan->job_id();

    SchedulerInfo scheduler_info;
    m_server_manager->GetSchedulerServerInfo(&scheduler_info);
    if (!SchedulerJobPlan(&scheduler_info, plan)) {
        LOG(ERROR) << "Scheduler job :" << plan->job_id() << " failed";
        m_job_manager->SchedulerJobFailed(plan->job_id());
        return;
    }

    if (!m_job_manager->FinishedSchedulerPlan(*plan)) {
        LOG(ERROR) << "Finished SchedulerPlan error, release reserved servers";
    }

    VLOG(10) << "Finish scheduler job : " << plan->job_id();
}

bool JobScheduler::SchedulerJobPlan(SchedulerInfo* info, SchedulerPlan* plan) {
    uint32_t inter_task_number = plan->inter_pair_size();
    uint32_t leaf_task_number = plan->leaf_pair_size();
    if (inter_task_number > info->total_inter_slot()) {
        LOG(ERROR) << "Not enought inter server slot for this job :"
            << inter_task_number << " > "
            << info->total_inter_slot();
        return false;
    }

    if (leaf_task_number > info->total_leaf_slot()) {
        LOG(ERROR) << "Not enought leaf server slot for this job :"
            << leaf_task_number << " < "
            << info->total_leaf_slot();
        return false;
    }

    m_scheduler->SchedulerInterTask(info, plan);
    m_scheduler->SchedulerLeafTask(info, plan);
    return true;
}

}  // namespace master
}  // namespace gunir
