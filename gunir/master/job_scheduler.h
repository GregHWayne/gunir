// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_MASTER_JOB_SCHEDULER_H
#define  GUNIR_MASTER_JOB_SCHEDULER_H

#include "toft/base/scoped_ptr.h"
#include "toft/base/uncopyable.h"
#include "toft/system/threading/base_thread.h"

#include "gunir/master/scheduler.h"
#include "gunir/master/scheduler_plan.pb.h"

namespace gunir {
namespace master {

class JobManager;
class Scheduler;
class ServerManager;

class JobScheduler : public toft::BaseThread {
    DECLARE_UNCOPYABLE(JobScheduler);

public:
    JobScheduler(JobManager* job_manager,
                 ServerManager* server_manager);
    virtual ~JobScheduler();

protected:
    void Entry();
    void SchedulerJob(SchedulerPlan* plan);
    bool SchedulerJobPlan(SchedulerInfo* info, SchedulerPlan* plan);

private:
    JobManager* m_job_manager;
    ServerManager* m_server_manager;
    toft::scoped_ptr<Scheduler> m_scheduler;
};

}  // namespace master
}  // namespace gunir

#endif  // GUNIR_MASTER_JOB_SCHEDULER_H
