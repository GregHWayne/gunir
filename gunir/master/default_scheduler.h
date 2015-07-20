// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_MASTER_DEFAULT_SCHEDULER_H
#define  GUNIR_MASTER_DEFAULT_SCHEDULER_H

#include <queue>
#include <vector>

#include "gunir/master/scheduler.h"

namespace gunir {
namespace master {

class DefaultScheduler : public Scheduler {
public:
    DefaultScheduler();
    virtual ~DefaultScheduler();

    virtual void SchedulerLeafTask(SchedulerInfo* info, SchedulerPlan* plan);
    virtual void SchedulerInterTask(SchedulerInfo* info, SchedulerPlan* plan);

protected:
    typedef std::priority_queue<ServerSchedulerInfo*,
            std::vector<ServerSchedulerInfo*>, LessCmp> QueueType;

    void InitInterTask(SchedulerInfo* info);
    void InitLeafTask(SchedulerInfo* info);

    void SchedulerTask(TaskServerPair* pair);

protected:
    QueueType m_queue;
};

}  // namespace master
}  // namespace gunir

#endif  // GUNIR_MASTER_DEFAULT_SCHEDULER_H
