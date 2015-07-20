// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_MASTER_SCHEDULER_H
#define  GUNIR_MASTER_SCHEDULER_H

#include "gunir/master/scheduler_plan.pb.h"

namespace gunir {
namespace master {

class Scheduler {
protected:
    Scheduler() {}

public:
    struct LessCmp {
        bool operator() (ServerSchedulerInfo* lhs, ServerSchedulerInfo* rhs) {
            return lhs->left_slot_number() < rhs->left_slot_number();
        }
    };

public:
    virtual ~Scheduler() {}

    virtual void SchedulerLeafTask(SchedulerInfo* info, SchedulerPlan* plan) = 0;
    virtual void SchedulerInterTask(SchedulerInfo* info, SchedulerPlan* plan) = 0;

    virtual void SchedulerTaskWithServer(TaskServerPair* pair,
                                         ServerSchedulerInfo* info) {
        pair->set_server_id(info->id());
        info->set_left_slot_number(info->left_slot_number() - 1);
        info->set_reserve_slot_number(info->reserve_slot_number() + 1);
        pair->set_server_addr(info->server_addr());
    }
};

}  // namespace master
}  // namespace gunir

#endif  // GUNIR_MASTER_SCHEDULER_H
