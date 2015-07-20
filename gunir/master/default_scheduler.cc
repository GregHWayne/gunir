// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/master/default_scheduler.h"

namespace gunir {
namespace master {
DefaultScheduler::DefaultScheduler() {}

DefaultScheduler::~DefaultScheduler() {}

void DefaultScheduler::InitInterTask(SchedulerInfo* info) {
    while (!m_queue.empty()) {
        m_queue.pop();
    }

    for (int32_t i = 0; i < info->inter_server_size(); ++i) {
        m_queue.push(info->mutable_inter_server(i));
    }
}

void DefaultScheduler::InitLeafTask(SchedulerInfo* info) {
    while (!m_queue.empty()) {
        m_queue.pop();
    }

    for (int32_t i = 0; i < info->leaf_server_size(); ++i) {
        m_queue.push(info->mutable_leaf_server(i));
    }
}

void DefaultScheduler::SchedulerTask(TaskServerPair* pair) {
    ServerSchedulerInfo* info = m_queue.top();
    SchedulerTaskWithServer(pair, info);
    m_queue.pop();
    m_queue.push(info);
}

void DefaultScheduler::SchedulerInterTask(SchedulerInfo* info,
                                          SchedulerPlan* plan) {
    InitInterTask(info);

    for (int32_t i = 0; i < plan->inter_pair_size(); ++i) {
        SchedulerTask(plan->mutable_inter_pair(i));
        VLOG(30) << " Scheduler inter task : "
            << plan->inter_pair(i).task_id()
            << ", with inter server : " << plan->inter_pair(i).server_id();
    }
}

void DefaultScheduler::SchedulerLeafTask(SchedulerInfo* info,
                                         SchedulerPlan* plan) {
    InitLeafTask(info);

    for (int32_t i = 0; i < plan->leaf_pair_size(); ++i) {
        SchedulerTask(plan->mutable_leaf_pair(i));
        VLOG(30) << " Scheduler leaf task : "
            << plan->leaf_pair(i).task_id()
            << ", with leaf server : " << plan->leaf_pair(i).server_id();
    }
}

}  // namespace master
}  // namespace gunir
