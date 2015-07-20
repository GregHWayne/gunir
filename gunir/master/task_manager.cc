// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/master/task_manager.h"

#include "gunir/master/inter_task.h"
#include "gunir/master/leaf_task.h"
#include "gunir/types.h"
#include "gunir/utils/bit_map.h"

namespace gunir {
namespace master {

TaskManager::TaskManager(uint64_t job_id,
                         uint32_t inter_task_num,
                         uint32_t leaf_task_num)
    : m_job_id(job_id),
      m_inter_task_number(inter_task_num),
      m_leaf_task_number(leaf_task_num),
      m_cur_inter_task_index(0),
      m_cur_leaf_task_index(0),
      m_leaf_task(new LeafTask[m_leaf_task_number]),
      m_inter_task(new InterTask[m_inter_task_number]) {
    m_emit_succeed_inter_bitmap.reset(new BitMap(m_inter_task_number));
    m_emit_succeed_leaf_bitmap.reset(new BitMap(m_leaf_task_number));
    m_emit_succeed_inter_bitmap->Clear();
    m_emit_succeed_leaf_bitmap->Clear();
}

TaskManager::~TaskManager() {
}

uint32_t TaskManager::AddInterTask(const InterTaskInput& task_input,
                                   uint32_t parent_index) {
    CHECK_LT(m_cur_inter_task_index, m_inter_task_number);

    InterTask* task = &m_inter_task[m_cur_inter_task_index];
    task->SetBaseInfo(m_job_id, m_cur_inter_task_index);
    task->SetTaskInput(task_input);

    if (0 != m_cur_inter_task_index) {
        CHECK_LT(parent_index, m_cur_inter_task_index);

        CHECK_EQ(kInterTask,
                 m_inter_task[parent_index].GetTaskInput().child_type())
            << "Some thing error in task_manager";
        task->SetParentTaskID(parent_index);
        m_inter_task[parent_index].AddChildTaskID(m_cur_inter_task_index);
    }

    VLOG(30) << "Add Inter Task : " << task->GetTaskInfo().ShortDebugString();

    return m_cur_inter_task_index++;
}

InterTask* TaskManager::GetInterTask(uint32_t task_index) {
    CHECK_GT(m_cur_inter_task_index, task_index);
    return &m_inter_task[task_index];
}

uint32_t TaskManager::GetCurInterTaskNumber() const {
    return m_cur_inter_task_index;
}

uint32_t TaskManager::AddLeafTask(const LeafTaskInput& task_input,
                                  uint32_t parent_index) {
    CHECK_LT(m_cur_leaf_task_index, m_leaf_task_number);

    LeafTask* task = &m_leaf_task[m_cur_leaf_task_index];
    task->SetBaseInfo(m_job_id, m_cur_leaf_task_index);
    task->SetTaskInput(task_input);

    CHECK_EQ(kLeafTask,
             m_inter_task[parent_index].GetTaskInput().child_type())
        << "Some thing error in task_manager";

    CHECK_LT(parent_index, m_cur_inter_task_index);
    task->SetParentTaskID(parent_index);
    m_inter_task[parent_index].AddChildTaskID(m_cur_leaf_task_index);

    VLOG(30) << "Add Leaf Task : " << task->GetTaskInfo().ShortDebugString();

    return m_cur_leaf_task_index++;
}

LeafTask* TaskManager::GetLeafTask(uint32_t task_index) {
    CHECK_GT(m_cur_leaf_task_index, task_index);
    return &m_leaf_task[task_index];
}

uint32_t TaskManager::GetCurLeafTaskNumber() const {
    return m_cur_leaf_task_index;
}

void TaskManager::UpdateTaskState(const TaskState& task_state) {
    const TaskInfo& task_info = task_state.task_info();
    if (kInterTask == task_info.type()) {
        GetInterTask(task_info.task_id())->
            SetTaskStatus(task_info.task_status());
    } else if (kLeafTask == task_info.type()) {
        GetLeafTask(task_info.task_id())->
            SetTaskStatus(task_info.task_status());
    } else {
        LOG(FATAL) << "Something wrong in program , task_info:"
            << task_info.ShortDebugString();
    }
}

uint32_t TaskManager::GenerateSchedulerLevel(SchedulerPlan* plan) {
    switch (plan->type()) {
    case SchedulerPlan::kFullJobScheduler:
        return kMaxLevel;
    case SchedulerPlan::kFailedTaskScheduler:
        return kMaxLevel;
    default :
        return 0;
    }
}

uint32_t TaskManager::GenerateEmitterLevel(const SchedulerPlan& plan) {
    switch (plan.type()) {
    case SchedulerPlan::kFullJobScheduler:
        return kMaxLevel;
    case SchedulerPlan::kFailedTaskScheduler:
        return kMaxLevel;
    default :
        CHECK_EQ(plan.type(), SchedulerPlan::kFailedEmitTaskScheduler);
        return kMaxLevel;
    }
}

void TaskManager::GenerateSchedulerPlan(SchedulerPlan* plan,
                                        const TaskType& type,
                                        const uint32_t& task_id) {
    CHECK_EQ(m_cur_leaf_task_index, m_leaf_task_number);
    CHECK_EQ(m_cur_inter_task_index, m_inter_task_number);

    plan->set_job_id(m_job_id);

    if (kLeafTask == type) {
        SchedulerLeafTask(plan, task_id);
    } else if (kInterTask == type) {
        uint32_t level = GenerateSchedulerLevel(plan);
        SchedulerInterTask(plan, task_id, level);
    }
}

void TaskManager::SchedulerLeafTask(SchedulerPlan* plan,
                                    uint32_t task_id) {
    TaskServerPair* pair = plan->add_leaf_pair();
    pair->set_task_id(task_id);
    pair->set_server_id(kUnknownId);
    pair->set_server_addr(kUnknownAddr);

    const LeafTaskInput& input = GetLeafTask(task_id)->GetTaskInput();
    for (int32_t i = 0; i < input.scanner_input(0).tablet().ip_size(); ++i) {
        pair->add_task_ip(input.scanner_input(0).tablet().ip(i));
    }
}

void TaskManager::SchedulerInterTask(SchedulerPlan* plan,
                                     uint32_t task_id,
                                     uint32_t level) {
    TaskServerPair* pair = plan->add_inter_pair();
    pair->set_task_id(task_id);
    pair->set_server_id(kUnknownId);
    pair->set_server_addr(kUnknownAddr);

    if (0 == level) {
        return;
    }

    const InterTaskInput& input = GetInterTask(task_id)->GetTaskInput();
    const TaskType& type = input.child_type();
    if (type == kLeafTask) {
        for (int32_t i = 0; i < input.child_task_id_size(); ++i) {
            SchedulerLeafTask(plan, input.child_task_id(i));
        }
    } else {
        for (int32_t i = 0; i < input.child_task_id_size(); ++i) {
            SchedulerInterTask(plan, input.child_task_id(i), level - 1);
        }
    }
}

void TaskManager::GenerateEmitterPlan(const SchedulerPlan& scheduler_plan,
                                      EmitterPlan* emitter_plan) {
    FillSchedulerInfo(scheduler_plan);

    emitter_plan->set_job_id(m_job_id);

    TaskTreeNode* node = emitter_plan->mutable_root_node();
    if (scheduler_plan.inter_pair_size() > 0) {
        // if task has no cache,
        // then the child task tree should be emit, or else only
        // 1 level child level should be emitted
        uint32_t level = GenerateEmitterLevel(scheduler_plan);
        AddInterNode(node->mutable_inter(),
                     scheduler_plan.inter_pair(0).task_id(),
                     level);
    } else {
        CHECK_EQ(1, scheduler_plan.leaf_pair_size());
        AddLeafNode(node->mutable_leaf(),
                    scheduler_plan.leaf_pair(0).task_id());
    }
}

void TaskManager::FillSchedulerInfo(const SchedulerPlan& plan) {
    CHECK_EQ(plan.job_id(), m_job_id);

    for (int32_t i = 0; i < plan.inter_pair_size(); ++i) {
        const TaskServerPair& pair = plan.inter_pair(i);
        CHECK_NE(kUnknownId, pair.server_id());
        m_inter_task[pair.task_id()].SetServerID(pair.server_id());
        m_inter_task[pair.task_id()].SetServerAddr(pair.server_addr());
    }

    for (int32_t i = 0; i < plan.leaf_pair_size(); ++i) {
        const TaskServerPair& pair = plan.leaf_pair(i);
        CHECK_NE(kUnknownId, pair.server_id());
        m_leaf_task[pair.task_id()].SetServerID(pair.server_id());
        m_leaf_task[pair.task_id()].SetServerAddr(pair.server_addr());
    }
}

void TaskManager::AddInterNode(InterNode* node,
                               uint32_t task_id,
                               uint32_t level) {
    InterTask* task = &m_inter_task[task_id];

    InterTaskSpec* spec = node->mutable_task_spec();
    if (0 == task_id) {
        task->FillTaskSpec(spec);
    } else {
        task->FillTaskSpec(spec, GetParentTaskInfo(task));
    }

    if (0 == level) {
        return;
    }

    TaskType child_type = task->GetTaskInput().child_type();
    for (int32_t i = 0; i < spec->task_input().child_task_id_size(); ++i) {
        TaskTreeNode* child_node = node->add_child();

        if (kInterTask == child_type) {
            InterNode* inter_node = child_node->mutable_inter();
            AddInterNode(inter_node,
                         spec->task_input().child_task_id(i),
                         level - 1);
        } else {
            LeafNode* leaf_node = child_node->mutable_leaf();
            AddLeafNode(leaf_node, spec->task_input().child_task_id(i));
        }
    }
}

void TaskManager::AddLeafNode(LeafNode* node, uint32_t task_id) {
    LeafTask* task = &m_leaf_task[task_id];

    LeafTaskSpec* spec = node->mutable_task_spec();
    task->FillTaskSpec(spec, GetParentTaskInfo(task));
}

TaskInfo TaskManager::GetParentTaskInfo(Task* task) const {
    uint32_t parent_task_id = task->GetParentTaskID();

    CHECK_GT(m_inter_task_number, parent_task_id);
    InterTask* parent_task = &m_inter_task[parent_task_id];

    CHECK_EQ(task->GetTaskInfo().type(),
             parent_task->GetTaskInput().child_type());
    return parent_task->GetTaskInfo();
}

void TaskManager::EmitTaskSucceed(const TaskInfo& task_info) {
    if (kInterTask == task_info.type()) {
        m_emit_succeed_inter_bitmap->Set(task_info.task_id());
    } else {
        m_emit_succeed_leaf_bitmap->Set(task_info.task_id());
    }
}

bool TaskManager::IsAllTaskEmitSucceed() const {
    return m_emit_succeed_inter_bitmap->GetCount() == m_inter_task_number &&
        m_emit_succeed_leaf_bitmap->GetCount() == m_leaf_task_number;
}

}  // namespace master
}  // namespace gunir
