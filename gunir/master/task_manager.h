// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_MASTER_TASK_MANAGER_H
#define  GUNIR_MASTER_TASK_MANAGER_H

#include "toft/base/scoped_ptr.h"

#include "gunir/proto/task.pb.h"
#include "gunir/master/scheduler_plan.pb.h"

namespace gunir {
class BitMap;

namespace master {
class InterTask;
class LeafTask;
class Task;

class TaskManager {
public:
    TaskManager(uint64_t job_id,
                uint32_t leaf_task_num,
                uint32_t inter_task_num);
    ~TaskManager();

public:
    uint32_t AddInterTask(const InterTaskInput& input, uint32_t parent_index);
    InterTask* GetInterTask(uint32_t task_index);
    uint32_t GetCurInterTaskNumber() const;

    uint32_t AddLeafTask(const LeafTaskInput& input, uint32_t parent_index);
    LeafTask* GetLeafTask(uint32_t task_index);
    uint32_t GetCurLeafTaskNumber() const;

    void UpdateTaskState(const TaskState& task_state);

    void GenerateSchedulerPlan(SchedulerPlan* plan,
                               const TaskType& type,
                               const uint32_t& task_id);
    void GenerateEmitterPlan(const SchedulerPlan& scheduler_plan,
                             EmitterPlan* emitter_plan);

    void EmitTaskSucceed(const TaskInfo& task_info);
    bool IsAllTaskEmitSucceed() const;

private:
    void SchedulerLeafTask(SchedulerPlan* plan, uint32_t task_id);
    void SchedulerInterTask(SchedulerPlan* plan, uint32_t task_id,
                            uint32_t level);
    void AddLeafNode(LeafNode* node, uint32_t task_id);
    void AddInterNode(InterNode* node, uint32_t task_id,
                      uint32_t level);

    uint32_t GenerateSchedulerLevel(SchedulerPlan* plan);
    uint32_t GenerateEmitterLevel(const SchedulerPlan& plan);

    TaskInfo GetParentTaskInfo(Task* task) const;
    void FillSchedulerInfo(const SchedulerPlan& plan);

private:
    uint64_t m_job_id;
    uint32_t m_inter_task_number;
    uint32_t m_leaf_task_number;

    uint32_t m_cur_inter_task_index;
    uint32_t m_cur_leaf_task_index;

    toft::scoped_array<LeafTask> m_leaf_task;
    toft::scoped_array<InterTask> m_inter_task;

    toft::scoped_ptr<BitMap> m_emit_succeed_inter_bitmap;
    toft::scoped_ptr<BitMap> m_emit_succeed_leaf_bitmap;
};

}  // namespace master
}  // namespace gunir

#endif  // GUNIR_MASTER_TASK_MANAGER_H
