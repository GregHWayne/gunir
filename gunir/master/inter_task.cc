// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <vector>

#include "gunir/master/inter_task.h"

namespace gunir {
namespace master {

InterTask::InterTask()
    : Task() {
    m_task_info.set_type(kInterTask);
    m_task_input.set_child_type(kUnknownTask);
}

InterTask::InterTask(uint64_t job_id, uint32_t task_id)
    : Task(kInterTask, job_id, task_id) {
    m_task_input.set_child_type(kUnknownTask);
}

InterTask::~InterTask() {}

void InterTask::SetTaskInput(const InterTaskInput& input) {
    m_task_input.CopyFrom(input);
}

void InterTask::AddChildTaskID(uint32_t task_id) {
    m_task_input.add_child_task_id(task_id);
}

const InterTaskInput& InterTask::GetTaskInput() const {
    return m_task_input;
}

void InterTask::SetBaseInfo(uint64_t job_id, uint32_t task_id) {
    Task::SetBaseInfo(kInterTask, job_id, task_id);
}

void InterTask::FillTaskSpec(InterTaskSpec* task_spec) {
    CHECK_LT(0U, m_task_input.child_task_id_size());

    AddAttemptID();
    task_spec->mutable_task_info()->CopyFrom(m_task_info);
    task_spec->mutable_task_input()->CopyFrom(m_task_input);
}

void InterTask::FillTaskSpec(InterTaskSpec* task_spec,
                             const TaskInfo& parent_task_info) {
    CHECK_EQ(parent_task_info.task_id(), m_parent_task_id);
    CHECK_EQ(parent_task_info.job_id(), m_task_info.job_id());
    CHECK_EQ(kInterTask, parent_task_info.type());

    FillTaskSpec(task_spec);
    task_spec->mutable_parent_task_info()->CopyFrom(parent_task_info);
}

}  // namespace master
}  // namespace gunir
