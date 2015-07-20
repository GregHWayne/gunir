// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/master/leaf_task.h"

namespace gunir {
namespace master {

LeafTask::LeafTask() : Task() {
    m_task_info.set_type(kLeafTask);
}

LeafTask::LeafTask(uint32_t job_id, uint32_t task_id)
    : Task(kLeafTask, job_id, task_id) {
}

LeafTask::~LeafTask() {
}

void LeafTask::SetTaskInput(const LeafTaskInput& task_input) {
    m_task_input = task_input;
}

LeafTaskInput LeafTask::GetTaskInput() const {
    return m_task_input;
}

void LeafTask::SetBaseInfo(uint32_t job_id, uint32_t task_id) {
    Task::SetBaseInfo(kLeafTask, job_id, task_id);
}

void LeafTask::FillTaskSpec(LeafTaskSpec* task_spec,
                            const TaskInfo& parent_task_info) {
    CHECK_EQ(parent_task_info.task_id(), m_parent_task_id);
    CHECK_EQ(parent_task_info.job_id(), m_task_info.job_id());
    CHECK_EQ(kInterTask, parent_task_info.type());

    AddAttemptID();
    task_spec->mutable_task_info()->CopyFrom(m_task_info);
    task_spec->mutable_parent_task_info()->CopyFrom(parent_task_info);
    task_spec->mutable_task_input()->CopyFrom(m_task_input);
}

}  // namespace master
}  // namespace gunir
