// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/master/task.h"

#include "gunir/types.h"

namespace gunir {
namespace master {

Task::Task()
    : m_cur_attempt_id(0),
      m_parent_task_id(kUnknownId) {
    DefaultTaskInfo();
}

Task::Task(TaskType type, uint64_t job_id, uint32_t task_id)
    : m_cur_attempt_id(0),
      m_parent_task_id(kUnknownId) {
    SetBaseInfo(type, job_id, task_id);

    m_task_info.set_server_id(kUnknownId);
    m_task_info.set_server_addr("");
    m_task_info.set_task_status(kUnknownStatus);
}

Task::~Task() {}

void Task::SetBaseInfo(TaskType type, uint64_t job_id, uint32_t task_id) {
    m_task_info.set_type(type);
    m_task_info.set_job_id(job_id);
    m_task_info.set_task_id(task_id);
    m_task_info.set_attempt_id(m_cur_attempt_id);
}

void Task::DefaultTaskInfo() {
    m_task_info.set_type(kUnknownTask);
    m_task_info.set_task_id(kUnknownId);
    m_task_info.set_attempt_id(kUnknownId);
    m_task_info.set_job_id(kUnknownId);
    m_task_info.set_server_id(kUnknownId);
    m_task_info.set_server_addr("");
    m_task_info.set_task_status(kUnknownStatus);
}

}  // namespace master
}  // namespace gunir
