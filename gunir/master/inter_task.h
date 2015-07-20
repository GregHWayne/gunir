// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_MASTER_INTER_TASK_H
#define  GUNIR_MASTER_INTER_TASK_H

#include <vector>

#include "gunir/master/task.h"

namespace gunir {
namespace master {

class InterTask : public Task {
public:
    InterTask();
    InterTask(uint64_t job_id, uint32_t task_id);

    ~InterTask();

    void AddChildTaskID(uint32_t task_id);

    void SetTaskInput(const InterTaskInput& input);
    const InterTaskInput& GetTaskInput() const;

    void SetBaseInfo(uint64_t job_id, uint32_t task_id);

    void FillTaskSpec(InterTaskSpec* task_spec);
    void FillTaskSpec(InterTaskSpec* task_spec,
                      const TaskInfo& parent_task_info);

private:
    InterTaskInput m_task_input;
};

}  // namespace master
}  // namespace gunir

#endif  // GUNIR_MASTER_INTER_TASK_H
