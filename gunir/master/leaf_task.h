// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_MASTER_LEAF_TASK_H
#define  GUNIR_MASTER_LEAF_TASK_H

#include "gunir/master/task.h"

namespace gunir {
namespace master {

class LeafTask : public Task {
public:
    LeafTask();
    LeafTask(uint32_t job_id, uint32_t task_id);

    ~LeafTask();

    void SetTaskInput(const LeafTaskInput& input);

    LeafTaskInput GetTaskInput() const;

    void SetBaseInfo(uint32_t job_id, uint32_t task_id);

    void FillTaskSpec(LeafTaskSpec* task_spec,
                      const TaskInfo& parent_task_info);

private:
    LeafTaskInput m_task_input;
};

}  // namespace master
}  // namespace gunir

#endif  // GUNIR_MASTER_LEAF_TASK_H
