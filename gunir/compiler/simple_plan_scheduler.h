// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_COMPILER_SIMPLE_PLAN_SCHEDULER_H
#define  GUNIR_COMPILER_SIMPLE_PLAN_SCHEDULER_H

#include <vector>

#include "gunir/compiler/base_plan_scheduler.h"

namespace gunir {
namespace compiler {

class SimplePlanScheduler : public BasePlanScheduler {
public:
    SimplePlanScheduler();
    ~SimplePlanScheduler();

public:
    PlanNode* GetPlanStrategy(int32_t num_leaf_task);

private:
    void MergeNodes(const std::vector<PlanNode>& from_list,
                    std::vector<PlanNode>* to_list,
                    int32_t limit);
};

}  // namespace compiler
}  // namespace gunir

#endif  // GUNIR_COMPILER_SIMPLE_PLAN_SCHEDULER_H
