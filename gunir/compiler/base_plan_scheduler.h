// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description: schedule the plans and restruct the plan tree

#ifndef  GUNIR_COMPILER_BASE_PLAN_SCHEDULER_H
#define  GUNIR_COMPILER_BASE_PLAN_SCHEDULER_H

#include <stdint.h>
#include <vector>

namespace gunir {
namespace compiler {

struct PlanNode {
    enum NodeType {
        kRootInterServerNode,
        kMiddleInterServerNode,
        kLeafServerNode
    };
    std::vector<PlanNode> m_sub_node_list;
    NodeType m_type;
};

class BasePlanScheduler {
public:
    BasePlanScheduler() { }
    virtual ~BasePlanScheduler() { }

    virtual PlanNode* GetPlanStrategy(int32_t num_leaf_task) = 0;
};

}  // namespace compiler
}  // namespace gunir

#endif  // GUNIR_COMPILER_BASE_PLAN_SCHEDULER_H
