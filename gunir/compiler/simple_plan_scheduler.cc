// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include <algorithm>
#include <vector>

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "gunir/compiler/simple_plan_scheduler.h"

DECLARE_int32(gunir_compiler_inter_plan_limit);
DECLARE_int32(gunir_compiler_leaf_plan_limit);

namespace gunir {
namespace compiler {

SimplePlanScheduler::SimplePlanScheduler()
    : BasePlanScheduler() {
}

SimplePlanScheduler::~SimplePlanScheduler() {
}

PlanNode* SimplePlanScheduler::GetPlanStrategy(int32_t num_leaf_task) {

    CHECK_GT(FLAGS_gunir_compiler_inter_plan_limit, 1);
    CHECK_GT(FLAGS_gunir_compiler_leaf_plan_limit, 0);

    if (num_leaf_task <= 0) {
        return NULL;
    }

    std::vector<PlanNode> task_list[2];
    int32_t cur_index = 0;
    int32_t next_index = 1 - cur_index;

    // generate  task_list;  using rolling array
    PlanNode node;
    node.m_type = PlanNode::kLeafServerNode;

    for (int32_t i = 0; i < num_leaf_task; i++) {
        task_list[cur_index].push_back(node);
    }

    MergeNodes(task_list[cur_index], &task_list[next_index],
               FLAGS_gunir_compiler_leaf_plan_limit);

    task_list[cur_index].clear();
    std::swap(cur_index, next_index);

    while (task_list[cur_index].size() > 1) {
        MergeNodes(task_list[cur_index], &task_list[next_index],
                   FLAGS_gunir_compiler_inter_plan_limit);
        task_list[cur_index].clear();
        std::swap(cur_index, next_index);
    }

    PlanNode *root_node = new PlanNode;
    if (task_list[cur_index].size() != 1) {
        return NULL;
    }
    *root_node = task_list[cur_index][0];

    root_node->m_type = PlanNode::kRootInterServerNode;
    return root_node;
}

void SimplePlanScheduler::MergeNodes(const std::vector<PlanNode>& from_list,
                                     std::vector<PlanNode>* to_list,
                                     int32_t limit) {
    PlanNode temp_node;
    temp_node.m_type = PlanNode::kMiddleInterServerNode;
    int32_t size = 0;
    for (size_t i = 0 ; i < from_list.size(); i++) {
        size++;
        temp_node.m_sub_node_list.push_back(from_list[i]);
        if (size >= limit) {
            size = 0;
            to_list->push_back(temp_node);
            temp_node.m_sub_node_list.clear();
        }
    }

    if (size > 0) {
        to_list->push_back(temp_node);
    }
}

}  // namespace compiler
}  // namespace gunir
