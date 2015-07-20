// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <cstdio>

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/compiler/base_plan_scheduler.h"
#include "gunir/compiler/simple_plan_scheduler.h"

DECLARE_int32(gunir_compiler_leaf_plan_limit);
DECLARE_int32(gunir_compiler_inter_plan_limit);

namespace gunir {
namespace compiler {

void GetNumber(const PlanNode* root,
               uint32_t* inter_task_number,
               uint32_t* leaf_task_number) {
    if (root != NULL) {
        if (root->m_type == PlanNode::kRootInterServerNode ||
            root->m_type == PlanNode::kMiddleInterServerNode) {
            (*inter_task_number)++;
        } else {
            (*leaf_task_number)++;
        }
        for (size_t i = 0; i < root->m_sub_node_list.size(); i++) {
            GetNumber(&root->m_sub_node_list[i],
                      inter_task_number,
                      leaf_task_number);
        }
    }
}

TEST(SimplePlanSchedulerTest, test) {

    FLAGS_gunir_compiler_leaf_plan_limit = 2;
    FLAGS_gunir_compiler_inter_plan_limit = 2;

    uint32_t inter_task_number = 0;
    uint32_t leaf_task_number = 0;

    BasePlanScheduler* scheduler = new SimplePlanScheduler();
    PlanNode* root = scheduler->GetPlanStrategy(5);

    GetNumber(root, &inter_task_number, &leaf_task_number);

    EXPECT_EQ(inter_task_number, 6U);
    EXPECT_EQ(leaf_task_number, 5U);

    delete root;
    delete scheduler;
}

} // namespace compiler
} // namespace gunir

