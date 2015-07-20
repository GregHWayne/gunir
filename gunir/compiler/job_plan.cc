// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com),
//
//
#include "gunir/compiler/job_plan.h"

#include "thirdparty/gflags/gflags.h"
#include "gunir/compiler/parallel_planner.h"
#include "gunir/compiler/simple_planner.h"

DECLARE_string(gunir_planner_mode);

namespace gunir {
namespace compiler {

void JobPlan::Init(const SelectQuery& query) {
    if (FLAGS_gunir_planner_mode == "parallel") {
        m_planner.reset(new ParallelPlanner(query));
    } else {
        m_planner.reset(new SimplePlanner(query));
    }
}

TaskPlanProto JobPlan::GetTaskPlan() {
    TaskPlanProto task_plan_proto;
    bool is_succeed = m_planner->GenerateExecutePlan(&task_plan_proto);
    CHECK(is_succeed);
    return task_plan_proto;
}

} // namespace compiler
} // namespace gunir
