// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_JOB_PLAN_H
#define  GUNIR_COMPILER_JOB_PLAN_H

#include "gunir/compiler/parser/plan.pb.h"
#include "gunir/compiler/planner.h"
#include "gunir/compiler/select_query.h"

namespace gunir {
namespace compiler {

class JobPlan {
public:
    void Init(const SelectQuery& query);
    TaskPlanProto GetTaskPlan();

private:
    toft::scoped_ptr<Planner> m_planner;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_JOB_PLAN_H
