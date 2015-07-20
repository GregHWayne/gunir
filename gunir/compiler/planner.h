// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_PLANNER_H
#define  GUNIR_COMPILER_PLANNER_H

#include <vector>

#include "gunir/compiler/parser/plan.pb.h"
#include "gunir/compiler/plan.h"
#include "gunir/compiler/select_query.h"

namespace gunir {
namespace compiler {

class Planner {
public:
    explicit Planner(const SelectQuery& query) : m_query(query) {
    }
    virtual ~Planner() {}

    virtual bool GenerateExecutePlan(
        TaskPlanProto* task_plan_proto) = 0;

protected:
    SelectQuery m_query;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_PLANNER_H

