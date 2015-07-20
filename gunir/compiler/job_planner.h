// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_JOB_PLANNER_H
#define  GUNIR_COMPILER_JOB_PLANNER_H

#include <vector>

#include "gunir/compiler/parser/plan.pb.h"
#include "gunir/compiler/select_query.h"

namespace gunir {
namespace io {
class Scanner;
}  // namespace io

namespace compiler {
class SelectQuery;

class JobPlan {
public:
    bool Init(const SelectQuery& query);

    TaskPlanProto GetTaskPlan();
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_JOB_PLANNER_H
