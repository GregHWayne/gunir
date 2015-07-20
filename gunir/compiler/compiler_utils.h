// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef GUNIR_COMPILER_COMPILER_UTILS_H
#define GUNIR_COMPILER_COMPILER_UTILS_H

#include <string>
#include <vector>

#include "gunir/proto/task.pb.h"
#include "gunir/compiler/parser/plan.pb.h"

namespace gunir {

// give a taskplan, get plan's task type
TaskType GetTaskTypeByPlan(const compiler::TaskPlanProto& plan);

// give a taskplan , get the plan's intertask and leaftask number;
void GetTaskNumberByPlan(const compiler::TaskPlanProto& plan,
                         uint32_t* inter_server_num,
                         uint32_t* leaf_server_num);

void GetTabletFileAndColumnsFromPlan(
    const compiler::PlanProto& plan,
    std::vector<TabletInfo>* tablet_info,
    std::vector<std::vector<std::string> >* column_names);

} // namespace gunir
#endif // GUNIR_COMPILER_COMPILER_UTILS_H
