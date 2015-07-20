// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/compiler/compiler_utils.h"

namespace gunir {
TaskType GetTaskTypeByPlan(const compiler::TaskPlanProto& plan) {
    if (plan.sub_task_plan_list_size() > 0) {
        return kInterTask;
    }
    return kLeafTask;
}

TaskType GetChildTaskTypeByPlan(const compiler::TaskPlanProto& plan) {
    return GetTaskTypeByPlan(plan.sub_task_plan_list(0));
}

void GetTaskNumberByPlan(const compiler::TaskPlanProto& plan,
                         uint32_t* inter_server_num,
                         uint32_t* leaf_server_num) {
    if (kInterTask == GetChildTaskTypeByPlan(plan)) {
        *inter_server_num += plan.sub_task_plan_list_size();
        for (int32_t i = 0; i < plan.sub_task_plan_list_size(); ++i) {
            GetTaskNumberByPlan(plan.sub_task_plan_list(i),
                                inter_server_num,
                                leaf_server_num);
        }
    } else {
        *leaf_server_num += plan.sub_task_plan_list_size();
    }
}

void GetTabletFileAndColumnsFromPlan(
        const compiler::PlanProto& plan,
        std::vector<TabletInfo>* tablet_list,
        std::vector<std::vector<std::string> >* column_list) {
    TabletInfo info;
    std::vector<std::string> new_column;
    switch (plan.type()) {
    case compiler::PlanProto::kScan :
        info.CopyFrom(plan.scan().tablet());
        for (int i = 0; i < plan.scan().affect_column_list_size(); ++i) {
            new_column.push_back(plan.scan().affect_column_list(i).name());
        }
        tablet_list->push_back(info);
        column_list->push_back(new_column);
        break;
    case compiler::PlanProto::kFilter :
        GetTabletFileAndColumnsFromPlan(
            plan.filter().subplan(), tablet_list, column_list);
        break;

    case compiler::PlanProto::kSort :
        GetTabletFileAndColumnsFromPlan(
            plan.sort().subplan(), tablet_list, column_list);
        break;

    case compiler::PlanProto::kUniq :
        GetTabletFileAndColumnsFromPlan(
            plan.uniq().subplan(), tablet_list, column_list);
        break;

    case compiler::PlanProto::kWithin :
        GetTabletFileAndColumnsFromPlan(
            plan.within().subplan(), tablet_list, column_list);
        break;

    case compiler::PlanProto::kAggregate :
        GetTabletFileAndColumnsFromPlan(
            plan.aggregate().subplan(), tablet_list, column_list);
        break;

    case compiler::PlanProto::kProjection :
        GetTabletFileAndColumnsFromPlan(
            plan.projection().subplan(), tablet_list, column_list);
        break;

    case compiler::PlanProto::kLimit :
        GetTabletFileAndColumnsFromPlan(
            plan.limit().subplan(), tablet_list, column_list);
        break;
    case compiler::PlanProto::kJoin :
        GetTabletFileAndColumnsFromPlan(plan.join().left_table().subplan(),
                                        tablet_list, column_list);
        GetTabletFileAndColumnsFromPlan(plan.join().right_table().subplan(),
                                        tablet_list, column_list);
        break;
    default :
        break;
    }
}

} // namespace gunir
