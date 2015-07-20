// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_SIMPLE_PLANNER_H
#define  GUNIR_COMPILER_SIMPLE_PLANNER_H

#include <vector>

#include "gunir/compiler/parser/plan.pb.h"
#include "gunir/compiler/plan.h"
#include "gunir/compiler/planner.h"
#include "gunir/compiler/select_query.h"

namespace gunir {
namespace compiler {

class SimplePlanner : public Planner {
public:
    explicit SimplePlanner(const SelectQuery& query);

    bool GenerateExecutePlan(
        TaskPlanProto* task_plan_proto);

private:
    typedef std::shared_ptr<Expression> ShrExpr;

private:
    Plan* GenerateLimitPlan();

    Plan* GenerateDistinctPlan(Plan* subplan);

    Plan* GenerateOrderbyPlan();

    Plan* GenerateProjectedPlan();

    Plan* GenerateSingleTableFilterPlan(
        const std::shared_ptr<TableEntry>& entry,
        bool has_where);

    Plan* GenerateJoinPlan();

    Plan* GenerateScanPlan(
        const std::shared_ptr<TableEntry>& entry,
        const std::vector<AffectedColumnInfo>& affected_column_infos,
        const std::vector<uint64_t>& affect_ids);


    Plan* GenerateAggregatePlan(Plan* subplan);

    std::vector<OrderByColumn> GetOrderByColumnsFromDisAgg();
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_SIMPLE_PLANNER_H

