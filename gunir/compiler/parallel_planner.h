// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_PARALLEL_PLANNER_H
#define  GUNIR_COMPILER_PARALLEL_PLANNER_H

#include <map>
#include <string>
#include <vector>

#include "gunir/compiler/base_plan_scheduler.h"
#include "gunir/compiler/parser/plan.pb.h"
#include "gunir/compiler/plan.h"
#include "gunir/compiler/planner.h"
#include "gunir/compiler/select_query.h"

namespace gunir {
namespace compiler {

class ParallelPlanner : public Planner {
private:
    typedef std::shared_ptr<TableEntry> ShrEntry;
    typedef std::shared_ptr<Expression> ShrExpr;
    typedef std::shared_ptr<Target> ShrTarget;

public:
    explicit ParallelPlanner(const SelectQuery& query);

    bool GenerateExecutePlan(
        TaskPlanProto* task_plan_proto);

private:
    TaskPlanProto* GetParallelPlan();
    TaskPlanProto* GetPlanForNode(const PlanNode& node);

    Plan* GetExecPlanForRootInterServer(
        TaskPlanProto* task_plan_proto, size_t num_subplan);
    Plan* GetExecPlanForInterServer(
        TaskPlanProto* task_plan_proto, size_t num_subplan);
    Plan* GetExecPlanForLeafServer(
        TaskPlanProto* task_plan_proto);

    Plan* GetFinalPlan(Plan* subplan);
    Plan* GetMergeAggregatePlan(bool is_root, int num_subplan);
    Plan* GetMergeSortPlan(size_t num_subplan);
    Plan* GetUnionPlan(size_t num_subplan);
    Plan* GetDistinctPlan();

    Plan* GetLocalAggregatePlan();
    Plan* GetOrderbyPlan();
    Plan* GetProjectionPlan();
    Plan* GetHavingPlan();
    Plan* GetFilterPlan();
    Plan* GetWhereFilterPlan(const std::shared_ptr<TableEntry>& entry,
                             const TabletInfo& tablet_file,
                             bool has_where);
    Plan* GetScanPlan(const std::shared_ptr<TableEntry>& entry,
                      const TabletInfo& tablet_file);
    Plan* GetJoinPlan();

    std::vector<ColumnInfo> GetColumnsInResultSchema();
    std::vector<ColumnInfo> GetColumnsInTransSchema();

    std::vector<uint64_t> GetAffectIds(const std::vector<ColumnInfo>& colume);

    std::vector<BQType> GetTupleTypesOfColumns(
        const std::vector<ColumnInfo>& column_infos);
    std::vector<OrderByColumn> GetOrderByColumnsFromGroupBy();
    std::vector<OrderByColumn> GetOrderByColumnsFromDistinct();
    std::vector<OrderByColumn> GetOrderByColumnsFromDisAgg();
    std::vector<BQType> GetSubPlanTupleTypes();
    void GetFinalSchemaColumnInfo(
        const std::string& table_schema,
        const std::string& message_name,
        std::vector<ColumnInfo>* column_infos);

    Plan* AddLimitPlan(Plan* plan);

    bool HasWithinAgg() {
        return (m_query.GetAggregateType() == SelectQuery::kWithinAgg);
    }

    bool HasGroupByAgg() {
        return (m_query.GetAggregateType() == SelectQuery::kGroupByAgg);
    }

    bool HasSort() {
        return (m_query.GetOrderByColumns().size() > 0);
    }

    bool HasDistinct() {
        return (m_query.GetDistinctColumns().size() > 0);
    }

    bool HasHaving() {
        return (m_query.GetHaving() != NULL);
    }

private:
    TaskPlanProto m_task_plan_proto;
    toft::scoped_ptr<BasePlanScheduler> m_plan_scheduler;

    ShrEntry m_entry;
    std::vector<TabletInfo> m_tablet_files;
    size_t m_tablet_to_assign;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_PARALLEL_PLANNER_H

