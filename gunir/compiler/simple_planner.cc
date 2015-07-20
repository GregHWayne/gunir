// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <string>
#include <vector>

#include "gunir/compiler/simple_planner.h"

#include "gunir/compiler/aggregate_plan.h"
#include "gunir/compiler/filter_plan.h"
#include "gunir/compiler/join_plan.h"
#include "gunir/compiler/limit_plan.h"
#include "gunir/compiler/merge_plan.h"
#include "gunir/compiler/projection_plan.h"
#include "gunir/compiler/scan_plan.h"
#include "gunir/compiler/sort_plan.h"
#include "gunir/compiler/uniq_plan.h"
#include "gunir/compiler/within_plan.h"

namespace gunir {
namespace compiler {

SimplePlanner::SimplePlanner(const SelectQuery& query)
    : Planner(query) {
}

bool SimplePlanner::GenerateExecutePlan(TaskPlanProto* task_plan_proto) {
    Plan* plan = GenerateLimitPlan();

    if (plan == NULL) {
        LOG(ERROR) << "Cannot generate plan";
        return false;
    }

    PlanProto* exec_plan = task_plan_proto->mutable_exec_plan();
    plan->CopyToProto(exec_plan);
    task_plan_proto->set_table_schema_string(
        m_query.GetResultSchema());
    task_plan_proto->set_table_message_name(
        SchemaBuilder::kQueryResultMessageName);

    delete plan;
    return true;
}

Plan* SimplePlanner::GenerateLimitPlan() {
    Plan* subplan = GenerateOrderbyPlan();
    if (subplan == NULL)
        return NULL;

    int64_t start, number;
    bool has_limit = m_query.GetLimit(&start, &number);

    if (!has_limit)
        return subplan;

    LimitPlan* plan = new LimitPlan(subplan, start, number);
    return plan;
}

Plan* SimplePlanner::GenerateDistinctPlan(Plan* subplan) {

    if (subplan == NULL) {
        return NULL;
    }

    const std::vector<DistinctColumn>&
        distinct_columns = m_query.GetDistinctColumns();

    std::vector<OrderByColumn> orderby_columns;
    std::vector<size_t> distinct_targets;

    for (size_t i = 0; i < distinct_columns.size(); ++i) {
        distinct_targets.push_back(distinct_columns[i].m_affect_id);
        orderby_columns.push_back(OrderByColumn(distinct_columns[i]));
    }

    SortPlan* sort_plan = new SortPlan(subplan, orderby_columns);
    UniqPlan* plan = new UniqPlan(sort_plan, distinct_targets);
    return plan;
}

std::vector<OrderByColumn> SimplePlanner::GetOrderByColumnsFromDisAgg() {
    const std::vector<DistinctColumn>&
        distinct_columns = m_query.GetDisAggColumns();
    std::vector<OrderByColumn> orderby_columns;

    for (size_t i = 0; i < distinct_columns.size(); ++i) {
        orderby_columns.push_back(OrderByColumn(distinct_columns[i]));
    }
    return orderby_columns;
}

Plan* SimplePlanner::GenerateJoinPlan() {
    const std::vector<std::shared_ptr<TableEntry> >& entrys =
        m_query.GetTableEntry();

    if (m_query.GetHasJoin()) {
        const std::vector<JoinOperator>& join_ops = m_query.GetJoinOps();
        const std::vector<ShrExpr>& join_exprs = m_query.GetJoinExpr();
        CHECK_EQ(1U, join_ops.size()) << "join ops  size is " <<join_ops.size();
        CHECK_EQ(1U, join_exprs.size()) << "join exprs size is " <<join_exprs.size();
        CHECK_EQ(2U, entrys.size()) << "entry size is " << entrys.size();
        std::vector<Plan*> subplans;

        Plan* left_plan = GenerateSingleTableFilterPlan(entrys[0], true);
        subplans.push_back(left_plan);
        Plan* right_plan = GenerateSingleTableFilterPlan(entrys[1], false);
        subplans.push_back(right_plan);

        if (left_plan == NULL || right_plan == NULL) {
            return NULL;
        }

        const std::vector<AffectedColumnInfo>& left_affected_column_infos =
        entrys[0]->GetAffectedColumnInfo();

        std::vector<uint64_t> left_affect_ids;
        std::vector<uint64_t> right_affect_ids;

        for (size_t i = 0 ; i < left_affected_column_infos.size(); ++i) {
            left_affect_ids.push_back(left_affected_column_infos[i].m_affect_id);
        }

        const std::vector<AffectedColumnInfo>& right_affected_column_infos =
        entrys[1]->GetAffectedColumnInfo();
        for (size_t i = 0 ; i <  right_affected_column_infos.size(); ++i) {
            right_affect_ids.push_back(right_affected_column_infos[i].m_affect_id);
        }

        JoinPlan* plan = new JoinPlan(subplans, join_exprs[0], join_ops[0],
                                      left_affect_ids, right_affect_ids);
        return plan;
    } else {
        CHECK_EQ(1, entrys.size()) << "Only one table is permitted in this version";
        return GenerateSingleTableFilterPlan(entrys[0], true);
    }
}

Plan* SimplePlanner::GenerateOrderbyPlan() {
    Plan* subplan = GenerateProjectedPlan();
    if (subplan == NULL)
        return NULL;

    bool has_distinct = m_query.GetHasDistinct();

    if (has_distinct) {
        subplan = GenerateDistinctPlan(subplan);
    }

    const std::vector<OrderByColumn>& orderby_columns =
        m_query.GetOrderByColumns();
    if (orderby_columns.size() == 0) {
        return subplan;
    }

    SortPlan* plan = new SortPlan(subplan, orderby_columns);
    return plan;
}

Plan* SimplePlanner::GenerateProjectedPlan() {
    Plan* subplan = GenerateJoinPlan();

    if (subplan == NULL)
        return NULL;
    const std::vector<std::shared_ptr<Target> > target_list =
        m_query.GetTargets();
    const std::vector<AffectedColumnInfo>& subplan_affect_column_infos =
        m_query.GetAffectedColumnInfo();

    if (m_query.GetAggregateType() == SelectQuery::kGroupByAgg) {
        subplan = GenerateAggregatePlan(subplan);
    } else if (m_query.GetAggregateType() == SelectQuery::kWithinAgg) {
        subplan = new WithinPlan(
            target_list, subplan_affect_column_infos, subplan);
    } else {
        subplan = new ProjectionPlan(
            target_list, subplan_affect_column_infos, subplan);
    }

    const std::shared_ptr<Expression>& having_filter = m_query.GetHaving();

    if (having_filter != NULL) {
        subplan = new FilterPlan(subplan, having_filter);
    }
    return subplan;
}

Plan* SimplePlanner::GenerateAggregatePlan(Plan* subplan) {
    const std::vector<std::shared_ptr<Target> > target_list =
        m_query.GetTargets();
    std::vector<int> result_define_levels;

    std::vector<BQType> subplan_tuple_types;
    const std::vector<AffectedColumnInfo>& subplan_affect_column_infos =
        m_query.GetAffectedColumnInfo();

    for (size_t i = 0; i < subplan_affect_column_infos.size(); ++i) {
        subplan_tuple_types.push_back(
            subplan_affect_column_infos[i].m_column_info.m_type);
    }

    std::vector<OrderByColumn> orderby_columns;
    const std::vector<GroupByColumn>& groupby_columns =
        m_query.GetGroupByColumns();

    for (size_t i = 0; i < groupby_columns.size(); ++i) {
        orderby_columns.push_back(OrderByColumn(groupby_columns[i]));
    }

    AggregatePlanProto::AggregateMode mode = AggregatePlanProto::kNotParallel;

    Plan* sort_plan = new SortPlan(subplan, orderby_columns);
    std::vector<Plan*> subplans;
    subplans.push_back(sort_plan);
    Plan* merge_plan = new MergePlan(subplans, orderby_columns);

    return new AggregatePlan(merge_plan, mode, target_list,
                             groupby_columns, subplan_tuple_types);
}

Plan* SimplePlanner::GenerateSingleTableFilterPlan(
    const std::shared_ptr<TableEntry>& entry, bool has_where) {

    const std::vector<AffectedColumnInfo>& all_affected_column_infos =
        m_query.GetAffectedColumnInfo();

    const std::vector<AffectedColumnInfo>& entry_affected_column_infos =
        entry->GetAffectedColumnInfo();

    std::vector<uint64_t> affect_ids;
    for (size_t i = 0 ; i < entry_affected_column_infos.size(); ++i) {
        affect_ids.push_back(entry_affected_column_infos[i].m_affect_id);
    }

    // CHECK_EQ(1, entrys.size()) << "Only one table is permitted in this version";
    Plan* scan_plan = GenerateScanPlan(entry,
                                       all_affected_column_infos,
                                       affect_ids);
    if (scan_plan == NULL)
        return NULL;

    const std::shared_ptr<Expression>& where = m_query.GetWhere();
    if (where == NULL || has_where == false)
        return scan_plan;

    FilterPlan* filter_plan = new FilterPlan(scan_plan, where);
    return filter_plan;
}

Plan* SimplePlanner::GenerateScanPlan(
    const std::shared_ptr<TableEntry>& entry,
    const std::vector<AffectedColumnInfo>& affected_column_infos,
    const std::vector<uint64_t>& affect_ids) {

    std::vector<ColumnInfo> column_infos;

    for (size_t i = 0; i < affected_column_infos.size(); i++) {
        column_infos.push_back(affected_column_infos[i].m_column_info);
    }

    const std::vector<TabletInfo>& tablet_files = entry->GetTabletFile();
    CHECK_EQ(1U, tablet_files.size())
        << "SimplePlanner can only process 1 tablet_file";

    ScanPlan* plan = new ScanPlan(entry->GetTableName(),
                                  tablet_files[0],
                                  column_infos,
                                  affect_ids);

    return plan;
}

} // namespace compiler
} // namespace gunir

