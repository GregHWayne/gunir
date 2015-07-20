// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/parallel_planner.h"

#include <algorithm>

#include "gunir/compiler/aggregate_plan.h"
#include "gunir/compiler/filter_plan.h"
#include "gunir/compiler/join_plan.h"
#include "gunir/compiler/limit_plan.h"
#include "gunir/compiler/merge_plan.h"
#include "gunir/compiler/projection_plan.h"
#include "gunir/compiler/scan_plan.h"
#include "gunir/compiler/simple_plan_scheduler.h"
#include "gunir/compiler/sort_plan.h"
#include "gunir/compiler/union_plan.h"
#include "gunir/compiler/uniq_plan.h"
#include "gunir/compiler/within_plan.h"

namespace gunir {
namespace compiler {

ParallelPlanner::ParallelPlanner(const SelectQuery& query)
    : Planner(query),
      m_plan_scheduler(new SimplePlanScheduler()) {

    const std::vector<ShrEntry>& entrys = m_query.GetTableEntry();

    CHECK_LE(entrys.size(), 2U) << "Join is not supported";
    m_entry = entrys[0];

    m_tablet_files = m_entry->GetTabletFile();
    CHECK_LE(1U, m_tablet_files.size());

    m_tablet_to_assign = 0;
}

bool ParallelPlanner::GenerateExecutePlan(
    TaskPlanProto* task_plan_proto) {

    TaskPlanProto* proto = GetParallelPlan();
    if (proto == NULL) {
        LOG(ERROR) << "Cannot generate plan";
        return false;
    }

    task_plan_proto->CopyFrom(*proto);
    delete proto;

    return true;
}

TaskPlanProto* ParallelPlanner::GetParallelPlan() {
    int num_leaf_task = m_tablet_files.size();

    toft::scoped_ptr<PlanNode> root_plan_node(
        m_plan_scheduler->GetPlanStrategy(num_leaf_task));

    TaskPlanProto* proto = GetPlanForNode(*root_plan_node);

    return proto;
}

TaskPlanProto* ParallelPlanner::GetPlanForNode(const PlanNode& node) {
    TaskPlanProto* my_task_plan_proto = new TaskPlanProto();
    Plan* plan = NULL;

    for (size_t i = 0; i < node.m_sub_node_list.size(); ++i) {
        TaskPlanProto* proto = GetPlanForNode(node.m_sub_node_list[i]);
        (my_task_plan_proto->mutable_sub_task_plan_list())->AddAllocated(proto);
    }

    size_t num_subplan = node.m_sub_node_list.size();
    switch (node.m_type) {
    case PlanNode::kLeafServerNode:
        plan = GetExecPlanForLeafServer(my_task_plan_proto);
        plan = AddLimitPlan(plan);
        break;

    case PlanNode::kMiddleInterServerNode:
        plan = GetExecPlanForInterServer(my_task_plan_proto, num_subplan);
        plan = AddLimitPlan(plan);
        break;

    case PlanNode::kRootInterServerNode:
        plan = GetExecPlanForRootInterServer(my_task_plan_proto, num_subplan);
        break;
    }

    if (plan == NULL) {
        return NULL;
    }

    plan->CopyToProto(my_task_plan_proto->mutable_exec_plan());
    delete plan;

    return my_task_plan_proto;
}

Plan* ParallelPlanner::GetExecPlanForRootInterServer(
    TaskPlanProto* task_plan_proto, size_t num_subplan) {
    bool is_root = true;

    task_plan_proto->set_table_schema_string(m_query.GetResultSchema());
    task_plan_proto->set_table_message_name(
        SchemaBuilder::kQueryResultMessageName);

    Plan* subplan = NULL;
    if (HasGroupByAgg()) {
        subplan = GetMergeAggregatePlan(is_root, num_subplan);
    } else if (HasSort() && !HasDistinct()) {
        subplan = GetMergeSortPlan(num_subplan);
    } else {
        subplan = GetUnionPlan(num_subplan);
    }

    return GetFinalPlan(subplan);
}

Plan* ParallelPlanner::GetExecPlanForInterServer(
    TaskPlanProto* task_plan_proto, size_t num_subplan) {
    bool is_root = false;

    if (HasGroupByAgg()) {
        task_plan_proto->set_table_schema_string(m_query.GetTransSchema());
        task_plan_proto->set_table_message_name(
            SchemaBuilder::kQueryTransMessageName);
        return GetMergeAggregatePlan(is_root, num_subplan);
    }

    task_plan_proto->set_table_schema_string(m_query.GetResultSchema());
    task_plan_proto->set_table_message_name(
        SchemaBuilder::kQueryResultMessageName);

    if (HasSort()) {
        return GetMergeSortPlan(num_subplan);
    }
    return GetUnionPlan(num_subplan);
}

Plan* ParallelPlanner::GetExecPlanForLeafServer(
    TaskPlanProto* task_plan_proto) {

    // agg plan
    if (HasGroupByAgg()) {
        task_plan_proto->set_table_schema_string(m_query.GetTransSchema());
        task_plan_proto->set_table_message_name(
            SchemaBuilder::kQueryTransMessageName);
        return GetLocalAggregatePlan();
    }

    task_plan_proto->set_table_schema_string(m_query.GetResultSchema());
    task_plan_proto->set_table_message_name(
        SchemaBuilder::kQueryResultMessageName);

    // distinct plan
    if (HasDistinct()) {
        return GetDistinctPlan();
    }

    // plain plan
    return GetOrderbyPlan();
}

Plan* ParallelPlanner::GetFinalPlan(Plan* plan) {
    const ShrExpr& having_filter = m_query.GetHaving();

    if (having_filter != NULL) {
        plan = new FilterPlan(plan, having_filter);
    }

    if ((HasGroupByAgg() || HasDistinct()) && HasSort()) {
        plan = new SortPlan(plan, m_query.GetOrderByColumns());
    }

    int64_t start, number;
    bool has_limit = m_query.GetLimit(&start, &number);

    if (has_limit) {
        plan = new LimitPlan(plan, start, number);
    }
    return plan;
}

Plan* ParallelPlanner::GetMergeAggregatePlan(bool is_root, int num_subplan) {
    std::vector<OrderByColumn> orderby_columns = GetOrderByColumnsFromGroupBy();

    std::vector<ColumnInfo>
        input_columns = GetColumnsInTransSchema();

    std::vector<uint64_t> affect_ids = GetAffectIds(input_columns);
    AggregatePlanProto::AggregateMode agg_mode;

    if (is_root) {
        agg_mode = AggregatePlanProto::kFinalAgg;
    } else {
        agg_mode = AggregatePlanProto::kMergeAgg;
    }

    std::vector<Plan*> subplans;

    for (int i = 0; i < num_subplan; ++i) {
        subplans.push_back(new ScanPlan(input_columns, affect_ids));
    }

    Plan* merge_plan = new MergePlan(subplans, orderby_columns);

    return new AggregatePlan(merge_plan,
                             agg_mode,
                             m_query.GetTargets(),
                             m_query.GetGroupByColumns(),
                             GetTupleTypesOfColumns(input_columns));
}

Plan* ParallelPlanner::GetMergeSortPlan(size_t num_subplan) {
    std::vector<ColumnInfo>
        column_infos = GetColumnsInResultSchema();

    std::vector<uint64_t> affect_ids = GetAffectIds(column_infos);
    std::vector<Plan*> subplan_list;
    for (size_t i = 0; i < num_subplan; ++i) {
        subplan_list.push_back(new ScanPlan(column_infos, affect_ids));
    }

    return new MergePlan(subplan_list, m_query.GetOrderByColumns());
}

std::vector<uint64_t> ParallelPlanner::GetAffectIds(
    const std::vector<ColumnInfo>& column_infos) {
    std::vector<uint64_t> affect_ids;
    for (size_t i = 0; i < column_infos.size(); ++i) {
        affect_ids.push_back(static_cast<uint64_t>(i));
    }
    return affect_ids;
}

Plan* ParallelPlanner::GetUnionPlan(size_t num_subplan) {
    std::vector<ColumnInfo>
        column_infos = GetColumnsInResultSchema();

    std::vector<uint64_t> affect_ids = GetAffectIds(column_infos);
    std::vector<Plan*> subplan_list;
    for (size_t i = 0; i < num_subplan; ++i) {
        subplan_list.push_back(new ScanPlan(column_infos, affect_ids));
    }

    if (HasDistinct()) {
        return new MergePlan(subplan_list, GetOrderByColumnsFromDistinct());
    } else {
        return new UnionPlan(subplan_list);
    }
}

Plan* ParallelPlanner::GetLocalAggregatePlan() {
    Plan* subplan = GetFilterPlan();
    if (subplan == NULL) {
        return NULL;
    }

    AggregatePlanProto::AggregateMode agg_mode;
    agg_mode = AggregatePlanProto::kLocalAgg;

    std::vector<OrderByColumn> orderby_columns = GetOrderByColumnsFromGroupBy();

    subplan = new SortPlan(subplan, orderby_columns);
    return new AggregatePlan(subplan,
                             agg_mode,
                             m_query.GetTargets(),
                             m_query.GetGroupByColumns(),
                             GetSubPlanTupleTypes());
}

Plan* ParallelPlanner::GetOrderbyPlan() {
    Plan* projection_plan = GetHavingPlan();
    if (projection_plan == NULL || !HasSort()) {
        return projection_plan;
    }

    const std::vector<OrderByColumn>&
        orderby_columns = m_query.GetOrderByColumns();

    return new SortPlan(projection_plan, orderby_columns);
}

Plan* ParallelPlanner::GetDistinctPlan() {
    Plan* subplan = GetHavingPlan();
    if (subplan == NULL || !HasDistinct()) {
        return subplan;
    }

    std::vector<OrderByColumn> orderby_columns
        = GetOrderByColumnsFromDistinct();
    subplan = new SortPlan(subplan, orderby_columns);

    const std::vector<DistinctColumn>&
        distinct_columns = m_query.GetDistinctColumns();

    std::vector<size_t> distinct_targets;
    for (size_t i = 0; i < distinct_columns.size(); ++i) {
        distinct_targets.push_back(distinct_columns[i].m_affect_id);
    }

    return new UniqPlan(subplan, distinct_targets);
}

Plan* ParallelPlanner::GetHavingPlan() {
    Plan* subplan = GetProjectionPlan();
    if (subplan == NULL || !HasHaving()) {
        return subplan;
    }

    const ShrExpr& having_filter = m_query.GetHaving();
    return new FilterPlan(subplan, having_filter);
}


Plan* ParallelPlanner::GetFilterPlan() {
    Plan* subplan = NULL;
    if (m_query.GetHasJoin()) {
        subplan = GetJoinPlan();
    } else {
        bool has_where = true;
        CHECK_LT(m_tablet_to_assign, m_tablet_files.size());
        subplan = GetWhereFilterPlan(m_entry,
                                     m_tablet_files[m_tablet_to_assign],
                                     has_where);
        m_tablet_to_assign++;
    }
    return subplan;
}

Plan* ParallelPlanner::GetProjectionPlan() {
    Plan* subplan = GetFilterPlan();
    if (subplan == NULL) {
        return subplan;
    }

    const std::vector<ShrTarget>& target_list = m_query.GetTargets();
    const std::vector<AffectedColumnInfo>&
        subplan_affect_column_infos = m_query.GetAffectedColumnInfo();

    if (HasWithinAgg()) {
        subplan = new WithinPlan(
            target_list, subplan_affect_column_infos, subplan);
    } else {
        subplan = new ProjectionPlan(
            target_list, subplan_affect_column_infos, subplan);
    }

    return subplan;
}

Plan* ParallelPlanner::GetWhereFilterPlan(
        const std::shared_ptr<TableEntry>& entry,
        const TabletInfo& tablet_file,
        bool has_where) {
    Plan* scan_plan = GetScanPlan(entry, tablet_file);
    const ShrExpr& where = m_query.GetWhere();

    if (scan_plan == NULL || where == NULL || has_where == false) {
        return scan_plan;
    }

    FilterPlan* where_plan = new FilterPlan(scan_plan, where);
    return where_plan;
}

Plan* ParallelPlanner::GetJoinPlan() {
    const std::vector<std::shared_ptr<TableEntry> >& entrys =
        m_query.GetTableEntry();
    const std::vector<JoinOperator>& join_ops = m_query.GetJoinOps();
    const std::vector<ShrExpr>& join_exprs = m_query.GetJoinExpr();
    std::vector<uint64_t> left_affect_ids;
    std::vector<uint64_t> right_affect_ids;
    bool has_where;

    CHECK_EQ(1U, join_ops.size()) << "join ops  size is " << join_ops.size();
    CHECK_EQ(1U, join_exprs.size()) << "join exprs size is " << join_exprs.size();
    CHECK_EQ(2U, entrys.size()) << "entry size is " << entrys.size();

    std::vector<Plan*> subplans;

    // left plan
    has_where = true;
    CHECK_LT(m_tablet_to_assign, m_tablet_files.size());
    Plan* left_plan = GetWhereFilterPlan(entrys[0],
                                         m_tablet_files[m_tablet_to_assign],
                                         has_where);
    m_tablet_to_assign++;
    subplans.push_back(left_plan);

    // right plan , join table
    has_where = false;
    Plan* right_plan = GetWhereFilterPlan(entrys[1],
                                          (entrys[1]->GetTabletFile())[0],
                                          has_where);
    subplans.push_back(right_plan);

    if (left_plan == NULL || right_plan == NULL) {
        return NULL;
    }

    const std::vector<AffectedColumnInfo>& left_affected_column_infos =
        entrys[0]->GetAffectedColumnInfo();
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
}

Plan* ParallelPlanner::GetScanPlan(const std::shared_ptr<TableEntry>& entry,
                                   const TabletInfo& tablet_file) {
    const std::vector<AffectedColumnInfo>& affected_column_infos =
        m_query.GetAffectedColumnInfo();
    std::vector<ColumnInfo> column_list;
    for (size_t i = 0; i < affected_column_infos.size(); i++) {
        column_list.push_back(affected_column_infos[i].m_column_info);
    }

    const std::vector<AffectedColumnInfo>& entry_affected_column_infos =
        entry->GetAffectedColumnInfo();
    std::vector<uint64_t> affect_ids;
    for (size_t i = 0 ; i < entry_affected_column_infos.size(); ++i) {
        affect_ids.push_back(entry_affected_column_infos[i].m_affect_id);
    }

    const std::string& table_name = entry->GetTableName();
    Plan* scan_plan = new ScanPlan(table_name,
                                   tablet_file,
                                   column_list,
                                   affect_ids);
    return scan_plan;
}

std::vector<ColumnInfo> ParallelPlanner::GetColumnsInResultSchema() {
    std::string table_schema = m_query.GetResultSchema();
    std::string message_name = SchemaBuilder::kQueryResultMessageName;

    std::vector<ColumnInfo> column_infos;
    GetFinalSchemaColumnInfo(
        table_schema, message_name, &column_infos);
    return column_infos;
}

std::vector<ColumnInfo> ParallelPlanner::GetColumnsInTransSchema() {
    CHECK(HasGroupByAgg());

    std::string table_schema = m_query.GetTransSchema();
    std::string message_name = SchemaBuilder::kQueryTransMessageName;

    std::vector<ColumnInfo> column_infos;
    GetFinalSchemaColumnInfo(
        table_schema, message_name, &column_infos);
    return column_infos;
}

std::vector<BQType> ParallelPlanner::GetTupleTypesOfColumns(
    const std::vector<ColumnInfo>& column_infos) {
    std::vector<BQType> subplan_tuple_types;

    for (size_t i = 0; i < column_infos.size(); ++i) {
        subplan_tuple_types.push_back(column_infos[i].m_type);
    }
    return subplan_tuple_types;
}

std::vector<OrderByColumn> ParallelPlanner::GetOrderByColumnsFromDistinct() {
    const std::vector<DistinctColumn>&
        distinct_columns = m_query.GetDistinctColumns();
    std::vector<OrderByColumn> orderby_columns;

    for (size_t i = 0; i < distinct_columns.size(); ++i) {
        orderby_columns.push_back(OrderByColumn(distinct_columns[i]));
    }
    return orderby_columns;
}

std::vector<OrderByColumn> ParallelPlanner::GetOrderByColumnsFromDisAgg() {
    const std::vector<DistinctColumn>&
        distinct_columns = m_query.GetDisAggColumns();
    std::vector<OrderByColumn> orderby_columns;

    for (size_t i = 0; i < distinct_columns.size(); ++i) {
        orderby_columns.push_back(OrderByColumn(distinct_columns[i]));
    }
    return orderby_columns;
}

std::vector<OrderByColumn> ParallelPlanner::GetOrderByColumnsFromGroupBy() {
    const std::vector<GroupByColumn>&
        groupby_columns = m_query.GetGroupByColumns();
    std::vector<OrderByColumn> orderby_columns;

    for (size_t i = 0; i < groupby_columns.size(); ++i) {
        orderby_columns.push_back(OrderByColumn(groupby_columns[i]));
    }

    return orderby_columns;
}

std::vector<BQType> ParallelPlanner::GetSubPlanTupleTypes() {
    const std::vector<AffectedColumnInfo>&
        subplan_affect_column_infos = m_query.GetAffectedColumnInfo();

    std::vector<BQType> subplan_tuple_types;
    for (size_t i = 0; i < subplan_affect_column_infos.size(); ++i) {
        const AffectedColumnInfo& column = subplan_affect_column_infos[i];

        subplan_tuple_types.push_back(column.m_column_info.m_type);
    }
    return subplan_tuple_types;
}

void ParallelPlanner::GetFinalSchemaColumnInfo(
    const std::string& table_schema,
    const std::string& message_name,
    std::vector<ColumnInfo>* column_infos) {

    TableSchema schema;
    CHECK(schema.InitSchemaFromFileDescriptorProto(
            table_schema, message_name)) << "Init schema failed";

    schema.GetAllColumnInfo(column_infos);
}

Plan* ParallelPlanner::AddLimitPlan(Plan* plan) {
    if (HasGroupByAgg()) {
        if (HasSort() || HasHaving()) {
            return plan;
        }
    }

    // can be optimized when only sort or distinct in the future
    if (HasDistinct()) {
        return plan;
    }
    int64_t start, number;
    bool has_limit = m_query.GetLimit(&start, &number);

    if (has_limit) {
        plan = new LimitPlan(plan, 0, start + number);
    }
    return plan;
}

} // namespace compiler
} // namespace gunir

