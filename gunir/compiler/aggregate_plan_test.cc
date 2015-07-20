// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <cstdio>
#include <string>

#include "thirdparty/gtest/gtest.h"

#include "gunir/compiler/aggregate_plan.h"
#include "gunir/compiler/compiler_test_helper.h"
#include "gunir/compiler/merge_plan.h"
#include "gunir/compiler/scan_plan.h"
#include "gunir/compiler/sort_plan.h"

namespace gunir {
namespace compiler {

Plan* GetPlanForLeafServer(
    const SelectQuery* select_query,
    AggregatePlanProto::AggregateMode mode);

std::vector<OrderByColumn> GetOrderByColumn(
    const std::vector<GroupByColumn>& groupby_columns);

Plan* GetPlanForInterServer(
    const SelectQuery* select_query,
    std::vector<Plan*> subplans,
    AggregatePlanProto::AggregateMode mode);

Plan* GetPlanForMultiLevelAggregateTest(
    std::vector<io::Scanner*>* scanners,
    std::string* result_schema);

static const char* kTestQuery =
    " SELECT "
    " department_id, institute_id, "
    " cnt_student, sum_id, avg_id, sum_age, avg_age, max_age, min_age, "
    " COUNT(student_id) AS CNT_STUDENT, "
    " SUM(student_id) AS SUM_ID, "
    " AVG(student_id) AS AVG_ID, "
    " SUM(student_age) AS SUM_AGE, "
    " AVG(student_age) AS AVG_AGE, "
    " MAX(student_age) AS MAX_AGE, "
    " MIN(student_age) AS MIN_AGE, "
    " SUM(student_id) + 2 * AVG(student_id) AS ALL_ID, "
    " AVG(student_age) + MAX(student_age) * MIN(student_age) - SUM(student_age) AS ALL_AGE "
    " FROM Department "
    " GROUPBY department_id, institute_id, "
    " cnt_student, sum_id, avg_id, sum_age, avg_age, max_age, min_age;";
static const int kAggStart = 2;
static const int kAggNumber = 7;

static const char* kTestQueryProto = "./testdata/aggregate_test/Department.proto";
static const char* kInputTestData = "./testdata/aggregate_test/aggregate_test_src_0";

static const int kLeafPlanNumber = 9;
static const int kGroupSize = 3;


TEST(AggregatePlanTest, aggregate_test) {
    Executor executor;
    std::vector<io::Scanner*> scanners;

    bool result = ProcessQuery(kTestQuery, kTestQueryProto, kInputTestData,
                               &executor, &scanners);
    ASSERT_TRUE(result);

    io::Slice* executor_output_slice;

    while (executor.NextSlice(&executor_output_slice)) {
        for (int i = kAggStart; i < kAggStart + kAggNumber; ++i) {
            const io::Block* b1;
            const io::Block* b2;
            b1 = executor_output_slice->GetBlock(i);
            b2 = executor_output_slice->GetBlock(i + kAggNumber);

            int64_t v1, v2;

            CHECK_EQ(b1->GetValueType(), io::Block::TYPE_INT64);
            v1 = *(reinterpret_cast<const int64_t*> (b1->GetValue().data()));

            if (b2->GetValueType() == io::Block::TYPE_INT64) {
                v2 = *(reinterpret_cast<const int64_t*> (b2->GetValue().data()));
            } else {
                CHECK_EQ(b2->GetValueType(), io::Block::TYPE_DOUBLE);
                v2 = static_cast<int64_t>
                    (*(reinterpret_cast<const double*> (b2->GetValue().data())));
            }
            if (v1 == v2) {
                continue;
            }

            LOG(ERROR) << "result error" << kAggStart << ":" << i
                << ":" << i + kAggNumber;
            ASSERT_EQ(v1, v2);
        }
    }

    for (size_t i = 0; i < scanners.size(); ++i) {
        delete scanners[i];
    }
}

TEST(AggregatePlanTest, multi_level_agg_test) {
    Executor executor;
    std::string result_schema;
    io::Slice* executor_output_slice;
    std::vector<io::Scanner*> scanners;

    Plan* plan = GetPlanForMultiLevelAggregateTest(&scanners, &result_schema);
    executor.Init(plan, result_schema, "QueryResult");

    while (executor.NextSlice(&executor_output_slice)) {
        for (int i = kAggStart; i < kAggStart + kAggNumber; ++i) {
            const io::Block* b1;
            const io::Block* b2;
            b1 = executor_output_slice->GetBlock(i);
            b2 = executor_output_slice->GetBlock(i + kAggNumber);

            int64_t v1, v2;

            v1 = *(reinterpret_cast<const int64_t*> (b1->GetValue().data()));

            if (b2->GetValueType() == io::Block::TYPE_INT64) {
                v2 = *(reinterpret_cast<const int64_t*> (b2->GetValue().data()));
            } else {
                v2 = static_cast<int64_t>
                    (*(reinterpret_cast<const double*> (b2->GetValue().data())));
            }
            if (v1 == v2) {
                continue;
            }

            if (v1 == v2 || v1 * kLeafPlanNumber == v2) {
                continue;
            }

            LOG(ERROR) << "result error " << kAggStart << ":" << i
            << ":" << i + kAggNumber;
            ASSERT_EQ(v1 * kLeafPlanNumber, v2);
        }
    }

    for (size_t i = 0; i < scanners.size(); ++i) {
        delete scanners[i];
    }
}

Plan* GetPlanForMultiLevelAggregateTest(
    std::vector<io::Scanner*>* scanners,
    std::string* result_schema) {
    SelectQuery* select_query;
    if (!ParseQuery(kTestQuery,
                    kTestQueryProto,
                    kInputTestData,
                    &select_query)) {
        return NULL;
    }
    toft::scoped_ptr<SelectQuery> scoped_query(select_query);
    *result_schema = select_query->GetResultSchema();

    std::vector<std::string> tables = select_query->GetQueryTables();
    std::vector<TableInfo> table_infos =
        GetTableInfos(tables, kTestQueryProto, kInputTestData);

    // construct leaf server and inter server for aggregate plan
    std::vector<Plan*> merge_subplan;
    std::vector<Plan*> final_subplan;
    for (int i = 0; i < kLeafPlanNumber; i++) {
        merge_subplan.push_back(
            GetPlanForLeafServer(select_query,
                                 AggregatePlanProto::kLocalAgg));

        scanners->push_back(GetScanner(*select_query, table_infos[0]));

        if ((i + 1) % kGroupSize == 0) {
            Plan* plan = GetPlanForInterServer(
                select_query, merge_subplan, AggregatePlanProto::kMergeAgg);
            final_subplan.push_back(plan);
            merge_subplan.clear();
        }
    }

    for (int i = 0; i < kLeafPlanNumber / kGroupSize; ++i) {
        std::vector<io::Scanner*> temp_scanners;

        for (int j = 0; j < kGroupSize; ++j) {
            temp_scanners.push_back((*scanners)[i * kGroupSize + j]);
        }
        final_subplan[i]->SetScanner(temp_scanners);
    }

    Plan* plan = GetPlanForInterServer(
        select_query, final_subplan, AggregatePlanProto::kFinalAgg);
    return plan;
}

Plan* GetPlanForInterServer(const SelectQuery* select_query,
                            std::vector<Plan*> subplans,
                            AggregatePlanProto::AggregateMode mode) {
    std::vector<GroupByColumn> groupby_columns =
        select_query->GetGroupByColumns();

    std::vector<OrderByColumn> orderby_columns =
        GetOrderByColumn(groupby_columns);

    Plan* sort_plan = new MergePlan(subplans, orderby_columns);

    const std::vector<std::shared_ptr<Target> >& target_list =
        select_query->GetTargets();

    std::vector<BQType> subplan_tuple_types;
    std::string table_schema_string = select_query->GetTransSchema();
    std::string message_name = SchemaBuilder::kQueryTransMessageName;
    std::vector<ColumnInfo> column_infos;
    TableSchema schema;
    CHECK(schema.InitSchemaFromFileDescriptorProto(
            table_schema_string, message_name)) << "Init schema failed";
    schema.GetAllColumnInfo(&column_infos);
    for (size_t i = 0; i < column_infos.size(); ++i) {
        subplan_tuple_types.push_back(column_infos[i].m_type);
    }
    Plan* agg_plan = new AggregatePlan(sort_plan, mode, target_list,
                                       groupby_columns, subplan_tuple_types);

    if (mode != AggregatePlanProto::kFinalAgg) {
        PlanProto plan_proto;
        agg_plan->CopyToProto(&plan_proto);
        delete agg_plan;
        agg_plan = Plan::InitPlanFromProto(plan_proto);
    }

    return agg_plan;
}

std::vector<OrderByColumn> GetOrderByColumn(
    const std::vector<GroupByColumn>& groupby_columns) {
    std::vector<OrderByColumn> orderby_columns;

    for (size_t i = 0; i < groupby_columns.size(); ++i) {
        orderby_columns.push_back(OrderByColumn(groupby_columns[i]));
    }
    return orderby_columns;
}

Plan* GetPlanForLeafServer(const SelectQuery* select_query,
                           AggregatePlanProto::AggregateMode mode) {
    std::vector<ColumnInfo> column_list;
    const std::vector<std::shared_ptr<TableEntry> >& entrys =
        select_query->GetTableEntry();
    const std::shared_ptr<TableEntry>& entry = entrys[0];
    const std::vector<AffectedColumnInfo>& affected_column_infos =
        entry->GetAffectedColumnInfo();

    std::vector<uint64_t> affect_ids;
    for (size_t i = 0; i < affected_column_infos.size(); i++) {
        column_list.push_back(affected_column_infos[i].m_column_info);
        affect_ids.push_back(affected_column_infos[i].m_affect_id);
    }

    TabletInfo info;
    info.set_name(kInputTestData);
    ScanPlan* subplan = new ScanPlan(entry->GetTableName(),
                                     info,
                                     column_list,
                                     affect_ids);
    std::vector<GroupByColumn> groupby_columns =
        select_query->GetGroupByColumns();
    std::vector<OrderByColumn> orderby_columns =
        GetOrderByColumn(groupby_columns);

    Plan* sort_plan = new SortPlan(subplan, orderby_columns);

    std::vector<BQType> subplan_tuple_types;
    const std::vector<AffectedColumnInfo>& subplan_affect_column_infos =
        select_query->GetAffectedColumnInfo();

    for (size_t i = 0; i < subplan_affect_column_infos.size(); ++i) {
        subplan_tuple_types.push_back(
            subplan_affect_column_infos[i].m_column_info.m_type);
    }

    const std::vector<std::shared_ptr<Target> >& target_list =
        select_query->GetTargets();

    Plan* agg_plan = new AggregatePlan(sort_plan, mode, target_list,
                                       groupby_columns, subplan_tuple_types);

    PlanProto plan_proto;
    agg_plan->CopyToProto(&plan_proto);
    delete agg_plan;
    agg_plan = Plan::InitPlanFromProto(plan_proto);

    return agg_plan;
}

} // namespace compiler
} // namespace gunir

