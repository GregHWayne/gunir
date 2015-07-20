// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <cstdio>
#include <string>

#include "thirdparty/gtest/gtest.h"

#include "gunir/compiler/compiler_test_helper.h"
#include "gunir/compiler/merge_plan.h"
#include "gunir/compiler/scan_plan.h"
#include "gunir/compiler/sort_plan.h"

namespace gunir {
namespace compiler {
static const char* kTestQuery =
    " SELECT "
    " student_id, grade, department_id, score "
    " FROM Student "
    " ORDERBY student_id, grade DESC, department_id ASC, score;";

static const char* kTestQueryProto = "./testdata/sort_test/Student.proto";
static const char* kInputTestData = "./testdata/sort_test/sort_test_src_0";
static const char* kResultTestData = "./testdata/sort_test/sort_test_result_0";

static const char* kInputMessageName = "Student";
static const char* kResultMessageName = "QueryResult";

static const size_t kStreamNumber = 5;

bool ProcessOneQuery(Executor* executor,
                     std::vector<io::Scanner*>* all_scanners) {
    std::vector<io::Scanner*> scanners;

    bool result =
        ProcessQuery(kTestQuery,
                     kTestQueryProto,
                     kInputTestData,
                     executor,
                     &scanners);
    all_scanners->insert(
        all_scanners->end(), scanners.begin(), scanners.end());
    return result;
}

// Init kStreamNumber streams for merge sort
MergePlan* GetMergePlan(std::vector<io::Scanner*>* all_scanners,
                        std::string* result_schema,
                        Executor** sub_executors) {
    SelectQuery* select_query;
    if (!ParseQuery(
            kTestQuery, kTestQueryProto, kInputTestData, &select_query)) {
        return NULL;
    }
    toft::scoped_ptr<SelectQuery> query(select_query);

    *result_schema =
        select_query->GetResultSchema();

    std::vector<OrderByColumn> orderby_columns =
        select_query->GetOrderByColumns();
    std::vector<ColumnInfo> result_column_info =
        select_query->GetResultColumnInfo();

    std::vector<uint64_t> affect_ids;

    for (size_t i = 0 ; i < result_column_info.size() ; ++i) {
        affect_ids.push_back(static_cast<uint64_t>(i));
    }

    std::vector<Plan*> scan_plans;
    *sub_executors = new Executor[kStreamNumber];

    TabletInfo info;
    info.set_name(kInputTestData);
    for (size_t i = 0; i < kStreamNumber; ++i) {
        if (!ProcessOneQuery(&(*sub_executors)[i], all_scanners)) {
            return NULL;
        }

        scan_plans.push_back(
            new ScanPlan(kInputMessageName,
                         info,
                         result_column_info,
                         affect_ids));

        std::vector<io::Scanner*> scanners;
        scanners.push_back(&(*sub_executors)[i]);
        scan_plans[i]->SetScanner(scanners);
    }

    return new MergePlan(scan_plans, orderby_columns);
}

bool GetLocalScanner(LocalTabletScanner* local_scanner) {
    std::vector<std::string> query_result_columns;

    query_result_columns.push_back("Student.student_id");
    query_result_columns.push_back("Student.grade");
    query_result_columns.push_back("Student.department_id");
    query_result_columns.push_back("Student.score");

    return local_scanner->Init(
            kResultMessageName, kResultTestData, query_result_columns);
}

TEST(MergePlanTest, merge_test) {
    std::vector<io::Scanner*> all_scanners;
    std::string result_schema;
    Executor* sub_executors;
    Executor executor;

    // init executor for merge sort
    MergePlan* merge_plan =
        GetMergePlan(&all_scanners, &result_schema, &sub_executors);
    ASSERT_TRUE(merge_plan != NULL);
    executor.Init(merge_plan, result_schema, kResultMessageName);

    // init scanner for correct result
    LocalTabletScanner local_scanner;
    ASSERT_TRUE(GetLocalScanner(&local_scanner));

    io::Slice* executor_output_slice = NULL;
    io::Slice* corrent_slice = NULL;
    int count = 0;

    while (count < 100 && executor.NextSlice(&executor_output_slice)) {
        if (count % kStreamNumber == 0) {
            ASSERT_TRUE(local_scanner.NextSlice(&corrent_slice));
        }

        ASSERT_EQ(corrent_slice->GetCount(),
                  executor_output_slice->GetCount()) <<
            SliceDebugInfo(corrent_slice, executor_output_slice);

        ASSERT_EQ(corrent_slice->GetSelectLevel(),
                  executor_output_slice->GetSelectLevel()) <<
            SliceDebugInfo(corrent_slice, executor_output_slice);

        ASSERT_TRUE(IsSliceEqual(corrent_slice, executor_output_slice)) <<
            SliceDebugInfo(corrent_slice, executor_output_slice);
        count++;
    }

    for (size_t i = 0; i < all_scanners.size(); ++i) {
        delete all_scanners[i];
    }
    delete[] sub_executors;
}

} // namespace compiler
} // namespace gunir

