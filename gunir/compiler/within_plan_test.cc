// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <string>

#include "gunir/compiler/compiler_test_helper.h"
#include "gunir/compiler/within_plan.h"

namespace gunir {
namespace compiler {

TEST(WithinPlanTest, aggregate_function_test) {
    const char* query =
        " SELECT docid, "
        " MAX(links.forward) WITHIN RECORD as max_fwd, "
        " MIN(links.forward) WITHIN RECORD as min_fwd, "
        " AVG(links.forward) WITHIN RECORD as avg_fwd, "
        " SUM(links.forward) WITHIN RECORD as sum_fwd, "
        " COUNT(links.forward) WITHIN RECORD as count_fwd "
        " FROM Document;";

    Executor executor;
    std::vector<io::Scanner*> scanners;

    bool result =
        ProcessQuery(query,
                     "./testdata/within_test/Document.proto",
                     "./testdata/within_test/aggregate_forward_src",
                     &executor,
                     &scanners);
    ASSERT_TRUE(result);

    LocalTabletScanner local_scanner;
    std::vector<std::string> query_result_columns;
    query_result_columns.push_back("AggregateForward.docid");
    query_result_columns.push_back("AggregateForward.max_fwd");
    query_result_columns.push_back("AggregateForward.min_fwd");
    query_result_columns.push_back("AggregateForward.avg_fwd");
    query_result_columns.push_back("AggregateForward.sum_fwd");
    query_result_columns.push_back("AggregateForward.count_fwd");

    ASSERT_TRUE(local_scanner.Init(
            "QueryResult",
            "./testdata/within_test/aggregate_forward_result",
            query_result_columns));

    io::Slice* executor_output_slice;
    io::Slice* right_result_slice;
    while (executor.NextSlice(&executor_output_slice)) {
        ASSERT_TRUE(local_scanner.NextSlice(&right_result_slice));
        ASSERT_EQ(right_result_slice->GetCount(),
                  executor_output_slice->GetCount());

        ASSERT_EQ(right_result_slice->GetSelectLevel(),
                  executor_output_slice->GetSelectLevel());

        ASSERT_TRUE(IsSliceEqual(right_result_slice, executor_output_slice));
    }

    ASSERT_FALSE(local_scanner.NextSlice(&right_result_slice));
    for (size_t i = 0; i < scanners.size(); ++i) {
        delete scanners[i];
    }
}

TEST(WithinPlanTest, multi_level_within_agg_test) {
    const char* query =
        " SELECT docid,"
        " MAX(links.backward) WITHIN RECORD AS max_bwd,"
        " SUM(links.backward) WITHIN links AS sum_bwd,"
        " COUNT(name.language.code) WITHIN RECORD AS cnt_record_code,"
        " COUNT(name.language.code) WITHIN name AS cnt_name_code,"
        " COUNT(name.language.code) WITHIN name.language AS cnt_lang_code"
        " FROM Document;";

    Executor executor;
    std::vector<io::Scanner*> scanners;

    bool result = ProcessQuery(
        query,
        "./testdata/within_test/Document.proto",
        "./testdata/within_test/multi_level_agg_src",
        &executor,
        &scanners);
    ASSERT_TRUE(result);

    LocalTabletScanner local_scanner;
    std::vector<std::string> query_result_columns;
    query_result_columns.push_back("MultiLevelAggregate.docid");
    query_result_columns.push_back("MultiLevelAggregate.max_bwd");
    query_result_columns.push_back("MultiLevelAggregate.links.sum_bwd");
    query_result_columns.push_back("MultiLevelAggregate.cnt_record_code");
    query_result_columns.push_back("MultiLevelAggregate.name.cnt_name_code");
    query_result_columns.push_back(
        "MultiLevelAggregate.name.language.cnt_lang_code");

    ASSERT_TRUE(
        local_scanner.Init("QueryResult",
                           "./testdata/within_test/multi_level_agg_result",
                           query_result_columns));

    io::Slice* executor_output_slice;
    io::Slice* right_result_slice;
    while (executor.NextSlice(&executor_output_slice)) {
        ASSERT_TRUE(local_scanner.NextSlice(&right_result_slice));

        ASSERT_EQ(right_result_slice->GetCount(),
                  executor_output_slice->GetCount());

        ASSERT_EQ(right_result_slice->GetSelectLevel(),
                  executor_output_slice->GetSelectLevel());

        ASSERT_TRUE(IsSliceEqual(right_result_slice, executor_output_slice));
    }

    ASSERT_FALSE(local_scanner.NextSlice(&right_result_slice));
    for (size_t i = 0; i < scanners.size(); ++i) {
        delete scanners[i];
    }
}

} // namespace compiler
} // namespace gunir

