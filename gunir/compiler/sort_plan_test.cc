// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <cstdio>
#include <string>

#include "thirdparty/gtest/gtest.h"

#include "gunir/compiler/compiler_test_helper.h"
#include "gunir/compiler/sort_plan.h"

namespace gunir {
namespace compiler {

TEST(SortPlanTest, sort_test) {
    const char* query =
        " SELECT "
        " student_id, grade, department_id, score "
        " FROM Student "
        " ORDERBY student_id, grade DESC, department_id ASC, score;";

    Executor executor;
    std::vector<io::Scanner*> scanners;

    bool result =
        ProcessQuery(query,
                     "./testdata/sort_test/Student.proto",
                     "./testdata/sort_test/sort_test_src_0",
                     &executor,
                     &scanners);
    ASSERT_TRUE(result);

    LocalTabletScanner local_scanner;
    std::vector<std::string> query_result_columns;
    query_result_columns.push_back("Student.student_id");
    query_result_columns.push_back("Student.grade");
    query_result_columns.push_back("Student.department_id");
    query_result_columns.push_back("Student.score");

    ASSERT_TRUE(local_scanner.Init(
            "QueryResult",
            "./testdata/sort_test/sort_test_result_0",
            query_result_columns));

    io::Slice* executor_output_slice;
    io::Slice* right_result_slice;
    while (executor.NextSlice(&executor_output_slice)) {
        ASSERT_TRUE(local_scanner.NextSlice(&right_result_slice));
        ASSERT_EQ(right_result_slice->GetCount(),
                  executor_output_slice->GetCount()) <<
            SliceDebugInfo(right_result_slice, executor_output_slice);

        ASSERT_EQ(right_result_slice->GetSelectLevel(),
                  executor_output_slice->GetSelectLevel()) <<
            SliceDebugInfo(right_result_slice, executor_output_slice);

        ASSERT_TRUE(IsSliceEqual(right_result_slice, executor_output_slice)) <<
            SliceDebugInfo(right_result_slice, executor_output_slice);
    }

    ASSERT_FALSE(local_scanner.NextSlice(&right_result_slice));
    for (size_t i = 0; i < scanners.size(); ++i) {
        delete scanners[i];
    }
}

} // namespace compiler
} // namespace gunir

