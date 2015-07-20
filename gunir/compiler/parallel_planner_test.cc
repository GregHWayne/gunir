// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <cstdio>

#include "thirdparty/gtest/gtest.h"

#include "gunir/io/data_holder.h"
#include "gunir/compiler/compiler_test_helper.h"
#include "gunir/compiler/compiler_utils.h"
#include "gunir/compiler/executor.h"
#include "gunir/compiler/parallel_planner.h"
#include "gunir/compiler/parser/big_query_parser.h"
#include "gunir/compiler/schema_builder.h"
#include "gunir/compiler/select_query.h"

#include "gunir/io/slice.h"
#include "gunir/utils/proto_helper.h"

DECLARE_int32(gunir_compiler_leaf_plan_limit);
DECLARE_int32(gunir_compiler_inter_plan_limit);


DEFINE_int32(gunir_compiler_parallel_buffer_size, 1024,
             "size of write buffer");

namespace gunir {
namespace compiler {

extern const char* kTabletSpliter;
static const char* kStudentProtoFile =
    "./testdata/parallel_planner_test/Student.proto";

static const char* kStudentInputData =
    "./testdata/parallel_planner_test/sort_test_src_0";

static const char* kStudentOutputData =
    "./testdata/parallel_planner_test/sort_test_result_0";

std::vector<io::Scanner*> g_all_scanners;
Executor* ConvertTaskPlanProtoToExecutor(
    const TaskPlanProto& proto) {
    std::vector<io::Scanner*> sub_scanners;

    for (int i = 0; i < proto.sub_task_plan_list_size(); ++i) {
        Executor* sub_executor = ConvertTaskPlanProtoToExecutor(
            proto.sub_task_plan_list(i));
        sub_scanners.push_back(sub_executor);
    }

    Executor* executor = new Executor();

    bool is_succeed = executor->Init(proto.exec_plan(),
                                     proto.table_schema_string(),
                                     proto.table_message_name());
    CHECK(is_succeed);
    if (sub_scanners.size() == 0) {
        std::vector<TabletInfo> info_list;
        std::vector<std::vector<std::string> >column_name_list;
        GetTabletFileAndColumnsFromPlan(proto.exec_plan(),
                                        &info_list,
                                        &column_name_list);

        for (size_t i = 0; i < info_list.size(); i++) {
            LocalTabletScanner* local_scanner = new LocalTabletScanner();
            EXPECT_TRUE(local_scanner->Init(
                    kStudentProtoFile,
                    info_list[i].name(),
                    column_name_list[i]));
            sub_scanners.push_back(local_scanner);
        }
    }
    executor->SetScanner(sub_scanners);
    g_all_scanners.insert(g_all_scanners.end(),
                          sub_scanners.begin(), sub_scanners.end());
    return executor;
}

Executor* ConvertQueryToExecutor(const std::string& query,
                                 const std::string& proto_file,
                                 const std::string& input_data) {
    SelectQuery* select_query;
    CHECK(ParseQuery(query,
                     proto_file,
                     input_data,
                     &select_query));

    // Parallel plan
    TaskPlanProto task_plan_proto;

    ParallelPlanner planer(*select_query);
    planer.GenerateExecutePlan(&task_plan_proto);

    delete select_query;
    return ConvertTaskPlanProtoToExecutor(task_plan_proto);
}

bool IsResultCorrect(Executor* executor,
                     LocalTabletScanner* local_scanner,
                     int d_factor) {
    io::Slice* exec_output;
    io::Slice* corrent_result;
    int count = 0;

    while (executor->NextSlice(&exec_output)) {
        if (count % d_factor == 0) {
            if (!local_scanner->NextSlice(&corrent_result)) {
                return false;
            }
        }

        EXPECT_EQ(corrent_result->GetCount(),
                  exec_output->GetCount()) <<
            SliceDebugInfo(corrent_result, exec_output);
        if (corrent_result->GetCount() != exec_output->GetCount()) {
            return false;
        }

        EXPECT_EQ(corrent_result->GetSelectLevel(),
                  exec_output->GetSelectLevel()) <<
            SliceDebugInfo(corrent_result, exec_output);
        if (corrent_result->GetSelectLevel() !=
            exec_output->GetSelectLevel()) {
            return false;
        }

        EXPECT_TRUE(IsSliceEqual(corrent_result, exec_output)) <<
            SliceDebugInfo(corrent_result, exec_output);
        if (!IsSliceEqual(corrent_result, exec_output)) {
            return false;
        }
        count++;
    }

    EXPECT_FALSE(local_scanner->NextSlice(&corrent_result));
    return true;
}

std::string DuplicateFile(const std::string& f, int d_factor) {
    std::string files;

    for (int i = 0; i < d_factor; ++i) {
        files = files + f + kTabletSpliter;
    }
    return files;
}

TEST(ParallelPlannerTest, union_test) {
    const int kDuplicateFactor = 1;
    const char* query =
        " SELECT "
        " student_id, grade, department_id, score "
        " FROM Student "
        " WHERE student_id >= 0 ";
    FLAGS_gunir_compiler_leaf_plan_limit = 2;
    FLAGS_gunir_compiler_inter_plan_limit = 2;

    Executor* executor = ConvertQueryToExecutor(
        query, kStudentProtoFile,
        DuplicateFile(kStudentInputData, kDuplicateFactor));
    ASSERT_NE(static_cast<Executor*>(NULL), executor);

    std::vector<std::string> query_result_columns;
    query_result_columns.push_back("Student.student_id");
    query_result_columns.push_back("Student.grade");
    query_result_columns.push_back("Student.department_id");
    query_result_columns.push_back("Student.score");

    LocalTabletScanner local_scanner;
    ASSERT_TRUE(local_scanner.Init(
            kStudentProtoFile,
            kStudentInputData,
            query_result_columns));

    ASSERT_TRUE(IsResultCorrect(executor, &local_scanner, kDuplicateFactor));

    for (size_t i = 0; i < g_all_scanners.size(); ++i) {
        delete g_all_scanners[i];
    }
    g_all_scanners.clear();
    delete executor;
}

TEST(ParallelPlannerTest, sort_test) {
    const int kDuplicateFactor = 3;
    const char* query =
        " SELECT "
        " student_id, grade, department_id, score "
        " FROM Student "
        " WHERE student_id >= 0 "
        " ORDERBY student_id, grade DESC, department_id ASC, score;";
    FLAGS_gunir_compiler_leaf_plan_limit = 2;
    FLAGS_gunir_compiler_inter_plan_limit = 2;

    Executor* executor = ConvertQueryToExecutor(
        query, kStudentProtoFile,
        DuplicateFile(kStudentInputData, kDuplicateFactor));
    ASSERT_NE(static_cast<Executor*>(NULL), executor);

    std::vector<std::string> query_result_columns;
    query_result_columns.push_back("Student.student_id");
    query_result_columns.push_back("Student.grade");
    query_result_columns.push_back("Student.department_id");
    query_result_columns.push_back("Student.score");

    LocalTabletScanner local_scanner;
    ASSERT_TRUE(local_scanner.Init(
            SchemaBuilder::kQueryResultMessageName,
            kStudentOutputData,
            query_result_columns));

    ASSERT_TRUE(IsResultCorrect(executor, &local_scanner, kDuplicateFactor));

    for (size_t i = 0; i < g_all_scanners.size(); ++i) {
        delete g_all_scanners[i];
    }
    g_all_scanners.clear();
    delete executor;
}

TEST(ParallelPlannerTest, aggregate_test) {
    const char* query =
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
        " AVG(student_age) + MAX(student_age) * "
        " MIN(student_age) - SUM(student_age) AS ALL_AGE "
        " FROM Department "
        " GROUPBY department_id, institute_id, "
        " cnt_student, sum_id, avg_id, sum_age, avg_age, max_age, min_age "
        " HAVING sum_id > 0 "
        " LIMIT 10, 100";
    static const int kAggStart = 2;
    static const int kAggNumber = 7;

    static const char* kDepartmentProto =
        "./testdata/parallel_planner_test/Department.proto";
    static const char* kAggregateInputData =
        "./testdata/parallel_planner_test/aggregate_test_src_0";

    FLAGS_gunir_compiler_leaf_plan_limit = 2;
    FLAGS_gunir_compiler_inter_plan_limit = 2;
    const int kDuplicateFactor = 6;

    Executor* executor = ConvertQueryToExecutor(
        query, kDepartmentProto,
        DuplicateFile(kAggregateInputData, kDuplicateFactor));
    ASSERT_NE(static_cast<Executor*>(NULL), executor);

    io::Slice* executor_output_slice;
    while (executor->NextSlice(&executor_output_slice)) {
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
                    (*(reinterpret_cast<const double*> (b1->GetValue().data())));
            }
            if (v1 == v2 || v1 * kDuplicateFactor == v2) {
                continue;
            }

            LOG(ERROR) << "not equal";
            ASSERT_EQ(v1 * kDuplicateFactor, v2);
        }
    }
    for (size_t i = 0; i < g_all_scanners.size(); ++i) {
        delete g_all_scanners[i];
    }
    g_all_scanners.clear();
    delete executor;
}

} // namespace compiler
} // namespace gunir

