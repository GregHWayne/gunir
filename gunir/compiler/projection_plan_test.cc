// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <cstdio>
#include <string>

#include "thirdparty/gtest/gtest.h"

#include "gunir/compiler/compiler_test_helper.h"
#include "gunir/compiler/projection_plan.h"

namespace gunir {
namespace compiler {

std::string SliceDebugInfo(const io::Slice* s1, const io::Slice* s2);
bool IsSliceEqual(io::Slice* slice1, io::Slice* slice2);

bool IsSliceEqualWithoutDLevel(io::Slice* slice1, io::Slice* slice2) {

    if (slice1->GetCount() !=  slice2->GetCount()) {
        return false;
    }
    for (size_t i = 0; i < slice1->GetCount(); ++i) {
        if (slice1->HasBlock(i) != slice2->HasBlock(i)) {
            LOG(ERROR) << "Has Block Not Equal";
            return false;
        }

        if (!slice1->HasBlock(i)) {
            continue;
        }

        const io::Block *b1, *b2;
        b1 = slice1->GetBlock(i);
        b2 = slice2->GetBlock(i);

        if (b1->IsNull() != b2->IsNull() ||
            b1->GetRepLevel() != b2->GetRepLevel()) {
            LOG(ERROR) << "level not equal";
            return false;
        }

        if (b1->IsNull()) {
            continue;
        }

        EXPECT_EQ(b1->GetValue(), b2->GetValue());
    }
    return true;
}

TEST(ProjectionPlanTest, projection_with_all_test) {
    const char* query = "SELECT docid, links.backward, links.forward, "
                        "name.url, name.language.code, name.language.country "
                        "FROM Document ;";

    Executor executor;
    std::vector<io::Scanner*> scanners;

    bool result =
        ProcessQuery(query,
                     "./testdata/projection_test/Document.proto",
                     "./testdata/projection_test/projection_test_src_0",
                     &executor,
                     &scanners);

    ASSERT_TRUE(result);

    LocalTabletScanner local_scanner;
    std::vector<std::string> query_result_columns;
    query_result_columns.push_back("Document.docid");
    query_result_columns.push_back("Document.links.backward");
    query_result_columns.push_back("Document.links.forward");
    query_result_columns.push_back("Document.name.url");
    query_result_columns.push_back("Document.name.language.code");
    query_result_columns.push_back("Document.name.language.country");

    ASSERT_TRUE(local_scanner.Init(
            "Document",
            "./testdata/projection_test/projection_test_src_0",
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

        // cause the d_level will change in result schema temporary
        ASSERT_TRUE(IsSliceEqualWithoutDLevel(right_result_slice,
                                              executor_output_slice))
            << SliceDebugInfo(right_result_slice, executor_output_slice);
    }

    ASSERT_FALSE(local_scanner.NextSlice(&right_result_slice));
    for (size_t i = 0; i < scanners.size(); ++i) {
        delete scanners[i];
    }
}


TEST(ProjectionPlanTest, projection_with_filter_test) {
    const char* query =
        " SELECT "
        " docid, name.url, name.language.code, name.language.country "
        " FROM Document "
        " WHERE "
        " docid % 2 == 0 AND "
        " (name.url CONTAINS 'C' OR name.url CONTAINS 'D') AND "
        " LENGTH(name.language.country) > 3;";

    Executor executor;
    std::vector<io::Scanner*> scanners;

    bool result =
        ProcessQuery(query,
                     "./testdata/projection_test/Document.proto",
                     "./testdata/projection_test/projection_test_src_0",
                     &executor,
                     &scanners);
    ASSERT_TRUE(result);

    LocalTabletScanner local_scanner;
    std::vector<std::string> query_result_columns;
    query_result_columns.push_back("Document.docid");
    query_result_columns.push_back("Document.name.url");
    query_result_columns.push_back("Document.name.language.code");
    query_result_columns.push_back("Document.name.language.country");

    ASSERT_TRUE(local_scanner.Init(
            "QueryResult",
            "./testdata/projection_test/projection_test_result_0",
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


TEST(ProjectionPlanTest, all_data_type_test) {
    const char* query =
        "SELECT * FROM AllDataType;";

    Executor executor;
    std::vector<io::Scanner*> scanners;

    bool result =
        ProcessQuery(query,
                     "./testdata/datatype_test/AllDataType.proto",
                     "./testdata/datatype_test/datatype_test_src_0",
                     &executor,
                     &scanners);
    ASSERT_TRUE(result);

    LocalTabletScanner local_scanner;
    std::vector<std::string> query_result_columns;
    query_result_columns.push_back("QueryResult._double");
    query_result_columns.push_back("QueryResult._float");
    query_result_columns.push_back("QueryResult._int32");
    query_result_columns.push_back("QueryResult._int64");
    query_result_columns.push_back("QueryResult._uint32");
    query_result_columns.push_back("QueryResult._uint64");
    query_result_columns.push_back("QueryResult._sint32");
    query_result_columns.push_back("QueryResult._sint64");
    query_result_columns.push_back("QueryResult._fixed32");
    query_result_columns.push_back("QueryResult._fixed64");
    query_result_columns.push_back("QueryResult._sfixed32");
    query_result_columns.push_back("QueryResult._sfixed64");
    query_result_columns.push_back("QueryResult._bool");
    query_result_columns.push_back("QueryResult._string");
    query_result_columns.push_back("QueryResult._bytes");

    ASSERT_TRUE(local_scanner.Init(
            "QueryResult",
            "./testdata/datatype_test/datatype_test_result_0",
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

