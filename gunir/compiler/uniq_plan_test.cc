// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <cstdio>

#include "toft/base/string/number.h"
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

class MockUniqDocumentScanner : public io::Scanner {
public:
    explicit MockUniqDocumentScanner(size_t block_number)
        : m_returned(0),
        m_block_helper(FLAGS_gunir_compiler_parallel_buffer_size) {
            m_slice.reset(new io::Slice(block_number));
    }

    bool NextSlice(io::Slice** slice) {

        if (m_returned >= kMaxReturnNumber) {
            return false;
        }

        *slice = m_slice.get();

        io::Block block;
        // userid
        block.SetRepLevel(0);
        block.SetDefLevel(0);

        int64_t userid = m_returned%kMod;
        if (!m_block_helper.SetBlockValue(
                &block, io::Block::TYPE_INT64,
                reinterpret_cast<const char*>(&userid),
                io::Block::kValueTypeLength[io::Block::TYPE_INT64])) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }
        m_slice->SetHasBlock(0U, true);
        *((*slice)->MutableBlock(0U)) = block;

        // info.name
        block.SetRepLevel(0);
        block.SetDefLevel(0);
        std::string v_str;

        if (userid == 1) {
            v_str = "WangKun" + NumberToString(3);
        } else {
            v_str = "WangKun" + NumberToString(m_returned%2);
        }

        if (!m_block_helper.SetBlockValue(
                &block,
                io::Block::TYPE_STRING,
                v_str.c_str(),
                v_str.length())) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }

        m_slice->SetHasBlock(1U, true);
        *((*slice)->MutableBlock(1U)) = block;


        // nick not null
        block.SetRepLevel(0);
        block.SetDefLevel(1);
        v_str = "cat";
        if (!m_block_helper.SetBlockValue(
                &block, io::Block::TYPE_STRING,
                v_str.c_str(),
                v_str.length())) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }
        m_slice->SetHasBlock(2U, true);
        *((*slice)->MutableBlock(2U)) = block;

        m_returned++;
        return true;
    }

private:
    int64_t m_returned;
    toft::scoped_ptr<io::Slice> m_slice;
    static const int64_t kMaxReturnNumber = 10;
    static const int64_t kMod = 3;
    io::BlockHelper m_block_helper;
};

std::vector<io::Scanner*> g_all_scanners;

std::vector<TableInfo> GetTableInfos(std::vector<std::string> tables) {
    std::vector<TableInfo> table_infos;

    for (size_t i = 0; i < tables.size(); ++i) {
        TableSchema schema;
        std::string table_proto =
            std::string("testdata/") + tables[i] + ".proto";

        LOG(ERROR) << "table_proto: " << table_proto;
        schema.InitSchemaFromProtoFile(table_proto, tables[i]);
        std::string schema_string = schema.GetTableSchemaString();

        TableInfo info;

        info.set_table_name(tables[i]);
        info.set_message_name(tables[i]);
        info.set_table_schema(schema_string);

        std::string* file1 = (info.mutable_tablets())->Add()->mutable_name();
        *file1 = "some/file1";

        table_infos.push_back(info);
    }
    return table_infos;
}

SelectQuery AnalyzeSelectQuery(const char* query) {
    QueryStmt* query_stmt_ptr;

    CHECK_EQ(0, parse_line(query, &query_stmt_ptr));
    toft::scoped_ptr<QueryStmt> query_stmt(query_stmt_ptr);

    SelectQuery select_query(query_stmt->select());

    std::vector<std::string> tables = select_query.GetQueryTables();
    std::vector<TableInfo> table_infos = GetTableInfos(tables);

    CHECK(select_query.Init(table_infos));

    CHECK(select_query.Analyze());

    return select_query;
}

Executor* ConvertTaskToExecutor(
    const TaskPlanProto& proto,
    size_t col_num) {

    std::vector<io::Scanner*> sub_scanners;

    for (int i = 0; i < proto.sub_task_plan_list_size(); ++i) {
        Executor* sub_executor = ConvertTaskToExecutor(
            proto.sub_task_plan_list(i),
            col_num);
        sub_scanners.push_back(sub_executor);
    }

    Executor* executor = new Executor();

    bool is_succeed = executor->Init(proto.exec_plan(),
                                     proto.table_schema_string(),
                                     proto.table_message_name());
    CHECK(is_succeed);
    if (sub_scanners.size() == 0) {
        sub_scanners.push_back(new MockUniqDocumentScanner(col_num));
    }

    executor->SetScanner(sub_scanners);
    g_all_scanners.insert(g_all_scanners.end(),
                          sub_scanners.begin(), sub_scanners.end());
    return executor;
}

Executor* ConvertQueryToExecutor(const char* query) {
    SelectQuery select_query = AnalyzeSelectQuery(query);
    // Parallel plan
    TaskPlanProto task_plan_proto;
    ParallelPlanner planer(select_query);
    CHECK(planer.GenerateExecutePlan(&task_plan_proto));
    const std::vector<std::shared_ptr<TableEntry> >& entrys =
        select_query.GetTableEntry();

    CHECK_EQ(1U, entrys.size());

    size_t col_num = entrys[0]->GetAffectedColumnInfo().size();

    return ConvertTaskToExecutor(task_plan_proto,
                                 col_num);
}

TEST(UniqPlanPlainTest, uniq_test) {
    const char* query = "SELECT userid, distinct info.name, nick from UniqDocument "
        "WHERE userid >= 1 "
        "ORDERBY userid "
        "LIMIT 10";

    FLAGS_gunir_compiler_leaf_plan_limit = 2;
    FLAGS_gunir_compiler_inter_plan_limit = 2;

    Executor* executor = ConvertQueryToExecutor(query);

    ASSERT_NE(static_cast<Executor*>(NULL), executor);

    io::Slice* executor_output_slice;
    size_t record = 0;
    while (executor->NextSlice(&executor_output_slice)) {
        // LOG(ERROR) << executor_output_slice->DebugString();
        record++;
    }
    ASSERT_EQ(3U, record);

    for (size_t i = 0; i < g_all_scanners.size(); ++i) {
        delete g_all_scanners[i];
    }
    g_all_scanners.clear();
    delete executor;
}

TEST(UniqPlanAggregateTest, uniq_test) {

    const char* query = "SELECT userid, count(userid), "
        "count(distinct info.name), nick from UniqDocument "
        "WHERE userid <= 1 "
        "GROUPBY userid, nick "
        "ORDERBY userid "
        "LIMIT 10";

    FLAGS_gunir_compiler_leaf_plan_limit = 2;
    FLAGS_gunir_compiler_inter_plan_limit = 2;

    Executor* executor = ConvertQueryToExecutor(query);

    ASSERT_NE(static_cast<Executor*>(NULL), executor);

    io::Slice* executor_output_slice;
    const io::Block* block;
    size_t record = 0;
    int64_t v;

    while (executor->NextSlice(&executor_output_slice)) {
        // LOG(ERROR) << executor_output_slice->DebugString();
        record++;
        block = executor_output_slice->GetBlock(0);
        v = *(reinterpret_cast<const int64_t*> (block->GetValue().data()));
        if (record == 1)
            EXPECT_EQ(v, 0);
        else
            EXPECT_EQ(v, 1);

        block = executor_output_slice->GetBlock(1);
        v = *(reinterpret_cast<const int64_t*> (block->GetValue().data()));

        if (record == 1)
            EXPECT_EQ(v, 4);
        else
            EXPECT_EQ(v, 3);

        block = executor_output_slice->GetBlock(2);
        v = *(reinterpret_cast<const int64_t*> (block->GetValue().data()));

        if (record == 1)
            EXPECT_EQ(v, 2);
        else
            EXPECT_EQ(v, 1);
    }

    ASSERT_EQ(2U, record);

    for (size_t i = 0; i < g_all_scanners.size(); ++i) {
        delete g_all_scanners[i];
    }
    g_all_scanners.clear();
    delete executor;
}

} // namespace compiler
} // namespace gunir

