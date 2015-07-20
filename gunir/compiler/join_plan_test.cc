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

class MockJoinRightDocumentScanner : public io::Scanner {
public:
    explicit MockJoinRightDocumentScanner(size_t block_number)
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

        int64_t userid = m_returned%5;
        if (!m_block_helper.SetBlockValue(
                &block, io::Block::TYPE_INT64,
                reinterpret_cast<const char*>(&userid),
                io::Block::kValueTypeLength[io::Block::TYPE_INT64])) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }
        m_slice->SetHasBlock(0U, true);
        *((*slice)->MutableBlock(0U)) = block;

        // info.age when record is even ,value is null
        block.SetRepLevel(0);
        block.SetDefLevel(1);
        int64_t age = m_returned;

        if (userid == 3) {
            age = -1;
        }

        if (!m_block_helper.SetBlockValue(
                &block, io::Block::TYPE_INT64,
                reinterpret_cast<const char*>(&age),
                io::Block::kValueTypeLength[io::Block::TYPE_INT64])) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }

        m_slice->SetHasBlock(1U, true);
        *((*slice)->MutableBlock(1U)) = block;


        // info.name
        block.SetRepLevel(0);
        block.SetDefLevel(0);
        if (!m_block_helper.SetBlockValue(
                &block,
                io::Block::TYPE_STRING,
                "WangKun",
                7)) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }
        m_slice->SetHasBlock(2U, true);
        *((*slice)->MutableBlock(2U)) = block;


        // nick not null
        block.SetRepLevel(0);
        block.SetDefLevel(1);
        std::string v_str = "cat_" + NumberToString(m_returned);
        if (!m_block_helper.SetBlockValue(
                &block, io::Block::TYPE_STRING,
                v_str.c_str(),
                v_str.length())) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }
        m_slice->SetHasBlock(3U, true);
        *((*slice)->MutableBlock(3U)) = block;


        // table_type
        block.SetRepLevel(0);
        block.SetDefLevel(0);
        v_str = "right_" + NumberToString(m_returned);
        if (!m_block_helper.SetBlockValue(
                &block, io::Block::TYPE_STRING,
                v_str.c_str(),
                v_str.length())) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }
        m_slice->SetHasBlock(4U, true);
        *((*slice)->MutableBlock(4U)) = block;

        m_returned++;
        return true;
    }

private:
    int64_t m_returned;
    toft::scoped_ptr<io::Slice> m_slice;
    static const int64_t kMaxReturnNumber = 10;
    io::BlockHelper m_block_helper;
};

class MockJoinLeftDocumentScanner : public io::Scanner {
public:
    explicit MockJoinLeftDocumentScanner(size_t block_number)
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
        if (!m_block_helper.SetBlockValue(
                &block, io::Block::TYPE_INT64,
                reinterpret_cast<const char*>(&m_returned),
                io::Block::kValueTypeLength[io::Block::TYPE_INT64])) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }
        m_slice->SetHasBlock(0U, true);
        *((*slice)->MutableBlock(0U)) = block;

        // info.name
        block.SetRepLevel(0);
        block.SetDefLevel(0);
        if (!m_block_helper.SetBlockValue(
                &block,
                io::Block::TYPE_STRING,
                "WangKun",
                7)) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }
        m_slice->SetHasBlock(1U, true);
        *((*slice)->MutableBlock(1U)) = block;


        // info.age when record is even ,value is null
        block.SetRepLevel(0);
        block.SetDefLevel(1);
        if (m_returned >= 0) {
            if (!m_block_helper.SetBlockValue(
                    &block, io::Block::TYPE_INT64,
                    reinterpret_cast<const char*>(&m_returned),
                    io::Block::kValueTypeLength[io::Block::TYPE_INT64])) {
                LOG(ERROR) << "buffer size is too small";
                return false;
            }
        } else {
            block.SetValueType(io::Block::TYPE_NULL);
        }
        m_slice->SetHasBlock(2U, true);
        *((*slice)->MutableBlock(2U)) = block;


        // nick not null
        block.SetRepLevel(0);
        block.SetDefLevel(1);
        std::string v_str = "cat_" + NumberToString(m_returned);
        if (!m_block_helper.SetBlockValue(
                &block, io::Block::TYPE_STRING,
                v_str.c_str(),
                v_str.length())) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }
        m_slice->SetHasBlock(3U, true);
        *((*slice)->MutableBlock(3U)) = block;


        // table_type
        block.SetRepLevel(0);
        block.SetDefLevel(0);
        v_str = "left_" + NumberToString(m_returned);
        if (!m_block_helper.SetBlockValue(
                &block, io::Block::TYPE_STRING,
                v_str.c_str(),
                v_str.length())) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }
        m_slice->SetHasBlock(4U, true);
        *((*slice)->MutableBlock(4U)) = block;

        m_returned++;
        return true;
    }

private:
    int64_t m_returned;
    toft::scoped_ptr<io::Slice> m_slice;
    static const int64_t kMaxReturnNumber = 10;
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

Executor* ConvertJoinTaskToExecutor(
    const TaskPlanProto& proto,
    size_t t1_col_num,
    size_t t2_col_num) {

    std::vector<io::Scanner*> sub_scanners;

    for (int i = 0; i < proto.sub_task_plan_list_size(); ++i) {
        Executor* sub_executor = ConvertJoinTaskToExecutor(
            proto.sub_task_plan_list(i),
            t1_col_num,
            t2_col_num);
        sub_scanners.push_back(sub_executor);
    }

    Executor* executor = new Executor();

    bool is_succeed = executor->Init(proto.exec_plan(),
                                     proto.table_schema_string(),
                                     proto.table_message_name());
    CHECK(is_succeed);
    if (sub_scanners.size() == 0) {
        sub_scanners.push_back(new MockJoinLeftDocumentScanner(t1_col_num));

        sub_scanners.push_back(new MockJoinRightDocumentScanner(t2_col_num));
    }

    executor->SetScanner(sub_scanners);
    g_all_scanners.insert(g_all_scanners.end(),
                          sub_scanners.begin(), sub_scanners.end());
    return executor;
}

Executor* ConvertJoinQueryToExecutor(const char* query) {
    SelectQuery select_query = AnalyzeSelectQuery(query);
    // Parallel plan
    TaskPlanProto task_plan_proto;
    ParallelPlanner planer(select_query);
    CHECK(planer.GenerateExecutePlan(&task_plan_proto));
    const std::vector<std::shared_ptr<TableEntry> >& entrys =
        select_query.GetTableEntry();

    CHECK_EQ(2U, entrys.size());

    size_t t1_col_num = entrys[0]->GetAffectedColumnInfo().size();
    size_t t2_col_num = entrys[1]->GetAffectedColumnInfo().size();

    return ConvertJoinTaskToExecutor(task_plan_proto,
                                     t1_col_num,
                                     t2_col_num);
}

TEST(JoinPlanAggregateTest, join_test) {

    const char* query =
        "SELECT t1.userid as userid_left , count(t2.userid) as userid_right, "
        "count(t1.info.name) as name_left, count(t2.info.name) as name_right, "
        "count(t1.info.age) as age_left, count(t2.info.age) as age_right, "
        "count(t1.nick) as nick_left, count(t2.nick) as nick_right, "
        "count(t1.table_type) as type_left, count(t2.table_type) as type_right "
        "FROM DocumentJoinLeft as t1 where userid <= 6 "
        "LEFT OUTER JOIN DocumentJoinRight as t2 ON "
        "t1.userid=t2.userid and t2.info.age >= 2 "
        "groupby userid_left "
        "orderby userid_left DESC Limit 8;";


    FLAGS_gunir_compiler_leaf_plan_limit = 2;
    FLAGS_gunir_compiler_inter_plan_limit = 2;

    Executor* executor = ConvertJoinQueryToExecutor(query);

    ASSERT_NE(static_cast<Executor*>(NULL), executor);

    io::Slice* executor_output_slice;
    size_t record = 0;
    while (executor->NextSlice(&executor_output_slice)) {
        record++;
    }

    ASSERT_EQ(7U, record);

    for (size_t i = 0; i < g_all_scanners.size(); ++i) {
        delete g_all_scanners[i];
    }
    g_all_scanners.clear();
    delete executor;
}

/*
 * schema :
 *    message Info{
 *      required string name = 1;
 *      optional int64 age;
 *    }
 *
 *    message Friends {
 *      required Info info = 1;
 *    }
 *
 *    message DocumentJoinRight {
 *       required int64 userid = 1;
 *       required Info info = 2;
 *       optional string nick = 3;
 *       repeated Friends friends = 4;
 *       required string table_type = 5;
 *    }

 * before filter
 * Left userid
 * 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
 * Right userid
 * 0, 1, 2, 3, 4, 0, 1, 2, 3, 4
 *
 *
 * after filter Left userid <= 6
 * Left userid
 * 0, 1, 2, 3, 4, 5, 6
 *
 * Right userid
 * 0, 1, || 2, 3, 4, 0, 1, 2, 3, 4
 * sepcail when userid 3  age = -1
 *
 * both age 0-9
 *
 * after Join ON t1.userid=t2.userid and t2.info.age >= 2
 * Left userid  Right userid  LeftAge   RightAge
 * (0, 0 , 0, 5)
 * (1, 1, 1, 6)
 *
 * (2, 2, 2, 2)
 * (2, 2, 2, 7)
 *
 * (3, null, 3, null)
 *
 * (4, 4, 4, 4)
 * (4, 4, 4, 9)
 *
 * (5, null, 5, null)
 *
 * (6, null, 6, null)
 *
 */

TEST(JoinPlanPlainTest, join_test) {
    const char* query =
        "SELECT t1.userid as userid_left , t2.userid as userid_right, "
        "t1.info.name as name_left, t2.info.name as name_right, "
        "t1.info.age as age_left, t2.info.age as age_right, "
        "t1.nick as nick_left, t2.nick as nick_right, "
        "t1.table_type as type_left, t2.table_type as type_right "
        "FROM DocumentJoinLeft as t1 where userid <= 6 "
        "LEFT OUTER JOIN DocumentJoinRight as t2 ON "
        "t1.userid=t2.userid and t2.info.age >= 2 "
        "orderby userid_left DESC Limit 5;";

    FLAGS_gunir_compiler_leaf_plan_limit = 2;
    FLAGS_gunir_compiler_inter_plan_limit = 2;

    Executor* executor = ConvertJoinQueryToExecutor(query);

    ASSERT_NE(static_cast<Executor*>(NULL), executor);

    io::Slice* executor_output_slice;
    size_t record = 0;
    while (executor->NextSlice(&executor_output_slice)) {
        record++;
    }

    ASSERT_EQ(5U, record);

    for (size_t i = 0; i < g_all_scanners.size(); ++i) {
        delete g_all_scanners[i];
    }
    g_all_scanners.clear();
    delete executor;
}


} // namespace compiler
} // namespace gunir

