// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_SIMPLE_PLANNER_TEST_H
#define  GUNIR_COMPILER_SIMPLE_PLANNER_TEST_H

#include <cstdio>
#include <string>
#include <vector>

#include "toft/base/string/number.h"

#include "gunir/proto/table.pb.h"
#include "gunir/compiler/executor.h"
#include "gunir/compiler/parser/big_query_parser.h"
#include "gunir/compiler/schema_builder.h"
#include "gunir/compiler/select_query.h"
#include "gunir/compiler/simple_planner.h"
#include "gunir/io/block_helper.h"
#include "gunir/io/slice.h"

#include "thirdparty/gtest/gtest.h"
#include "thirdparty/protobuf/descriptor.h"
#include "thirdparty/protobuf/descriptor.pb.h"

DEFINE_int32(gunir_compiler_simple_test_buffer_size, 1024*1024,
             "size of write buffer");

namespace gunir {
namespace compiler {

typedef ::google::protobuf::Descriptor PBDescriptor;
typedef ::google::protobuf::DescriptorPool PBDescriptorPool;
typedef ::google::protobuf::DescriptorProto PBDescriptorProto;
typedef ::google::protobuf::FieldDescriptor PBFieldDescriptor;
typedef ::google::protobuf::FieldDescriptorProto PBFieldDescriptorProto;
typedef ::google::protobuf::FileDescriptor PBFileDescriptor;
typedef ::google::protobuf::FileDescriptorProto PBFileDescriptorProto;

std::vector<TableInfo> GetTableInfos(std::vector<std::string> tables);
std::vector<DatumBlock*> AllocDatumRow(const SelectQuery&);
void ReleaseDatumRow(const std::vector<DatumBlock*>& datum_row);

class MockDocumentScanner : public io::Scanner {
public:
    explicit MockDocumentScanner(size_t block_number)
        : m_returned(0),
        m_block_helper(FLAGS_gunir_compiler_simple_test_buffer_size) {
            m_slice.reset(new io::Slice(block_number));
        }

    bool NextSlice(io::Slice** slice) {
        static uint32_t s_url_rep_level[kMaxReturnNumber] =
        {0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0};
        static uint32_t s_code_country_rep_level[kMaxReturnNumber] =
        {0, 2, 2, 2, 0, 2, 1, 2, 2, 0, 0};
        static int64_t docid_seq[kMaxReturnNumber] =
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9};

        if (m_returned >= kMaxReturnNumber) {
            return false;
        }

        *slice = m_slice.get();

        io::Block block;
        // docid
        block.SetRepLevel(0);
        block.SetDefLevel(0);
        if (!m_block_helper.SetBlockValue(
                &block, io::Block::TYPE_INT64,
                reinterpret_cast<const char*>(&docid_seq[m_returned]),
                io::Block::kValueTypeLength[io::Block::TYPE_INT64])) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }


        m_slice->SetHasBlock(0U, true);
        *((*slice)->MutableBlock(0U)) = block;

        // url
        block.SetRepLevel(s_url_rep_level[m_returned]);
        block.SetDefLevel(2);

        if (!m_block_helper.SetBlockValue(
                &block,
                io::Block::TYPE_STRING,
                "Hello World",
                11)) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }

        m_slice->SetHasBlock(1U, true);
        *((*slice)->MutableBlock(1U)) = block;

        // code
        block.SetRepLevel(
            s_code_country_rep_level[m_returned]);
        block.SetDefLevel(2);

        std::string v_str = "code_" + NumberToString(m_returned);
        if (!m_block_helper.SetBlockValue(
                &block, io::Block::TYPE_STRING,
                v_str.c_str(),
                v_str.length())) {
            LOG(ERROR) << "buffer size is too small";
            return false;
        }

        m_slice->SetHasBlock(2U, true);
        *((*slice)->MutableBlock(2U)) = block;

        // country
        block.SetRepLevel(
            s_code_country_rep_level[m_returned]);

        m_slice->SetHasBlock(3U, true);
        v_str = "country_" + NumberToString(m_returned);
        if (m_returned % 2 != 0) {
            block.SetDefLevel(3);
            if (!m_block_helper.SetBlockValue(
                    &block, io::Block::TYPE_STRING,
                    v_str.c_str(),
                    v_str.length())) {
                LOG(ERROR) << "buffer size is too small";
                return false;
            }
        } else {
            block.SetValueType(io::Block::TYPE_NULL);
            block.SetDefLevel(1);
        }

        *((*slice)->MutableBlock(3U)) = block;
        // *slice = m_slice.get();

        m_returned++;
        return true;
    }

    int64_t m_returned;
    toft::scoped_ptr<io::Slice> m_slice;
    static const int64_t kMaxReturnNumber = 11;
    io::BlockHelper m_block_helper;
};

std::vector<DatumBlock*> AllocDatumRow(const SelectQuery& query) {
    std::vector<DatumBlock*> datum_row;
    std::string table_schema = query.GetResultSchema();

    TableSchema schema;
    CHECK(schema.InitSchemaFromFileDescriptorProto(
            table_schema, SchemaBuilder::kQueryResultMessageName))
        << "Executor init from schema failed";

    std::vector<ColumnInfo> column_infos = schema.GetAllColumnInfo();
    datum_row.resize(column_infos.size());
    for (size_t i = 0; i < column_infos.size(); i++)
        datum_row[i] = new DatumBlock(column_infos[i].m_type);
    return datum_row;
}

void ReleaseDatumRow(const std::vector<DatumBlock*>& datum_row) {
    for (size_t i = 0; i < datum_row.size(); i++)
        delete datum_row[i];
}

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

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_SIMPLE_PLANNER_TEST_H
