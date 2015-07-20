// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
#include <string>
#include <vector>

#include "thirdparty/gtest/gtest.h"

#include "gunir/proto/table.pb.h"
#include "gunir/compiler/job_plan.h"
#include "gunir/compiler/parser/big_query_parser.h"
#include "gunir/compiler/schema_builder.h"
#include "gunir/compiler/select_query.h"

#include "thirdparty/protobuf/descriptor.h"
#include "thirdparty/protobuf/descriptor.pb.h"

namespace gunir {
namespace compiler {

typedef ::google::protobuf::Descriptor PBDescriptor;
typedef ::google::protobuf::DescriptorPool PBDescriptorPool;
typedef ::google::protobuf::DescriptorProto PBDescriptorProto;
typedef ::google::protobuf::FieldDescriptor PBFieldDescriptor;
typedef ::google::protobuf::FieldDescriptorProto PBFieldDescriptorProto;
typedef ::google::protobuf::FileDescriptor PBFileDescriptor;
typedef ::google::protobuf::FileDescriptorProto PBFileDescriptorProto;

std::vector<TableInfo> GetTableInfos(std::vector<std::string> tables) {
    std::vector<TableInfo> table_infos;

    for (size_t i = 0; i < tables.size(); ++i) {
        TableSchema schema;
        std::string table_proto =
            std::string("testdata/") + tables[i] + ".proto";

        schema.InitSchemaFromProtoFile(table_proto, tables[i]);
        const std::string schema_string = schema.GetTableSchemaString();

        TableInfo info;

        info.set_table_name(tables[i]);
        info.set_message_name(tables[i]);
        info.set_table_schema(schema_string);
        std::string* file1 = (info.mutable_tablets())->Add()->mutable_name();
        *file1 = "some/file";

        table_infos.push_back(info);
    }
    return table_infos;
}

TEST(JobPlanTest, test_method) {
    const char* query = "SELECT docid, name.url FROM Document where docid > 3;";
    QueryStmt* query_stmt;

    ASSERT_EQ(0, parse_line(query, &query_stmt));

    SelectQuery select_query(query_stmt->select());
    std::vector<std::string> tables = select_query.GetQueryTables();
    std::vector<TableInfo> table_infos = GetTableInfos(tables);

    ASSERT_TRUE(select_query.Init(table_infos));
    ASSERT_TRUE(select_query.Analyze());

    JobPlan job_plan;
    job_plan.Init(select_query);
    job_plan.GetTaskPlan();

    delete query_stmt;
}

} // namespace compiler
} // namespace gunir
