// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/parser/table_schema.h"

#include "thirdparty/gtest/gtest.h"

namespace gunir {
namespace compiler {

typedef ::google::protobuf::FieldDescriptor PBFieldDescriptor;

const char* test_proto_file =
    "testdata/table_schema_test/table_schema_test.proto";
const char* test_message = "Document";

std::vector<std::string> GetPlainOKTestPath();
std::vector<std::string> GetPlainNotOKTestPath();
std::vector<std::string> GetGroupOKTestPath();
std::vector<std::string> GetGroupNotOKTestPath();

TEST(GetStaticInfoTest, OK) {
    TableSchema schema;
    ASSERT_TRUE(schema.InitSchemaFromProtoFile(test_proto_file, test_message));

    std::vector<ColumnInfo> static_info = schema.GetAllColumnInfo();

    ASSERT_EQ(5U, static_info.size());

    ASSERT_EQ(2U, static_info[0].m_repeat_level);
    ASSERT_EQ(3U, static_info[0].m_define_level);
    ASSERT_EQ(BigQueryType::STRING, static_info[0].m_type);
    ASSERT_EQ("Document.header.meta.title",
              static_info[0].m_column_path_string);
    ASSERT_EQ(3U, static_info[1].m_column_path.size());

    ASSERT_EQ(2U, static_info[1].m_repeat_level);
    ASSERT_EQ(2U, static_info[1].m_define_level);
    ASSERT_EQ(BigQueryType::INT32, static_info[1].m_type);
    ASSERT_EQ("Document.header.meta.outline",
              static_info[1].m_column_path_string);
    ASSERT_EQ(3U, static_info[1].m_column_path.size());
}

TEST(GetTypeTest, OK) {
    TableSchema schema;
    ASSERT_TRUE(schema.InitSchemaFromProtoFile(test_proto_file, test_message));

    BQType type;
    ASSERT_TRUE(schema.GetFieldType(GetPlainOKTestPath(), &type));
    ASSERT_EQ(BigQueryType::STRING, type);
}

TEST(GetColumnInfoTest, OK) {
    TableSchema schema;
    ColumnInfo info;
    ASSERT_TRUE(schema.InitSchemaFromProtoFile(test_proto_file, test_message));

    ASSERT_TRUE(schema.GetColumnInfo(GetPlainOKTestPath(), &info));

    ASSERT_EQ(BigQueryType::STRING, info.m_type);
    ASSERT_EQ(PBFieldDescriptor::LABEL_OPTIONAL, info.m_label);
    ASSERT_EQ(1, info.m_column_number);
    ASSERT_EQ(2U, info.m_repeat_level);
}

TEST(GetTypeTest, notOK) {
    TableSchema schema;
    ASSERT_TRUE(schema.InitSchemaFromProtoFile(test_proto_file, test_message));

    BQType type;
    ASSERT_FALSE(schema.GetFieldType(GetPlainNotOKTestPath(), &type));

    // should not get BQType for group field
    ASSERT_FALSE(schema.GetFieldType(GetGroupOKTestPath(), &type));
}

TEST(GetLabelTest, OK) {
    TableSchema schema;
    ASSERT_TRUE(schema.InitSchemaFromProtoFile(test_proto_file, test_message));

    PBLabel label;
    ASSERT_TRUE(schema.GetFieldLabel(GetPlainOKTestPath(), &label));
    ASSERT_EQ(PBFieldDescriptor::LABEL_OPTIONAL, label);

    ASSERT_TRUE(schema.GetFieldLabel(GetGroupOKTestPath(), &label));
    ASSERT_EQ(PBFieldDescriptor::LABEL_REPEATED, label);
}

TEST(GetLabelTest, notOK) {
    TableSchema schema;
    ASSERT_TRUE(schema.InitSchemaFromProtoFile(test_proto_file, test_message));

    PBLabel label;
    ASSERT_FALSE(schema.GetFieldLabel(GetPlainNotOKTestPath(), &label));
    ASSERT_FALSE(schema.GetFieldLabel(GetGroupNotOKTestPath(), &label));
}

std::vector<std::string> GetPlainOKTestPath() {
    std::vector<std::string> path;

    path.push_back(std::string("header"));
    path.push_back(std::string("meta"));
    path.push_back(std::string("title"));

    return path;
}

std::vector<std::string> GetGroupOKTestPath() {
    std::vector<std::string> path;

    path.push_back(std::string("header"));
    path.push_back(std::string("meta"));
    return path;
}

std::vector<std::string> GetPlainNotOKTestPath() {
    std::vector<std::string> path;

    path.push_back(std::string("header"));
    path.push_back(std::string("meta"));
    path.push_back(std::string("title_error"));
    return path;
}

std::vector<std::string> GetGroupNotOKTestPath() {
    std::vector<std::string> path;

    path.push_back(std::string("header"));
    path.push_back(std::string("meta_error"));
    path.push_back(std::string("title"));
    return path;
}

} // namespace compiler
} // namespace gunir

