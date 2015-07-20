// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "thirdparty/gtest/gtest.h"

#include "gunir/proto/table.pb.h"
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

void GetTableSchema(TableSchema* result_schema, const SelectQuery& query) {
    std::string schema_string = query.GetResultSchema();
    ASSERT_TRUE(result_schema->InitSchemaFromFileDescriptorProto(
            schema_string, SchemaBuilder::kQueryResultMessageName));
}


std::vector<std::string> GetPath(const char* arg1 = NULL,
                                 const char* arg2 = NULL,
                                 const char* arg3 = NULL) {
    std::vector<std::string> path;

    if (arg1 != NULL) {
        path.push_back(arg1);
    } else {
        return path;
    }

    if (arg2 != NULL) {
        path.push_back(arg2);
    } else {
        return path;
    }

    if (arg3 != NULL) {
        path.push_back(arg3);
    }
    return path;
}

std::vector<TableInfo> GetTableInfos(std::vector<std::string> tables) {
    std::vector<TableInfo> table_infos;

    for (size_t i = 0; i < tables.size(); ++i) {
        TableSchema schema;
        std::string table_proto =
            std::string("testdata/analyze_query_test/") + tables[i] + ".proto";

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

#define ASSERT_ANALYZE_QUERY_NOT_OK(query) \
    do { \
        QueryStmt* query_stmt##_query; \
        ASSERT_EQ(0, parse_line(query, &query_stmt##_query)); \
        SelectQuery select_query(query_stmt##_query->select()); \
        std::vector<std::string> tables = select_query.GetQueryTables(); \
        std::vector<TableInfo> table_infos = GetTableInfos(tables); \
        ASSERT_TRUE(select_query.Init(table_infos)); \
        ASSERT_FALSE(select_query.Analyze()); \
        delete query_stmt##_query; \
    } while (0)


#define ASSERT_ANALYZE_QUERY_OK(query) \
    QueryStmt* query_stmt; \
    ASSERT_EQ(0, parse_line(query, &query_stmt)); \
    SelectQuery select_query(query_stmt->select()); \
    std::vector<std::string> tables = select_query.GetQueryTables(); \
    std::vector<TableInfo> table_infos = GetTableInfos(tables); \
    ASSERT_TRUE(select_query.Init(table_infos)); \
    ASSERT_TRUE(select_query.Analyze()); \
    delete query_stmt;


TEST(SelectQueryTest, simple_test) {
    const char* query = "SELECT docid, name.url FROM Document where docid > 3;";
    ASSERT_ANALYZE_QUERY_OK(query);
}

TEST(SelectQueryTest, within_test) {
    typedef std::shared_ptr<Target> ShrTarget;

    const char* query =
        "SELECT docid, "
        " SUM(LENGTH(name.url)) + 1 + AVG(docid) WITHIN RECORD AS count_url, "
        " MAX(LENGTH(name.language.code + ' ' + name.language.country)) "
        " WITHIN name.language AS count_cc, "
        " links.forward + 1 + links.backward as link, "
        " name.language.code "
        " FROM Document where docid > 3;";
    ASSERT_ANALYZE_QUERY_OK(query);

    // check targets
    std::vector<ShrTarget> targets = select_query.GetTargets();
    ASSERT_EQ(5U, targets.size());
    EXPECT_FALSE(targets[0]->HasWithin());

    EXPECT_EQ(0U, targets[1]->GetWithinLevel());
    EXPECT_TRUE(targets[1]->HasWithin());
    EXPECT_EQ(2U, targets[1]->GetAggregateFunctionExpression().size());
    EXPECT_EQ("Document", targets[1]->GetWithinTableName());
    EXPECT_EQ(0U, targets[1]->GetWithinMessagePath().size());

    EXPECT_EQ(2U, targets[2]->GetWithinLevel());
    EXPECT_TRUE(targets[2]->HasWithin());
    EXPECT_EQ(1U, targets[2]->GetAggregateFunctionExpression().size());
    EXPECT_EQ("Document", targets[2]->GetWithinTableName());
    EXPECT_EQ(2U, targets[2]->GetWithinMessagePath().size());
    EXPECT_EQ("name", targets[2]->GetWithinMessagePath().at(0));
    EXPECT_EQ("language", targets[2]->GetWithinMessagePath().at(1));

    EXPECT_FALSE(targets[3]->HasWithin());

    // check result schema
    TableSchema result_schema;
    GetTableSchema(&result_schema, select_query);
    ColumnInfo info;
    std::vector<std::string> path;

    path = GetPath("docid");
    ASSERT_TRUE(result_schema.GetColumnInfo(path, &info));

    path = GetPath("count_url");
    ASSERT_TRUE(result_schema.GetColumnInfo(path, &info));
    EXPECT_EQ(BigQueryType::DOUBLE, info.m_type);
    EXPECT_EQ(PBFieldDescriptor::LABEL_OPTIONAL, info.m_label);
    EXPECT_EQ(0U, info.m_repeat_level);
    EXPECT_EQ(1U, info.m_define_level);

    path = GetPath("name", "language", "count_cc");
    ASSERT_TRUE(result_schema.GetColumnInfo(path, &info));
    EXPECT_EQ(BigQueryType::UINT32, info.m_type);
    EXPECT_EQ(PBFieldDescriptor::LABEL_OPTIONAL, info.m_label);
    EXPECT_EQ(2U, info.m_repeat_level);
    EXPECT_EQ(3U, info.m_define_level);

    path = GetPath("name", "language", "code");
    ASSERT_TRUE(result_schema.GetColumnInfo(path, &info));

    path = GetPath("links", "link");
    ASSERT_TRUE(result_schema.GetColumnInfo(path, &info));

    ASSERT_EQ(targets.size(), result_schema.GetAllColumnInfo().size());
}

TEST(SelectQueryTest, within_test_not_exist_within_path) {
    const char* query =
        "SELECT docid, MAX(links.forward) WITHIN link_err as max_doc "
        " FROM Document where docid > 3;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query);
}

TEST(SelectQueryTest, within_test_should_not_within_data_field) {
    const char* query =
        "SELECT docid, MAX(docid) WITHIN docid as max_doc "
        " FROM Document where docid > 3;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query);
}

TEST(SelectQueryTest, within_test_affect_column_must_in_within_field) {
    const char* query =
        "SELECT docid, "
        " SUM(LENGTH(name.url)) + 1 + AVG(docid) WITHIN RECORD AS count_url, "
        " docid + MAX(LENGTH(name.language.code)) "
        " WITHIN name.language AS count_cc, " // err, docid not in name.language
        " links.forward + 1 + links.backward as link, "
        " name.language.code "
        " FROM Document where docid > 3;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query);
}

TEST(SelectQueryTest, wherecolumn_shouldnot_in_target) {
    const char* query =
        " SELECT docid + 1 as docid_1, name.url "
        " FROM Document WHERE docid_1 > 1;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query);
}

TEST(SelectQueryTest, simple_target_error_test) {
    const char* query =
        "SELECT docid_error, name.url FROM Document WHERE docid > 1;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query);
}

TEST(SelectQueryTest, target_is_star) {
    const char* query =
        "SELECT * FROM Document WHERE docid > 1;";
    ASSERT_ANALYZE_QUERY_OK(query);

    ASSERT_EQ(6U, select_query.GetTargets().size());
}

TEST(SelectQueryTest, target_should_be_data_field) {
    const char* query =
        "SELECT docid, name FROM Document WHERE docid > 1;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query);
}

TEST(SelectQueryTest, aggregate_should_not_nest) {
    const char* query =
        " SELECT docid, MAX(docid + MIN(docid)) WITHIN RECORD "
        " FROM Document WHERE docid > 1;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query);
}

TEST(SelectQueryTest, expr_default_alias) {
    const char* query =
        " SELECT docid + links.forward FROM Document;";
    ASSERT_ANALYZE_QUERY_OK(query);
}

TEST(SelectQueryTest, duplicate_alias) {
    const char* query =
        " SELECT docid, "
        " name.language.code as country, "
        " name.language.country "
        " FROM Document WHERE docid > 1;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query);
}

TEST(SelectQueryTest, duplicate_table_alias) {
    const char* query =
        " SELECT docid, "
        " name.language.code, "
        " name.language.country "
        " FROM Document AS DocId, DocId "
        " WHERE docid > 1;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query);
}

TEST(SelectQueryTest, where_type) {
    const char* query =
        " SELECT docid, CONCAT(name.url, 'abcd') as newurl"
        " FROM Document WHERE docid > 1 and name.url == 'nabcd';";
    ASSERT_ANALYZE_QUERY_OK(query);
}

TEST(SelectQueryTest, table_alias_test) {
    const char* query =
        " SELECT Document.docid, Doc.name.url "
        " FROM Document AS Doc WHERE docid > 1;";
    ASSERT_ANALYZE_QUERY_OK(query);
}

TEST(SelectQueryTest, simple_where_error_test) {
    const char* query =
        "SELECT docid, name.url FROM Document WHERE docid_error > 1;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query);
}

TEST(SelectQueryTest, where_return_type_error_test) {
    const char* query =
        "SELECT docid, name.url FROM Document WHERE docid + 1;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query);
}

TEST(SelectQueryTest, target_test) {
    const char* query =
        " SELECT "

        " docid, "
        " name.url, "
        " Document.links.forward, "
        " 1 AS const, "
        " docid + 1 AS newDocId, "
        " links.forward + 1 + links.backward AS link, "
        " SUBSTR(name.url, 0, 5) AS suburl, "
        " CONCAT(CONCAT(name.language.code, name.language.country), name.url) "
        " AS code_country_suburl "

        " FROM Document;";
    ASSERT_ANALYZE_QUERY_OK(query);
    // LOG(ERROR) << "result_schema\n:"
    // << select_query.GetReadableResultSchema();

    TableSchema result_schema;
    GetTableSchema(&result_schema, select_query);

    ColumnInfo info;
    std::vector<std::string> path;
    size_t target_number = 0;

    path = GetPath("docid");
    ASSERT_TRUE(result_schema.GetColumnInfo(path, &info));
    target_number++;

    path = GetPath("name", "url");
    ASSERT_TRUE(result_schema.GetColumnInfo(path, &info));
    EXPECT_EQ(BigQueryType::STRING, info.m_type);
    EXPECT_EQ(PBFieldDescriptor::LABEL_OPTIONAL, info.m_label);
    EXPECT_EQ(1, info.m_column_number);
    EXPECT_EQ(1U, info.m_repeat_level);
    EXPECT_EQ(2U, info.m_define_level);
    target_number++;

    path = GetPath("links", "forward");
    ASSERT_TRUE(result_schema.GetColumnInfo(path, &info));
    target_number++;

    path = GetPath("name", "url_error");
    ASSERT_FALSE(result_schema.GetColumnInfo(path, &info));

    path = GetPath("const");
    ASSERT_TRUE(result_schema.GetColumnInfo(path, &info));
    target_number++;

    path = GetPath("newDocId");
    ASSERT_TRUE(result_schema.GetColumnInfo(path, &info));
    target_number++;

    path = GetPath("links", "link");
    ASSERT_TRUE(result_schema.GetColumnInfo(path, &info));
    EXPECT_EQ(PBFieldDescriptor::LABEL_REPEATED, info.m_label);
    EXPECT_EQ(2, info.m_column_number);
    EXPECT_EQ(1U, info.m_repeat_level);
    EXPECT_EQ(2U, info.m_define_level);

    path = GetPath("links", "link", "value");
    ASSERT_TRUE(result_schema.GetColumnInfo(path, &info));
    EXPECT_EQ(BigQueryType::DOUBLE, info.m_type);
    EXPECT_EQ(PBFieldDescriptor::LABEL_OPTIONAL, info.m_label);
    EXPECT_EQ(1, info.m_column_number);
    EXPECT_EQ(1U, info.m_repeat_level);
    EXPECT_EQ(3U, info.m_define_level);
    target_number++;

    path = GetPath("name", "suburl");
    ASSERT_TRUE(result_schema.GetColumnInfo(path, &info));
    EXPECT_EQ(BigQueryType::STRING, info.m_type);
    target_number++;

    path = GetPath("name", "language", "code_country_suburl");
    ASSERT_TRUE(result_schema.GetColumnInfo(path, &info));
    EXPECT_EQ(BigQueryType::STRING, info.m_type);
    EXPECT_EQ(PBFieldDescriptor::LABEL_OPTIONAL, info.m_label);
    EXPECT_EQ(1, info.m_column_number);
    EXPECT_EQ(2U, info.m_repeat_level);
    EXPECT_EQ(3U, info.m_define_level);
    target_number++;

    const std::vector<std::shared_ptr<Target> >& targets
        = select_query.GetTargets();
    ASSERT_EQ(target_number, targets.size());
}

TEST(SelectQueryTest, where_clause) {
    const char* query =
        " SELECT "

        " docid, "
        " name.url, "
        " Document.links.forward, "
        " 1 AS const, docid + 1 AS newDocId, "
        " links.forward + 1 + links.backward AS link, "
        " CONCAT(name.language.code, name.language.country) AS code_country "

        " FROM Document "
        " WHERE "
        " name.url CONTAINS 'http://' AND "
        " (1 + docid < docid + 1);";

    ASSERT_ANALYZE_QUERY_OK(query);
}

TEST(AggregateTest, within_and_groupby) {
    // within first, groupby later
    const char* query1 =
        " SELECT "
        " docid, "
        " COUNT(name.language.code) WITHIN name as cnt_code , "
        " COUNT(name.language.country) as cnt_c, "
        " name.url "
        " FROM Document;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query1);

    // groupby first, within later
    const char* query2 =
        " SELECT "
        " docid, "
        " COUNT(name.language.code) as cnt_code , "
        " COUNT(name.language.country) WITHIN name as cnt_c, "
        " name.url "
        " FROM Document;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query2);

    do {
        const char* query3 =
            " SELECT "
            " docid, "
            " COUNT(name.language.code) WITHIN name as cnt_code, "
            " COUNT(name.language.country) WITHIN name as cnt_c, "
            " name.url "
            " FROM Document;";
        ASSERT_ANALYZE_QUERY_OK(query3);
    } while (0);

    do {
        const char* query4 =
            " SELECT "
            " docid, "
            " COUNT(name.language.code) as cnt_code, "
            " COUNT(name.language.country) as cnt_c "
            " FROM Document GROUPBY docid;";
        ASSERT_ANALYZE_QUERY_OK(query4);
    } while (0);
}

TEST(GroupByTest, validate_groupby_columns) {
    // groupby err docid
    const char* query1 =
        " SELECT "
        " docid, "
        " COUNT(name.language.code) as cnt_code, "
        " COUNT(name.language.country) + COUNT(links.forward) as cnt_c "
        " FROM Document GROUPBY docid_err;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query1);

    // duplicate groupby
    const char* query2 =
        " SELECT "
        " docid, "
        " COUNT(name.language.code) as cnt_code, "
        " COUNT(name.language.country) as cnt_c "
        " FROM Document GROUPBY docid, docid, docid;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query2);

    // repeat level error
    const char* query3 =
        " SELECT "
        " docid, "
        " COUNT(name.language.code) as cnt_code, "
        " COUNT(name.language.country) as cnt_c, "
        " name.url "
        " FROM Document GROUPBY docid, name.url;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query3);

    // ok
    do {
        const char* query4 =
            " SELECT "
            " docid, "
            " COUNT(name.language.code) as cnt_code, "
            " COUNT(name.language.country) as cnt_c "
            " FROM Document GROUPBY docid;";
        ASSERT_ANALYZE_QUERY_OK(query4);
    } while (0);

    do {
        const char* query5 =
            " SELECT "
            " docid, "
            " COUNT(docid) + docid as cnt_docid, "
            " COUNT(links.forward) as cnt_c "
            " FROM Document GROUPBY docid;";
        ASSERT_ANALYZE_QUERY_OK(query5);
    } while (0);


    // links.forward is not in groupby list
    const char* query6 =
        " SELECT "
        " docid, "
        " COUNT(docid) + docid as cnt_docid, "
        " links.forward + COUNT(links.forward) as cnt_c "
        " FROM Document GROUPBY docid;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query6);

    // name.url not in groupby list
    const char* query7 =
        " SELECT "
        " docid, "
        " COUNT(name.language.code) as cnt_code, "
        " COUNT(name.language.country) as cnt_c, "
        " name.url "
        " FROM Document GROUPBY docid;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query7);

    // name.url not in groupby list
    const char* query8 =
        " SELECT "
        " docid, name.url "
        " FROM Document GROUPBY docid;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query8);

    // ok
    do {
        const char* query9 =
            " SELECT "
            " COUNT(name.language.code) as cnt_code, "
            " COUNT(name.language.country) as cnt_c "
            " FROM Document;";
        ASSERT_ANALYZE_QUERY_OK(query9);
    } while (0);

    const char* query10 =
        " SELECT "
        " docid, "
        " COUNT(name.language.code) as cnt_code, "
        " COUNT(name.language.country) as cnt_c "
        " FROM Document;";
    ASSERT_ANALYZE_QUERY_NOT_OK(query10);
}

TEST(SelectQueryTest, having_analyze_test) {
    do {
        const char* query =
            " SELECT "
            " docid, "
            " COUNT(name.language.code) as cnt_code "
            " FROM Document "
            " GROUPBY docid "
            " HAVING cnt_code > 0 && docid < 1;";
        ASSERT_ANALYZE_QUERY_OK(query);
    } while (0);

    do {
        const char* query =
            " SELECT "
            " docid, "
            " COUNT(name.language.code) WITHIN name as cnt_code "
            " FROM Document "
            " HAVING cnt_code > 0;";
        ASSERT_ANALYZE_QUERY_OK(query);
    } while (0);

    do {
        const char* query =
            " SELECT "
            " docid, "
            " COUNT(name.language.code) WITHIN name as cnt_code "
            " FROM Document "
            " HAVING name.cnt_code > 0;";
        ASSERT_ANALYZE_QUERY_OK(query);
    } while (0);
}

TEST(SelectQueryTest, count_star_no_supported) {
    const char* query =
        "SELECT COUNT(*) AS cnt FROM Document;";
    ASSERT_ANALYZE_QUERY_OK(query);
}

} // namespace compiler
} // namespace gunir

