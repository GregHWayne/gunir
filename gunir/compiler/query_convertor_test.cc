// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <cstdio>
#include <string>

#include "toft/base/string/number.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/data_holder.h"
#include "gunir/compiler/compiler_test_helper.h"
#include "gunir/compiler/compiler_utils.h"
#include "gunir/compiler/parser/big_query_parser.h"
#include "gunir/compiler/query_convertor.h"
#include "gunir/compiler/select_query.h"

#include "gunir/io/slice.h"

namespace gunir {
namespace compiler {

QueryStmt* AnalyzeQueryStmt(const char* query) {
    QueryStmt* query_stmt_ptr;
    CHECK_EQ(0, parse_line(query, &query_stmt_ptr));
    return query_stmt_ptr;
}

TEST(QueryConvertorTest, top_function_test) {
    QueryConvertor query_convertor;
    std::string err_msg;

    const char* correct_query = "SELECT TOP(userid, 5), COUNT(*) from table;";
    QueryStmt* query_stmt_ptr = AnalyzeQueryStmt(correct_query);
    EXPECT_TRUE(query_convertor.Convert(query_stmt_ptr->mutable_select(),
                                        &err_msg));
    delete query_stmt_ptr;

    err_msg="";
    const char* plain_query = "SELECT COUNT(*) from table;";
    query_stmt_ptr = AnalyzeQueryStmt(plain_query);
    EXPECT_TRUE(query_convertor.Convert(query_stmt_ptr->mutable_select(),
                                        &err_msg));
    delete query_stmt_ptr;

    err_msg="";
    const char* incorrect_query = "SELECT Top(docid, docid), COUNT(*) from table;";
    query_stmt_ptr = AnalyzeQueryStmt(incorrect_query);
    EXPECT_FALSE(query_convertor.Convert(query_stmt_ptr->mutable_select(),
                                         &err_msg));
    delete query_stmt_ptr;

    err_msg="";
    incorrect_query = "SELECT Top(docid, 1), COUNT(docid) from table;";
    query_stmt_ptr = AnalyzeQueryStmt(incorrect_query);
    EXPECT_FALSE(query_convertor.Convert(query_stmt_ptr->mutable_select(),
                                         &err_msg));
    delete query_stmt_ptr;

    err_msg="";
    incorrect_query = "SELECT Top(1, 1), COUNT(*) from table;";
    query_stmt_ptr = AnalyzeQueryStmt(incorrect_query);
    EXPECT_FALSE(query_convertor.Convert(query_stmt_ptr->mutable_select(),
                                         &err_msg));
    delete query_stmt_ptr;

    err_msg="";
    incorrect_query = "SELECT Top(docid, 1), COUNT(*) "
                      "from table groupby docid";
    query_stmt_ptr = AnalyzeQueryStmt(incorrect_query);
    EXPECT_FALSE(query_convertor.Convert(query_stmt_ptr->mutable_select(),
                                         &err_msg));
    delete query_stmt_ptr;

    err_msg="";
    incorrect_query = "SELECT Top(docid, 1), COUNT(*) "
                      "from table orderby docid;";
    query_stmt_ptr = AnalyzeQueryStmt(incorrect_query);
    EXPECT_FALSE(query_convertor.Convert(query_stmt_ptr->mutable_select(),
                                         &err_msg));
    delete query_stmt_ptr;

    err_msg="";
    incorrect_query = "SELECT Top(docid, 1), COUNT(*) "
                      "from table limit 10;";
    query_stmt_ptr = AnalyzeQueryStmt(incorrect_query);
    EXPECT_FALSE(query_convertor.Convert(query_stmt_ptr->mutable_select(),
                                         &err_msg));
    delete query_stmt_ptr;
}

} // namespace compiler
} // namespace gunir

