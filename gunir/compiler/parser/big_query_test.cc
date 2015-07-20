// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/parser/big_query_parser.h"

#include <cstdio>
#include <cstdlib>

#include "thirdparty/gtest/gtest.h"

#include "gunir/compiler/parser/big_query_types.h"

namespace gunir {
namespace compiler {

void TestHasSimpleTarget(const RawTarget& target, const char* target_name);

void TestHasSimpleTable(const RawTable& table, const char* table_name);

void TestHasFunctionTarget(const RawFunction& function, const char* func_name,
                           const char* arg1, const char* arg2);

TEST(CreateTest, test) {
    const char* query = "CREATE t FROM '/path/for/read' '/schema_file' 'Hello';";
    QueryStmt* query_stmt;

    ASSERT_EQ(0, parse_line(query, &query_stmt));

    ASSERT_STREQ("t", query_stmt->create().
                 table_name().char_string().c_str());

    ASSERT_STREQ("/path/for/read", query_stmt->create().
                 input_path().char_string().c_str());

    ASSERT_STREQ("/schema_file", query_stmt->create().
                 table_schema().char_string().c_str());
    delete query_stmt;
}

TEST(DropTest, test) {
    const char* query = "DROP t;";
    QueryStmt* query_stmt;

    ASSERT_EQ(0, parse_line(query, &query_stmt));
    ASSERT_STREQ("t", query_stmt->drop().table_name().char_string().c_str());
    delete query_stmt;
}

TEST(SimpleParseTest, test) {
    const char* query = "SELECT a, b FROM c, d LIMIT 3;";
    QueryStmt* query_stmt;

    ASSERT_EQ(0, parse_line(query, &query_stmt));
    ASSERT_EQ(0, (query_stmt->select()).limit().start());
    ASSERT_EQ(3, (query_stmt->select()).limit().number());
    delete query_stmt;
}

TEST(SimpleErrorTest, test) {
    const char* query = "SELECT a FROM b, "
        "WHERE c > d AND e < f LIMIT 3;";

    QueryStmt* query_stmt = NULL;
    ASSERT_EQ(1, parse_line(query, &query_stmt));

    delete query_stmt;
}

TEST(NestingSelectTest, test) {
    const char* query = "SELECT a FROM b, "
        " (SELECT aa FROM bb WHERE cc >= dd LEFT OUTER JOIN ee ON ff <= gg) AS hh"
        " WHERE c > d && e < f AND g * h != i / j OR k >= 1 "
        " INNER JOIN l ON m == n "
        " GROUPBY o, p.q "
        " ORDERBY r.s ASC, t DESC "
        " LIMIT 2, 3;";

    QueryStmt* query_stmt;
    ASSERT_EQ(0, parse_line(query, &query_stmt));
    delete query_stmt;
}

TEST(ParseSelectStmtTest, test) {
    const char* query = "SELECT a WITHIN RECORD, b, count(aa, bb) "
        "FROM c, d WHERE 'e.f' >= \"g.h\" OR i != j ORDERBY k.l.m.n;";

    QueryStmt* query_stmt;
    ASSERT_EQ(0, parse_line(query, &query_stmt));

    EXPECT_TRUE(query_stmt->has_select());
    EXPECT_FALSE(query_stmt->has_create());
    EXPECT_FALSE(query_stmt->has_drop());

    const SelectStmt& stmt = query_stmt->select();

    // check stmt
    EXPECT_TRUE(stmt.has_target_list());
    EXPECT_TRUE(stmt.has_from_list());
    EXPECT_TRUE(stmt.has_where_clause());
    EXPECT_FALSE(stmt.has_join());
    EXPECT_FALSE(stmt.has_groupby());
    EXPECT_FALSE(stmt.has_having());
    EXPECT_TRUE(stmt.has_orderby());
    EXPECT_FALSE(stmt.has_limit());

    // check targets
    const RawTargetList& target_list = stmt.target_list();
    EXPECT_TRUE(target_list.has_target_is_star());
    EXPECT_FALSE(target_list.target_is_star());
    EXPECT_EQ(3, target_list.target_list_size());

    TestHasSimpleTarget(target_list.target_list(0), "a");
    EXPECT_TRUE(target_list.target_list(0).within().is_record());
    TestHasSimpleTarget(target_list.target_list(1), "b");
    EXPECT_FALSE(target_list.target_list(1).has_within());
    // check function
    TestHasFunctionTarget(
        target_list.target_list(2).expression().atomic().function(),
        "count", "aa", "bb");

    // check from list
    const RawTableList& table_list = stmt.from_list();
    ASSERT_EQ(2, table_list.table_list_size());
    TestHasSimpleTable(table_list.table_list(0), "c");
    TestHasSimpleTable(table_list.table_list(1), "d");

    // check where clause
    const RawExpression& where = stmt.where_clause();
    ASSERT_EQ(kLogicalOr, where.op());
    const RawExpression& left = where.left();
    const RawExpression& right = where.right();
    ASSERT_EQ(kGreaterEqual, left.op());
    ASSERT_EQ(kNotEqual, right.op());

    // check expression
    ASSERT_STREQ("e.f", left.left().atomic().
                 char_string().char_string().c_str());
    ASSERT_STREQ("g.h", left.right().atomic().
                 char_string().char_string().c_str());

    ASSERT_STREQ("i", right.left().atomic().column().
                 field_list(0).char_string().c_str());
    ASSERT_STREQ("j", right.right().atomic().column().
                 field_list(0).char_string().c_str());
    delete query_stmt;
}

void TestHasSimpleTarget(const RawTarget& target, const char* target_name) {
    EXPECT_TRUE(target.has_expression());
    EXPECT_FALSE(target.has_alias());

    const RawExpression& expr = target.expression();
    EXPECT_FALSE(expr.has_left());
    EXPECT_FALSE(expr.has_right());
    EXPECT_TRUE(expr.has_atomic());
    EXPECT_FALSE(expr.has_op());

    const RawAtomicExpression& atomic = expr.atomic();
    EXPECT_FALSE(atomic.has_function());
    EXPECT_FALSE(atomic.has_integer());
    EXPECT_FALSE(atomic.has_floating());
    EXPECT_FALSE(atomic.has_char_string());
    EXPECT_TRUE(atomic.has_column());
    EXPECT_FALSE(atomic.has_expression());

    const ColumnPath& path = atomic.column();
    ASSERT_EQ(1, path.field_list_size());
    EXPECT_STREQ(target_name, path.field_list(0).char_string().c_str());
}

void TestHasFunctionTarget(const RawFunction& function, const char* func_name,
                           const char* arg1, const char* arg2) {
    EXPECT_STREQ(func_name, function.function_name().char_string().c_str());

    const RawArguments& args = function.args();
    EXPECT_FALSE(args.arg_is_star());

    const RawExpressionList& expr_list = args.arg_list();
    EXPECT_EQ(2, expr_list.expr_list_size());

    EXPECT_STREQ(arg1, expr_list.expr_list(0).atomic().column().
                 field_list(0).char_string().c_str());
    EXPECT_STREQ(arg2, expr_list.expr_list(1).atomic().column().
                 field_list(0).char_string().c_str());
}

void TestHasSimpleTable(const RawTable& table, const char* table_name) {
    EXPECT_TRUE(table.has_table_name());
    EXPECT_FALSE(table.has_select_stmt());
    EXPECT_FALSE(table.has_alias());

    EXPECT_STREQ(table_name, table.table_name().char_string().c_str());
}

TEST(big_query_type_test, type_convert_test) {
    const char* type_string[BigQueryType::MAX_TYPE + 1] = {
        "", "BOOL", "INT32", "UINT32", "INT64", "UINT64",
        "FLOAT", "DOUBLE", "STRING", "ENUM", "MESSAGE", "BYTES"
    };

    for (int i = 0; i <= BigQueryType::MAX_TYPE; i++) {
        ASSERT_EQ(std::string(type_string[i]),
                  EnumToString(static_cast<BQType>(i)));
    }
}

TEST(big_query_lex_test, operator_test) {
    const char* query =
        " SELECT "
        " a0 <= b0 AS c0, "
        " a1 < b1 AS c1, "
        " a2 >= b2 AS c2, "
        " a3 > b3 AS c3, "
        " a4 == b4 AS c4, "
        " a5 = b5 AS c5, "
        " a6 <> b6 AS c6, "
        " a7 != b7 AS c7, "
        " a8 & b8 AS c8, "
        " ~ b9 AS c9, "
        " a10 | b10 AS c10, "
        " a11 ^ b11 AS c11, "
        " a12 << b12 AS c12, "
        " a13 >> b13 AS c13, "
        " a14 && b14 AS c14, "
        " a15 || b15 AS c15, "
        " !b16 AS c16, "
        " a17 / b17 AS c17, "
        " a18 * b18 AS c18, "
        " a19 + b19 AS c19, "
        " a20 - b20 AS c20, "
        " a21 % b21 AS c21, "
        " a21 AND b21 AS c21, "
        " a22 OR b22 AS c22, "
        " NOT a23 AS c23, "
        " a24 CONTAINS b24 AS c24, "
        " a25 DIV b25 AS c25 "
        " FROM table;";
    const Operators op_array[] = {
        kLessEqual, kLess, kGreaterEqual, kGreater,
        kEqual, kEqual, kNotEqual, kNotEqual,
        kBitwiseAnd, kBitwiseNot, kBitwiseOr, kBitwiseXor,
        kBitwiseLeftShift, kBitwiseRightShift,
        kLogicalAnd, kLogicalOr, kLogicalNot,
        kDiv, kMul, kAdd, kSub, kRemainder,
        kLogicalAnd, kLogicalOr, kLogicalNot,
        kContains, kDiv
    };

    QueryStmt* query_stmt;
    ASSERT_EQ(0, parse_line(query, &query_stmt));

    ASSERT_TRUE(query_stmt->has_select());
    const SelectStmt& stmt = query_stmt->select();

    // check stmt
    ASSERT_TRUE(stmt.has_target_list());
    const RawTargetList& target_list = stmt.target_list();
    int op_number = sizeof(op_array) / sizeof(Operators);
    ASSERT_EQ(op_number, target_list.target_list_size());

    for (int i = 0; i < target_list.target_list_size(); ++i) {
        const RawTarget& target = target_list.target_list(i);
        const RawExpression& expr = target.expression();
        ASSERT_TRUE(expr.has_op());
        ASSERT_EQ(op_array[i], expr.op());
    }
    delete query_stmt;
}

TEST(big_query_lex_test, numeric) {
    const char* query =
        " SELECT TRUE, FLASE, 0.5, +3, -4, +0.125, -0.25, "
        " 'abcd\\'\"efg\"hi\\n\\t\\\\' "
        " FROM table;";

    QueryStmt* query_stmt;
    ASSERT_EQ(0, parse_line(query, &query_stmt));
    ASSERT_TRUE(query_stmt->has_select());
    const SelectStmt& stmt = query_stmt->select();
    const RawTargetList& target_list = stmt.target_list();

    ASSERT_EQ(8, target_list.target_list_size());
    ASSERT_EQ(true, target_list.target_list(0).expression().atomic().boolean());
    // false == t1,
    // use ASSERT_EQ(false, target...) here has compile error
    ASSERT_FALSE(target_list.target_list(1).expression().atomic().boolean());
    ASSERT_DOUBLE_EQ(0.5, target_list.target_list(2).expression().atomic().floating());
    ASSERT_EQ(3, target_list.target_list(3).expression().atomic().integer());
    ASSERT_EQ(-4, target_list.target_list(4).expression().atomic().integer());
    ASSERT_DOUBLE_EQ(0.125, target_list.target_list(5).expression().atomic().floating());
    ASSERT_DOUBLE_EQ(-0.25, target_list.target_list(6).expression().atomic().floating());
    ASSERT_EQ("abcd'\"efg\"hi\n\t\\",
              target_list.target_list(7).expression().atomic().char_string().char_string());
    delete query_stmt;
}

TEST(big_query_lex_test, numeric_error) {
    const char* query1 =
        " SELECT \"abcd'efg'hi\n\" "
        " FROM table;";

    QueryStmt* query_stmt;
    ASSERT_EQ(1, parse_line(query1, &query_stmt));
    delete query_stmt;

    const char* query2 =
        " SELECT \"abcdefghi\0888\n\" "
        " FROM table;";
    ASSERT_EQ(1, parse_line(query2, &query_stmt));
    delete query_stmt;
}

TEST(big_query_lex_test, not_recognized_symbol) {
    const char* query1 =
        " SELECT abc + `? "
        " FROM table;";

    QueryStmt* query_stmt;
    ASSERT_EQ(1, parse_line(query1, &query_stmt));
    delete query_stmt;
}

} // namespace compiler
} // namespace gunir

