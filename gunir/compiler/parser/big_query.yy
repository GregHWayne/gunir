%{
// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin <qinan@baiducom>
//
#include "gunir/compiler/parser/big_query_parser.h"

#include <iostream>

using namespace gunir;
using namespace compiler;

RawExpression* make_expression(RawExpression* left, Operators op,
    RawExpression* right, YaccMemoryPool* memory_pool);

OrderColumnPath* add_order_column_path(const ColumnPath& column_path,
    OrderType type, YaccMemoryPool* memory_pool);

#define make_query_stmt(TYPE, Value1, Value)  \
            QueryStmt* query_stmt = NewMessage<QueryStmt>(memory_pool);\
            (query_stmt->mutable_##TYPE())->CopyFrom(*Value1);\
            Value = query_stmt;\
            (gunir_get_extra(yyscanner))->query_stmt = Value;\
            return 0;

%}

%locations
%parse-param {yyscan_t yyscanner}
%parse-param {YaccMemoryPool* memory_pool}
%parse-param {std::ostream* output}
%error-verbose

%union
{
    int64_t _int;
    bool _bool;
    double _float;
    gunir::compiler::StringMessage* _string;

    gunir::compiler::ColumnPath* _column_path;
    gunir::compiler::ColumnPathList* _column_path_list;
    gunir::compiler::CreateTableStmt* _create_stmt;

    gunir::compiler::DropTableStmt* _drop_stmt;
    gunir::compiler::HelpStmt* _help_stmt;
    gunir::compiler::ShowStmt* _show_stmt;
    gunir::compiler::HistoryStmt* _history_stmt;
    gunir::compiler::QuitStmt* _quit_stmt;
    gunir::compiler::DefineTableStmt* _define_stmt;

    gunir::compiler::JoinOperator _join_op;
    gunir::compiler::Limit* _limit;
    gunir::compiler::Operators  _operator;
    gunir::compiler::OrderColumnPathList* _order_column_path_list;
    gunir::compiler::OrderColumnPath* _order_column_path;
    gunir::compiler::OrderType _order_type;

    gunir::compiler::RawArguments* _raw_arguments;
    gunir::compiler::RawAtomicExpression* _raw_atomic_expression;
    gunir::compiler::RawExpressionList* _raw_expression_list;
    gunir::compiler::RawExpression* _raw_expression;
    gunir::compiler::RawFunction* _raw_function;
    gunir::compiler::RawJoin* _raw_join;
    gunir::compiler::RawTableList* _raw_table_list;
    gunir::compiler::RawTable* _raw_table;
    gunir::compiler::RawTargetList* _raw_target_list;
    gunir::compiler::RawTarget* _raw_target;
    gunir::compiler::RawWithin* _raw_within;

    gunir::compiler::QueryStmt* _query_stmt;
    gunir::compiler::SelectStmt* _select_stmt;
};

%token SHOW
%token HISTORY
%token HELP
%token QUIT
%token DEFINE 
%token CREATE
%token DROP
%token SELECT
%token WITHIN
%token RECORD
%token AS
%token FROM
%token WHERE
%token JOIN
%token INNER
%token LEFT
%token OUTER
%token ON
%token GROUPBY
%token HAVING
%token ORDERBY
%token DISTINCT
%token DESC
%token ASC
%token LIMIT

%token LOGICAL_OR
%token LOGICAL_AND
%token LOGICAL_NOT

%token BITWISE_AND
%token BITWISE_NOT
%token BITWISE_OR
%token BITWISE_XOR
%token BITWISE_LEFT_SHIFT
%token BITWISE_RIGHT_SHIFT

%token EQUAL
%token NOT_EQUAL
%token LESS_THAN_OR_EQUAL
%token LESS_THAN
%token GREATER_THAN_OR_EQUAL
%token GREATER_THAN

%token SLASH
%token DIV
%token STAR
%token PLUS
%token MINUS
%token REMAINDER
%token CONTAINS

%token DOT
%token COLON
%token COMMA
%token SEMICOLON
%token LPAREN
%token RPAREN
%token LSQUARE
%token RSQUARE
%token LCURLY
%token RCURLY
%token ENDOF
%token LEX_ERROR
%token WS

%token <_int> INT
%token <_float> FLOAT
%token <_bool> BOOLEAN 
%token <_string> ID
%token <_string> STRING

%type <_int> integer
%type <_float> float

%type <_query_stmt> query_stmt
%type <_create_stmt> createStatement
%type <_drop_stmt> dropStatement
%type <_help_stmt> helpStatement
%type <_show_stmt> showStatement
%type <_history_stmt> historyStatement
%type <_quit_stmt> quitStatement
%type <_select_stmt> selectStatement
%type <_define_stmt> defineStatement

%type <_raw_target_list> selectClause targetClause targetList
%type <_raw_target> target
%type <_raw_within> withinClause

%type <_raw_join> joinClause
%type <_join_op> joinOperator

%type <_limit> limitClause

%type <_raw_table_list> fromClause tableList
%type <_raw_table> table

%type <_column_path_list> groupbyClause columnPathList
%type <_column_path> columnPath
%type <_order_column_path_list> orderbyClause orderbyColumnPathList
%type <_order_column_path> orderbyColumnPath

%type <_raw_expression_list> expressionList
%type <_raw_expression> whereClause havingClause expression
%type <_raw_expression> binary10thPrcdexpr binary9thPrcdexpr binary8thPrcdexpr
%type <_raw_expression> binary7thPrcdexpr binary6thPrcdexpr binary5thPrcdexpr
%type <_raw_expression> binary4thPrcdexpr binary3rdPrcdexpr binary2ndPrcdexpr
%type <_raw_expression> binary1stPrcdexpr unaryPrefixExpr
%type <_raw_atomic_expression> atomExpr

%type <_raw_function> function
%type <_raw_arguments> args

%type <_string> alias tableName fieldName

%type <_operator> divideOp multiplyOp
%type <_operator> unaryPrefixOp binary1stPrcdOp binary2ndPrcdOp binary3rdPrcdOp
%type <_operator> binary4thPrcdOp binary5thPrcdOp binary6thPrcdOp
%type <_operator> binary7thPrcdOp binary8thPrcdOp 
%type <_operator> binary9thPrcdOp binary10thPrcdOp binary11thPrcdOp

%type <_string> id
%type <_string> char_string

%%

query_stmt:
        selectStatement SEMICOLON ENDOF {
            make_query_stmt(select, $1, $$)
        }

        | selectStatement ENDOF {
            make_query_stmt(select, $1, $$)
        }

        | createStatement SEMICOLON ENDOF {
            make_query_stmt(create, $1, $$)
        }

        | createStatement ENDOF {
            make_query_stmt(create, $1, $$)
        }

        | dropStatement SEMICOLON ENDOF {
            make_query_stmt(drop, $1, $$)
        }
        
        | dropStatement ENDOF {
            make_query_stmt(drop, $1, $$)
        }

        | showStatement SEMICOLON ENDOF {
            make_query_stmt(show, $1, $$)
        }

        | showStatement ENDOF {
            make_query_stmt(show, $1, $$)
        }

        | historyStatement SEMICOLON ENDOF {
            make_query_stmt(history, $1, $$)
        }

        | historyStatement ENDOF {
            make_query_stmt(history, $1, $$)
        }

        | helpStatement SEMICOLON ENDOF {
            make_query_stmt(help, $1, $$)
        }

        | helpStatement ENDOF {
            make_query_stmt(help, $1, $$)
        }

        | quitStatement SEMICOLON ENDOF {
            make_query_stmt(quit, $1, $$)
        }

        | quitStatement ENDOF {
            make_query_stmt(quit, $1, $$)
        }

        | defineStatement SEMICOLON ENDOF {
            make_query_stmt(define, $1, $$)
        }

        | defineStatement ENDOF {
            make_query_stmt(define, $1, $$)
        }

        | LEX_ERROR {
            return -1;
        }
        ;


createStatement:
            CREATE tableName FROM char_string char_string char_string {
            CreateTableStmt* stmt = NewMessage<CreateTableStmt>(memory_pool);

            (stmt->mutable_table_name())->CopyFrom(*$2);
            (stmt->mutable_input_path())->CopyFrom(*$4);
            (stmt->mutable_table_schema())->CopyFrom(*$5);
            (stmt->mutable_message_name())->CopyFrom(*$6);
            $$ = stmt;
        }

        | CREATE tableName FROM char_string char_string char_string char_string {
            CreateTableStmt* stmt = NewMessage<CreateTableStmt>(memory_pool);

            (stmt->mutable_table_name())->CopyFrom(*$2);
            (stmt->mutable_input_path())->CopyFrom(*$4);
            (stmt->mutable_table_schema())->CopyFrom(*$5);
            (stmt->mutable_message_name())->CopyFrom(*$6);
            (stmt->mutable_charset_encoding())->CopyFrom(*$7);
            $$ = stmt;
        }
        ;

dropStatement:
        DROP tableName {
            DropTableStmt* stmt = NewMessage<DropTableStmt>(memory_pool);
            (stmt->mutable_table_name())->CopyFrom(*$2);
            $$ = stmt;
        }
        ;

showStatement:
        SHOW {
            ShowStmt* stmt = NewMessage<ShowStmt>(memory_pool);
            $$ = stmt;
        }

        | SHOW tableName {
            ShowStmt* stmt = NewMessage<ShowStmt>(memory_pool);
            (stmt->mutable_table_name())->CopyFrom(*$2);
            $$ = stmt;
        }
        ;

helpStatement:
        HELP {
            HelpStmt* stmt = NewMessage<HelpStmt>(memory_pool);
            $$ = stmt;
        }

        | HELP char_string {
            HelpStmt* stmt = NewMessage<HelpStmt>(memory_pool);
            (stmt->mutable_cmd_name())->CopyFrom(*$2);
            $$ = stmt;
        }
        ;

historyStatement:
        HISTORY {
            HistoryStmt* stmt = NewMessage<HistoryStmt>(memory_pool);
            $$ = stmt;
        }

        | HISTORY integer {
            HistoryStmt* stmt = NewMessage<HistoryStmt>(memory_pool);
            stmt->set_start($2);
            $$ = stmt;
        }

        | HISTORY integer integer {
            HistoryStmt* stmt = NewMessage<HistoryStmt>(memory_pool);
            stmt->set_start($2);
            stmt->set_size($3);
            $$ = stmt;
        }
        ;

quitStatement:
        QUIT {
            QuitStmt* stmt = NewMessage<QuitStmt>(memory_pool);
            $$ = stmt;
        }
        ;

defineStatement:
        DEFINE tableName AS char_string {
            DefineTableStmt* stmt = NewMessage<DefineTableStmt>(memory_pool);
            (stmt->mutable_table_name())->CopyFrom(*$2);
            (stmt->mutable_input_path())->CopyFrom(*$4);
            $$ = stmt;
        }
        ;

selectStatement:
        selectClause fromClause whereClause joinClause groupbyClause
                     havingClause orderbyClause limitClause {
            SelectStmt* stmt = NewMessage<SelectStmt>(memory_pool);

            if ($1 != NULL) {
                (stmt->mutable_target_list())->CopyFrom(*$1);
            }

            if ($2 != NULL) {
                (stmt->mutable_from_list())->CopyFrom(*$2);
            }

            if ($3 != NULL) {
                (stmt->mutable_where_clause())->CopyFrom(*$3);
            }

            if ($4 != NULL) {
                (stmt->mutable_join())->CopyFrom(*$4);
            }

            if ($5 != NULL) {
                (stmt->mutable_groupby())->CopyFrom(*$5);
            }

            if ($6 != NULL) {
                (stmt->mutable_having())->CopyFrom(*$6);
            }

            if ($7 != NULL) {
                (stmt->mutable_orderby())->CopyFrom(*$7);
            }

            if ($8 != NULL) {
                (stmt->mutable_limit())->CopyFrom(*$8);
            }
            $$ = stmt;
        }
        ;

selectClause:
        SELECT targetClause {
            $$ = $2;
        }
        ;

targetClause:
        STAR {
            RawTargetList* target_list = NewMessage<RawTargetList>(memory_pool);
            target_list->set_target_is_star(true);
            $$ = target_list;
        }

        | targetList {
            $$ = $1;
        }
        ;

targetList:
        target {
            RawTargetList* target_list = NewMessage<RawTargetList>(memory_pool);
            RawTarget* t = (target_list->mutable_target_list())->Add();

            t->CopyFrom(*$1);
            target_list->set_target_is_star(false);
            $$ = target_list;
        }

        | targetList COMMA target {
            RawTarget* t = ($1->mutable_target_list())->Add();
            t->CopyFrom(*$3);
            $$ = $1;
        }
        ;

target:
        expression withinClause {
            RawTarget* target = NewMessage<RawTarget>(memory_pool);

            (target->mutable_expression())->CopyFrom(*$1);

            if ($2 != NULL) {
                (target->mutable_within())->CopyFrom(*$2);
            }
            $$ = target;
        }

        | expression withinClause AS alias {
            RawTarget* target = NewMessage<RawTarget>(memory_pool);

            (target->mutable_expression())->CopyFrom(*$1);

            if ($2 != NULL) {
                (target->mutable_within())->CopyFrom(*$2);
            }
            (target->mutable_alias())->CopyFrom(*$4);
            $$ = target;
        }
        ;

withinClause:
        WITHIN RECORD {
            RawWithin* within = NewMessage<RawWithin>(memory_pool);
            within->set_is_record(true);
            $$ = within;
        }

        | WITHIN columnPath {
            RawWithin* within = NewMessage<RawWithin>(memory_pool);

            within->set_is_record(false);
            (within->mutable_column())->CopyFrom(*$2);
            $$ = within;
        }

        | { $$ = NULL; }
        ;

fromClause:
        FROM tableList {
            $$ = $2;
        }
        ;

tableList:
        table {
            RawTableList* list = NewMessage<RawTableList>(memory_pool);

            RawTable* t =  (list->mutable_table_list())->Add();
            t->CopyFrom(*$1);
            $$ = list;
        }

        | tableList COMMA table {
            RawTable* t = ($1->mutable_table_list())->Add();
            t->CopyFrom(*$3);
            $$ = $1;
        }
        ;

table:
        tableName {
            RawTable* table = NewMessage<RawTable>(memory_pool);
            (table->mutable_table_name())->CopyFrom(*$1);
            $$ = table;
        }

        | tableName AS alias {
            RawTable* table = NewMessage<RawTable>(memory_pool);

            (table->mutable_table_name())->CopyFrom(*$1);
            (table->mutable_alias())->CopyFrom(*$3);
            $$ = table;
        }

        | LPAREN selectStatement RPAREN AS alias {
            RawTable* table = NewMessage<RawTable>(memory_pool);

            (table->mutable_select_stmt())->CopyFrom(*$2);
            (table->mutable_alias())->CopyFrom(*$5);
            $$ = table;
        }
        ;

whereClause:
        WHERE expression {
            $$ = $2;
        }

        | { $$ = NULL; }
        ;

joinClause:
        joinOperator JOIN table ON expression {
            RawJoin* join = NewMessage<RawJoin>(memory_pool);

            join->set_op($1);
            (join->mutable_partner())->CopyFrom(*$3);
            (join->mutable_expression())->CopyFrom(*$5);
            $$ = join;
        }

        | { $$ = NULL; }
        ;

joinOperator:
        LEFT OUTER {
            $$ = kLeftOuter;
        }

        | INNER {
            $$ = kInner;
        }
        
        | { $$ = kInner; }
        ;

groupbyClause:
        GROUPBY columnPathList {
            $$ = $2;
        }

        | { $$ = NULL; }
        ;

havingClause:
        HAVING expression {
            $$ = $2;
        }

        | { $$ = NULL; }
        ;

orderbyClause:
        ORDERBY orderbyColumnPathList {
            $$ = $2;
        }

        | { $$ = NULL; }
        ;

limitClause:
        LIMIT integer {
            Limit* limit = NewMessage<Limit>(memory_pool);

            if ($2 < 0) {
                yyerror(yyscanner, memory_pool, output,
                    "error, limit number is negative");
                return -1;
            }

            limit->set_start(0);
            limit->set_number($2);
            $$ = limit;
        }

        | LIMIT integer COMMA integer {
            Limit* limit = NewMessage<Limit>(memory_pool);

            if ($2 < 0 || $4 < 0) {
                yyerror(yyscanner, memory_pool, output,
                    "error, limit number is negative");
                return -1;
            }

            limit->set_start($2);
            limit->set_number($4);
            $$ = limit;
        }

        | { $$ = NULL; }
        ;

orderbyColumnPathList:
        orderbyColumnPath {
            OrderColumnPathList* list = 
                NewMessage<OrderColumnPathList>(memory_pool);

            OrderColumnPath* path = (list->mutable_path_list())->Add();
            path->CopyFrom(*$1);
            $$ = list;
        }

        | orderbyColumnPathList COMMA orderbyColumnPath {
            OrderColumnPath* path = ($1->mutable_path_list())->Add();
            path->CopyFrom(*$3);
            $$ = $1;
        }
        ;

// default asc
orderbyColumnPath:
        columnPath {
            $$ = add_order_column_path(*$1, kAsc, memory_pool);
        }

        | columnPath ASC {
            $$ = add_order_column_path(*$1, kAsc, memory_pool);
        }

        | columnPath DESC {
            $$ = add_order_column_path(*$1, kDesc, memory_pool);
        }
        ;

columnPathList:
        columnPath {
            ColumnPathList * list = NewMessage<ColumnPathList>(memory_pool);

            ColumnPath* path = (list->mutable_path_list())->Add();
            path->CopyFrom(*$1);
            $$ = list;
        }

        | columnPathList COMMA columnPath {
            ColumnPath* path = ($1->mutable_path_list())->Add();
            path->CopyFrom(*$3);
            $$ = $1;
        }
        ;

///Table names
columnPath:
        fieldName {
            ColumnPath* path = NewMessage<ColumnPath>(memory_pool);

            StringMessage* s = (path->mutable_field_list())->Add();
            s->CopyFrom(*$1);
            $$ = path;
        }

        | columnPath DOT fieldName {
            StringMessage* s = ($1->mutable_field_list())->Add();
            s->CopyFrom(*$3);
            $$ = $1;
        }

        | DISTINCT columnPath {
            $2->set_has_distinct(true);
            $$ = $2;
        }
        ;

fieldName:
        id { $$ = $1; }
        ;

tableName:
        id { $$ = $1; }
        ;

alias:
        id { $$ = $1; }
        ;

expression:
        binary10thPrcdexpr {
            $$ = $1;
        }

        | expression binary11thPrcdOp binary10thPrcdexpr {
            $$ = make_expression($1, $2, $3, memory_pool);
        }
        ;

binary10thPrcdexpr:
        binary9thPrcdexpr {
            $$ = $1;
        }

        | binary10thPrcdexpr binary10thPrcdOp binary9thPrcdexpr {
            $$ = make_expression($1, $2, $3, memory_pool);
        }
        ;

binary9thPrcdexpr:
        binary8thPrcdexpr {
            $$ = $1;
        }

        |   binary9thPrcdexpr binary9thPrcdOp binary8thPrcdexpr {
            $$ = make_expression($1, $2, $3, memory_pool);
        }
        ;

binary8thPrcdexpr:
        binary7thPrcdexpr {
            $$ = $1;
        }

        | binary8thPrcdexpr binary8thPrcdOp binary7thPrcdexpr {
            $$ = make_expression($1, $2, $3, memory_pool);
        }
        ;

binary7thPrcdexpr:
        binary6thPrcdexpr {
            $$ = $1;
        }

        | binary7thPrcdexpr binary7thPrcdOp binary6thPrcdexpr {
            $$ = make_expression($1, $2, $3, memory_pool);
        }
        ;

binary6thPrcdexpr:
        binary5thPrcdexpr {
            $$ = $1;
        }

        | binary6thPrcdexpr binary6thPrcdOp binary5thPrcdexpr {
            $$ = make_expression($1, $2, $3, memory_pool);
        }
        ;

binary5thPrcdexpr:
        binary4thPrcdexpr {
            $$ = $1;
        }

        | binary5thPrcdexpr binary5thPrcdOp binary4thPrcdexpr {
            $$ = make_expression($1, $2, $3, memory_pool);
        }
        ;

binary4thPrcdexpr:
        binary3rdPrcdexpr {
            $$ = $1;
        }

        | binary4thPrcdexpr binary4thPrcdOp binary3rdPrcdexpr {
            $$ = make_expression($1, $2, $3, memory_pool);
        }
        ;

binary3rdPrcdexpr:
        binary2ndPrcdexpr {
            $$ = $1;
        }

        | binary3rdPrcdexpr binary3rdPrcdOp binary2ndPrcdexpr {
            $$ = make_expression($1, $2, $3, memory_pool);
        }
        ;

binary2ndPrcdexpr:
        binary1stPrcdexpr {
            $$ = $1;
        }

        | binary2ndPrcdexpr binary2ndPrcdOp binary1stPrcdexpr {
            $$ = make_expression($1, $2, $3, memory_pool);
        }
        ;

binary1stPrcdexpr:
        unaryPrefixExpr {
            $$ = $1;
        }

        | binary1stPrcdexpr binary1stPrcdOp unaryPrefixExpr {
            $$ = make_expression($1, $2, $3, memory_pool);
        }
        ;

unaryPrefixExpr:
        atomExpr {
            RawExpression* expr = NewMessage<RawExpression>(memory_pool);
            (expr->mutable_atomic())->CopyFrom(*$1);
            $$ = expr;
        }

        | unaryPrefixOp unaryPrefixExpr {
            RawExpression* expr = NewMessage<RawExpression>(memory_pool);

            expr->set_op($1);
            (expr->mutable_left())->CopyFrom(*$2);
            $$ = expr;
        }
        ;

atomExpr:
        integer {
            RawAtomicExpression* expr = 
                NewMessage<RawAtomicExpression>(memory_pool);
            expr->set_integer($1);
            $$ = expr;
        }

        | float {
            RawAtomicExpression* expr = 
                NewMessage<RawAtomicExpression>(memory_pool);
            expr->set_floating($1);
            $$ = expr;
        }

        | BOOLEAN {
            RawAtomicExpression* expr = 
                NewMessage<RawAtomicExpression>(memory_pool);
            expr->set_boolean($1);
            $$ = expr;
        }

        | char_string {
            RawAtomicExpression* expr = 
                NewMessage<RawAtomicExpression>(memory_pool);
            (expr->mutable_char_string())->CopyFrom(*$1);
            $$ = expr;
        }

        | function {
            RawAtomicExpression* expr = 
                NewMessage<RawAtomicExpression>(memory_pool);
            (expr->mutable_function())->CopyFrom(*$1);
            $$ = expr;
        }

        | columnPath {
            RawAtomicExpression* expr = 
                NewMessage<RawAtomicExpression>(memory_pool);
            (expr->mutable_column())->CopyFrom(*$1);
            $$ = expr;
        }

        | LPAREN expression RPAREN {
            RawAtomicExpression* expr = 
                NewMessage<RawAtomicExpression>(memory_pool);
            (expr->mutable_expression())->CopyFrom(*$2);
            $$ = expr;
        }
        ;

function:
        id LPAREN args RPAREN {
            RawFunction* function = NewMessage<RawFunction>(memory_pool);

            (function->mutable_function_name())->CopyFrom(*$1);
            (function->mutable_args())->CopyFrom(*$3);
            $$ = function;
        }
        ;

args:
        expressionList {
            RawArguments* args = NewMessage<RawArguments>(memory_pool);

            (args->mutable_arg_list())->CopyFrom(*$1);
            args->set_arg_is_star(false);
            $$ = args;
        }

        | STAR {
            RawArguments* args = NewMessage<RawArguments>(memory_pool);
            args->set_arg_is_star(true);
            $$ = args;
        }
        ;

expressionList:
        expression {
            RawExpressionList* list = 
                NewMessage<RawExpressionList>(memory_pool);
            RawExpression* e = (list->mutable_expr_list())->Add();

            e->CopyFrom(*$1);
            $$ = list;
        }

        | expressionList COMMA expression {
            RawExpression* e = ($1->mutable_expr_list())->Add();
            e->CopyFrom(*$3);
            $$ = $1;
        }
        ;


// Ops
binary11thPrcdOp:
        LOGICAL_OR {
            $$ = kLogicalOr;
        }
        ;

binary10thPrcdOp:
        LOGICAL_AND {
            $$ = kLogicalAnd;
        }
        ;

binary9thPrcdOp:
        BITWISE_OR {
            $$ = kBitwiseOr;
        }
        ;

binary8thPrcdOp:
        BITWISE_XOR {
            $$ = kBitwiseXor;
        }
        ;

binary7thPrcdOp:
        BITWISE_AND {
            $$ = kBitwiseAnd;
        }
        ;

binary6thPrcdOp:
        EQUAL {
            $$ = kEqual;
        }

        | NOT_EQUAL {
            $$ = kNotEqual;
        }
        ;

binary5thPrcdOp:
        LESS_THAN {
            $$ = kLess;
        }

        | LESS_THAN_OR_EQUAL {
            $$ = kLessEqual;
        }

        | GREATER_THAN  {
            $$ = kGreater;
        }

        | GREATER_THAN_OR_EQUAL {
            $$ = kGreaterEqual;
        }
        ;

binary4thPrcdOp:
        BITWISE_LEFT_SHIFT {
            $$ = kBitwiseLeftShift;
        }
        
        | BITWISE_RIGHT_SHIFT {
            $$ = kBitwiseRightShift;
        }
        ;

binary3rdPrcdOp:
        PLUS {
            $$ = kAdd;
        }

        | MINUS {
            $$ = kSub;
        }
        ;

binary2ndPrcdOp:
        multiplyOp {
            $$ = $1;
        }

        | divideOp {
            $$ = $1;
        }

        | REMAINDER {
            $$ = kRemainder;
        }
        ;

// someword CONTAINS 'abc'
binary1stPrcdOp:
        CONTAINS {
            $$ = kContains;
        }
        ;

unaryPrefixOp:  
        BITWISE_NOT {
            $$ = kBitwiseNot;
        }

        | LOGICAL_NOT {
            $$ = kLogicalNot;
        }
        ;

divideOp:
        SLASH {
            $$ = kDiv;
        }
        
        | DIV {
            $$ = kDiv;
        }
        ;

multiplyOp:
        STAR {
            $$ = kMul;
        }
        ;

id:
        ID {
            $$ = $1;
        }
        ;

char_string:
        STRING {
            $$ = $1;
        }
        ;

integer:
        INT {
            $$ = $1;
        }

        | PLUS INT {
            $$ = $2;
        }

        | MINUS INT {
            $$ = $2 * -1;
        }
        ;

float:
        FLOAT {
            $$ = $1;
        }

        | PLUS FLOAT {
            $$ = $2;
        }

        | MINUS FLOAT {
            $$ = $2 * -1;
        }
        ;

%%

void yyerror(yyscan_t yyscanner, YaccMemoryPool* pool,
std::ostream* output, const char *err_msg) {
    const char* query = (gunir_get_extra(yyscanner))->query;

    if (query == NULL) {
        query = "<query is stdin>";
    }

    yylloc.first_column -= 1;
    yylloc.last_column -= 1;

    *output << err_msg << " near column " << yylloc.first_column
        << " to " << yylloc.last_column << " : " << std::endl;
    *output << query << std::endl;

    for (int i = 0; i < yylloc.first_column; ++i)
        *output << " ";
    *output << "^" << std::endl;
}

int gunir_wrap(void* p) {
    return 1;
}

std::string trim_query(const char* query) {
    const char* q = query;
    std::string trimed_query;

    while (*q != '\0') {
        if (*q == '\t' || *q == '\n') {
            trimed_query.push_back(' ');        
        }
        trimed_query.push_back(*q);        
        q++;
    }
    return trimed_query;
}

int parse_line(const char* query, gunir::compiler::QueryStmt** query_stmt,
               std::ostream* output) {
    std::string trimed_query = trim_query(query);
    yyscan_t yyscanner = NULL;
    BigQueryExtra extra_record;
    YaccMemoryPool memory_pool;

    extra_record.query = trimed_query.c_str();;

    gunir_lex_init(&yyscanner);
    gunir_set_extra(&extra_record, yyscanner);

    YY_BUFFER_STATE string_buffer = gunir__scan_string(query, yyscanner); 

    gunir__switch_to_buffer(string_buffer, yyscanner);
    gunir_set_column(0, yyscanner);

    int ret = yyparse(yyscanner, &memory_pool, output);

    gunir__delete_buffer(string_buffer, yyscanner);

    gunir_lex_destroy(yyscanner);

    if (ret == 0) {
        *query_stmt = new QueryStmt(*extra_record.query_stmt);
    } else {
        *query_stmt = NULL;
    }

    memory_pool.clearMessages();
    return ret;
}

RawExpression* make_expression(RawExpression* left, Operators op,
    RawExpression* right, YaccMemoryPool* memory_pool) {
    RawExpression* expr = NewMessage<RawExpression>(memory_pool);

    if (left != NULL) {
        (expr->mutable_left())->CopyFrom(*left);
    }
    if (right != NULL) {
        (expr->mutable_right())->CopyFrom(*right);
    }

    expr->set_op(op);
    return expr;
}

OrderColumnPath* 
add_order_column_path(const ColumnPath& column_path,
    OrderType type, YaccMemoryPool* memory_pool) {
    OrderColumnPath* path = NewMessage<OrderColumnPath>(memory_pool);

    (path->mutable_path())->CopyFrom(column_path);
    path->set_type(type);
    return path;
}
