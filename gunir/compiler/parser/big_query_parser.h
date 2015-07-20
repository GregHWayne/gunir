// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_PARSER_BIG_QUERY_PARSER_H
#define  GUNIR_COMPILER_PARSER_BIG_QUERY_PARSER_H

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "gunir/compiler/parser/select_stmt.pb.h"

#define YYLEX_PARAM yyscanner, memory_pool, output
#define yyconst const

typedef void* yyscan_t;
class YaccMemoryPool;
struct yy_buffer_state;
typedef struct yy_buffer_state *YY_BUFFER_STATE;

struct BigQueryExtra {
    const char* query;
    gunir::compiler::QueryStmt* query_stmt;
};
#define YY_EXTRA_TYPE BigQueryExtra*

// lex yacc setup/endup functions
int gunir_lex(yyscan_t, YaccMemoryPool*, std::ostream* output);
int gunir_lex_init(yyscan_t* ptr_yy_globals);
int gunir_lex_destroy(yyscan_t yyscanner);

extern "C" int gunir_wrap(void* p);
void yyerror(yyscan_t, YaccMemoryPool*,
             std::ostream*, const char* s);

// scan string
YY_BUFFER_STATE gunir__scan_string(yyconst char* yy_str, yyscan_t yyscanner);
void gunir__switch_to_buffer(YY_BUFFER_STATE new_buffer, yyscan_t yyscanner);
void gunir__delete_buffer(YY_BUFFER_STATE b, yyscan_t yyscanner);

// keep column for print error location
int gunir_get_column(yyscan_t yyscanner);
void gunir_set_column(int column_no, yyscan_t yyscanner);

// set extra type to for return QueryStmt*
YY_EXTRA_TYPE gunir_get_extra(yyscan_t yyscanner);
void gunir_set_extra(YY_EXTRA_TYPE user_defined, yyscan_t yyscanner);

// interface for user
int parse_line(const char* query, gunir::compiler::QueryStmt** query_stmt,
               std::ostream* output = &(std::cout));

// Yacc Memory Manager, the inner of yacc is state machine, if parse failed,
// we can't trace back to release allocated memory, so we keep the allocated
// memory here, and release them at the end of yyparse
class YaccMemoryPool {
typedef ::google::protobuf::Message PBMessage;

public:
    void addMessage(const PBMessage* m) {
        m_message_list.push_back(m);
    }

    void clearMessages() {
        std::vector<const PBMessage*>::iterator iter;

        for (iter = m_message_list.begin();
             iter != m_message_list.end();
             ++iter) {

            delete *iter;
            *iter = NULL;
        }
        m_message_list.clear();
    }

    ~YaccMemoryPool() { clearMessages(); }

private:
    std::vector<const PBMessage*> m_message_list;
};

template<class Message>
Message* NewMessage(YaccMemoryPool* manager) {
    Message* message = new Message();
    manager->addMessage(message);
    return message;
}

#endif  // GUNIR_COMPILER_PARSER_BIG_QUERY_PARSER_H
