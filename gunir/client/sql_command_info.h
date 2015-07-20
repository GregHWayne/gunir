// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_CLIENT_SQL_COMMAND_INFO_H
#define  GUNIR_CLIENT_SQL_COMMAND_INFO_H

#include <string>

namespace gunir {

#define LIGHT_GREEN "\033[1;32m"
#define LIGHT_YELLOW "\033[1;33m"
#define LIGHT_BLUE "\033[1;34m"
#define LIGHT_RED "\033[1;31m"

struct CommandInfo {
    const char* command;
    const char* usage;
    const char* descr;
};

static const CommandInfo g_command_lists[] =
{
    {"create", "create TABLE from \"INPUT\" \"SCHEMA\" \"MESSAGE\"", ""},
    {"define", "define tablename as filepath", ""},
    {"drop", "drop TABLE", ""},
    {"select", "select COLUMN from TABLE", ""},
    {"as", "select COLUMN as ALIAS from TABLE", ""},
    {"from", "select COLUMN from TABLE", ""},
    {"where", "where CONDITIONAL", ""},
    {"within", "within COLUMN or RECORD", ""},
    {"distinct", "select distinct docid from test", ""},
    {"record", "within COLUMN or RECORD", ""},
    {"groupby", "groupby COLUMN", ""},
    {"having", "having CONDITIONAL", ""},
    {"orderby", "orderby COLUMN", ""},
    {"limit", "limit NUMBER DESC ASC", ""},
    {"join", "select COLUMN from T1 join T2", ""},
    {"on", "select COLUMN from T1 join T2 on T1.a = T2.b", ""},
    {"inner", "select COLUMN from T1 inner join T2", ""},
    {"outer", "select COLUMN from T1 outer join T2", ""},
    {"left", "select COLUMN from T1 left join T2", ""},
    {"help", "print help infomation", ""},
    {"show", "print table infomation", ""},
    {"history", "print query history", ""},
    {"quit", "quit shell", ""},
    {"exit", "quit shell", ""},

    {"or", "select COLUMN from TABLE where CONDITIONAL or CONDITIONAL", ""},
    {"and", "select COLUMN from TABLE where CONDITIONAL and CONDITIONAL", ""},
    {"not", "select COLUMN from TABLE where not CONDITIONAL", ""},
    {"true", "select COLUMN from TABLE where CONDITIONAL = true", ""},
    {"false", "select COLUMN from TABLE where CONDITIONAL = false", ""},
    {"div", "select COLUMN from TABLE where COLUMN div 10 = 1", ""},
    {"contains", "select COLUMN from TABLE where COLUMN contains STRING", ""},

    {"concat", "function for concat two string : concat(str1, str2)", ""},
    {"length", "function for get string length: length(str)", ""},
    {"substr", "function for get sub string : "
        "substr(str1, start_pos, length)", ""},

    {"count", "select count(COLUMN) as cnt from TABLE", ""},
    {"sum", "select sum(COLUMN) as cnt from TABLE", ""},
    {"min", "select min(COLUMN) as cnt from TABLE", ""},
    {"max", "select max(COLUMN) as cnt from TABLE", ""},
    {"avg", "select avg(COLUMN) as cnt from TABLE", ""},
    {NULL, NULL, NULL}
};


} // namespace gunir

#endif  // GUNIR_CLIENT_SQL_COMMAND_INFO_H
