// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/plain_functions.h"

#include "gunir/compiler/add_function_helper.h"
#include "gunir/compiler/big_query_functions.h"

namespace gunir {
namespace compiler {

void SUBSTR(BQFunctionInfo* info) {
    Datum* d = info->return_datum;

    if (info->is_arg_null[0] || info->is_arg_null[1]) {
        info->is_null = true;
        return;
    }

    const std::string* str = info->arg[0]._STRING;
    int64_t start = info->arg[1]._INT64;
    int64_t length = info->arg[2]._INT64;

    *(d->_STRING) = str->substr(start, length);
}

BQType SUBSTR_arg_types[] = {
    BigQueryType::STRING,
    BigQueryType::INT64,
    BigQueryType::INT64
};
ADD_PLAIN_FUNCTION(SUBSTR, PlainFunctionInfo(
        "SUBSTR", SUBSTR, SUBSTR_arg_types, BigQueryType::STRING,
        sizeof(SUBSTR_arg_types) / sizeof(SUBSTR_arg_types[0])));

void CONCAT(BQFunctionInfo* info) {
    Datum* d = info->return_datum;

    if (info->is_arg_null[0] || info->is_arg_null[1]) {
        info->is_null = true;
        return;
    }

    *(d->_STRING) = (*info->arg[0]._STRING + *info->arg[1]._STRING);
    return;
}

BQType CONCAT_arg_types[] = {
    BigQueryType::STRING,
    BigQueryType::STRING
};
ADD_PLAIN_FUNCTION(CONCAT, PlainFunctionInfo(
        "CONCAT", CONCAT, CONCAT_arg_types, BigQueryType::STRING,
        sizeof(CONCAT_arg_types) / sizeof(CONCAT_arg_types[0])));

void LENGTH(BQFunctionInfo* info) {
    Datum* d = info->return_datum;

    if (info->is_arg_null[0]) {
        info->is_null = true;
        return;
    }

    d->_UINT32 = (info->arg[0]._STRING)->size();
    return;
}

BQType LENGTH_arg_types[] = {
    BigQueryType::STRING
};
ADD_PLAIN_FUNCTION(LENGTH, PlainFunctionInfo(
        "LENGTH", LENGTH, LENGTH_arg_types, BigQueryType::UINT32,
        sizeof(LENGTH_arg_types) / sizeof(LENGTH_arg_types[0])));

} // namespace compiler
} // namespace gunir

