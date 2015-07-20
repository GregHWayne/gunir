// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <cmath>
#include <limits>

#include "gunir/compiler/add_function_helper.h"
#include "gunir/compiler/aggregate_functions.h"

namespace gunir {
namespace compiler {

void DOUBLE_Sum_Trans(BQFunctionInfo* info) {
    Datum* new_value = info->return_datum;

    // all inputs are null so far
    if (info->is_arg_null[0]) {

        if (info->is_arg_null[1]) {
            info->is_null = true;
        } else {
            *new_value = info->arg[1];
        }
        return;
    }

    if (info->is_arg_null[1]) {
        *new_value = info->arg[0];
    } else {
        new_value->_DOUBLE = info->arg[0]._DOUBLE + info->arg[1]._DOUBLE;
    }
}
ADD_AGGREGATE_FUNCTION(SUM, AggregateFunctionInfo("SUM",
                                                  DOUBLE_Sum_Trans, // trans
                                                  DOUBLE_Sum_Trans, // merge
                                                  NULL, // final
                                                  BigQueryType::DOUBLE,
                                                  BigQueryType::DOUBLE,
                                                  BigQueryType::DOUBLE));

void INT64_Count_Trans(BQFunctionInfo* info) {
    Datum* new_value = info->return_datum;

    if (info->is_arg_null[0]) {
        if (info->is_arg_null[1]) {
            info->is_null = true;
        } else {
            new_value->_INT64 = 1;
        }
        return;
    }

    if (info->is_arg_null[1]) {
        *new_value = info->arg[0];
    } else {
        new_value->_INT64 = info->arg[0]._INT64 + 1;
    }
}

void INT64_Count_Merge(BQFunctionInfo* info) {
    Datum* new_value = info->return_datum;

    if (info->is_arg_null[0]) {
        if (info->is_arg_null[1]) {
            info->is_null = true;
        } else {
            new_value->_INT64 = info->arg[1]._INT64;
        }
        return;
    }

    if (info->is_arg_null[1]) {
        *new_value = info->arg[0];
    } else {
        new_value->_INT64 = info->arg[0]._INT64 + info->arg[1]._INT64;
    }
}
ADD_AGGREGATE_FUNCTION(COUNT, AggregateFunctionInfo("COUNT",
                                                    INT64_Count_Trans, // trans
                                                    INT64_Count_Merge, // merge
                                                    NULL, // final
                                                    BigQueryType::BYTES,
                                                    BigQueryType::INT64,
                                                    BigQueryType::INT64));
/*
 * data_type: INT32, UINT32, INT64, UINT64, FLOAT, DOUBLE
 * compare_type: LESS, GREATER
 * agg_name: MIN, MAX
 * OP: <, >
 */

#define MIN_MAX_AGGREGATE_STRING_BYTES(data_type, compare_type, agg_name, OP) \
void data_type##_##compare_type##_Trans(BQFunctionInfo* info) { \
    Datum* new_value = info->return_datum; \
    if (info->is_arg_null[0] && info->is_arg_null[1]) { \
        info->is_null = true; \
        return; \
    } \
    if (info->is_arg_null[0]) { \
        new_value->CopyFrom((info->arg[1]), info->arg_type[1]);\
        return; \
    } \
    if (info->is_arg_null[1]) { \
        new_value->CopyFrom((info->arg[0]), info->arg_type[0]);\
        return; \
    } \
    if (*(info->arg[0]._##data_type) OP *(info->arg[1]._##data_type)) { \
        new_value->CopyFrom((info->arg[0]), info->arg_type[0]);\
    } else { \
        new_value->CopyFrom((info->arg[1]), info->arg_type[1]);\
    } \
} \
ADD_AGGREGATE_FUNCTION(data_type##_##agg_name, \
                       AggregateFunctionInfo(#agg_name, \
                                             data_type##_##compare_type##_Trans, \
                                             data_type##_##compare_type##_Trans, \
                                             NULL, \
                                             BigQueryType::data_type, \
                                             BigQueryType::data_type, \
                                             BigQueryType::data_type))

#define MIN_MAX_AGGREGATE(data_type, compare_type, agg_name, OP) \
void data_type##_##compare_type##_Trans(BQFunctionInfo* info) { \
    Datum* new_value = info->return_datum; \
    if (info->is_arg_null[0] && info->is_arg_null[1]) { \
        info->is_null = true; \
        return; \
    } \
    if (info->is_arg_null[0]) { \
        new_value->CopyFrom((info->arg[1]), info->arg_type[1]);\
        return; \
    } \
    if (info->is_arg_null[1]) { \
        new_value->CopyFrom((info->arg[0]), info->arg_type[0]);\
        return; \
    } \
    if (info->arg[0]._##data_type OP info->arg[1]._##data_type) { \
        new_value->CopyFrom((info->arg[0]), info->arg_type[0]);\
    } else { \
        new_value->CopyFrom((info->arg[1]), info->arg_type[1]);\
    } \
} \
ADD_AGGREGATE_FUNCTION(data_type##_##agg_name, \
                       AggregateFunctionInfo(#agg_name, \
                                             data_type##_##compare_type##_Trans, \
                                             data_type##_##compare_type##_Trans, \
                                             NULL, \
                                             BigQueryType::data_type, \
                                             BigQueryType::data_type, \
                                             BigQueryType::data_type))


#define ALL_TYPE_MIN_MAX_AGGREGATE(compare_type, agg_name, OP) \
    MIN_MAX_AGGREGATE(INT32, compare_type, agg_name, OP); \
    MIN_MAX_AGGREGATE(UINT32, compare_type, agg_name, OP); \
    MIN_MAX_AGGREGATE(INT64, compare_type, agg_name, OP); \
    MIN_MAX_AGGREGATE(UINT64, compare_type, agg_name, OP); \
    MIN_MAX_AGGREGATE(FLOAT, compare_type, agg_name, OP); \
    MIN_MAX_AGGREGATE(DOUBLE, compare_type, agg_name, OP); \
    MIN_MAX_AGGREGATE_STRING_BYTES(STRING, compare_type, agg_name, OP); \
    MIN_MAX_AGGREGATE_STRING_BYTES(BYTES, compare_type, agg_name, OP);

ALL_TYPE_MIN_MAX_AGGREGATE(LESS, MIN, <); // NOLINT
ALL_TYPE_MIN_MAX_AGGREGATE(GREATER, MAX, >);

struct DoubleTransType {
    int64_t count;
    double sum;
};

void DOUBLE_Average_Trans(BQFunctionInfo* info) {
    Datum* new_value = info->return_datum;
    if (info->is_arg_null[0] && info->is_arg_null[1]) {
        info->is_null = true;
        return;
    }

    DoubleTransType* new_t =
        static_cast<DoubleTransType*>((new_value->_BYTES)->m_data);
    DoubleTransType* old_t =
        static_cast<DoubleTransType*>((info->arg[0]._BYTES)->m_data);

    if (info->is_arg_null[1]) {
        *new_t = *old_t;
        return;
    }

    new_t->count = old_t->count + 1;
    new_t->sum = old_t->sum + info->arg[1]._DOUBLE;
}

void DOUBLE_Average_Merge(BQFunctionInfo* info) {
    Datum* new_value = info->return_datum;
    if (info->is_arg_null[0] && info->is_arg_null[1]) {
        info->is_null = true;
        return;
    }

    DoubleTransType* new_t =
        static_cast<DoubleTransType*>((new_value->_BYTES)->m_data);
    DoubleTransType* old_t =
        static_cast<DoubleTransType*>((info->arg[0]._BYTES)->m_data);
    DoubleTransType* merge_t =
        static_cast<DoubleTransType*>((info->arg[1]._BYTES)->m_data);

    if (info->is_arg_null[1]) {
        *new_t = *old_t;
        return;
    }

    new_t->count = old_t->count + merge_t->count;
    new_t->sum = old_t->sum + merge_t->sum;
}

void DOUBLE_Average_Final(BQFunctionInfo* info) {
    if (info->is_arg_null[0]) {
        info->is_null = true;
        return;
    }

    Datum* new_value = info->return_datum;
    DoubleTransType* t =
        static_cast<DoubleTransType*>((info->arg[0]._BYTES)->m_data);

    if (t->count == 0) {
        new_value->_DOUBLE = NAN;
    } else {
        new_value->_DOUBLE = t->sum / t->count;
    }
}

ADD_AGGREGATE_FUNCTION(AVG, AggregateFunctionInfo("AVG",
                                                  DOUBLE_Average_Trans,
                                                  DOUBLE_Average_Merge,
                                                  DOUBLE_Average_Final,
                                                  BigQueryType::DOUBLE,
                                                  sizeof(DoubleTransType),
                                                  BigQueryType::DOUBLE));

} // namespace compiler
} // namespace gunir
