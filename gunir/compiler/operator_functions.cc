// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/operator_functions.h"

#include <cmath>
#include <cstdio>
#include <limits>

#include "gunir/compiler/add_function_helper.h"
#include "gunir/compiler/big_query_functions.h"

namespace gunir {
namespace compiler {

// ****************************Bit Wise Functions*******************************
// binary bitwise function
#define DEFINE_BINARY_BITWISE_FUNCTION(arg_type, fn_name, \
                                       bitwise_operator, bq_operator) \
    inline void arg_type##_##fn_name(BQFunctionInfo* info) { \
        Datum* datum = info->return_datum; \
        if (info->is_arg_null[0] || info->is_arg_null[1]) { \
            info->is_null = true; \
            return; \
        } \
        datum->_##arg_type = (info->arg[0]._##arg_type bitwise_operator \
                              info->arg[1]._##arg_type); \
    } \
    ADD_OPERATOR_FUNCTION(arg_type, fn_name, \
        OperatorFunctionInfo(bq_operator, \
                             #arg_type"_"#fn_name, \
                             arg_type##_##fn_name, \
                             BigQueryType::arg_type, \
                             BigQueryType::arg_type, 2));

// unary bitwise function
#define DEFINE_UNARY_BITWISE_FUNCTION(arg_type, fn_name, \
                                      bitwise_operator, bq_operator) \
    inline void arg_type##_##fn_name(BQFunctionInfo* info) { \
        Datum* datum = info->return_datum; \
        if (info->is_arg_null[0]) { \
            info->is_null = true; \
            return; \
        } \
        datum->_##arg_type = bitwise_operator(info->arg[0]._##arg_type); \
    } \
    ADD_OPERATOR_FUNCTION(arg_type, fn_name, \
        OperatorFunctionInfo(bq_operator, \
                             #arg_type"_"#fn_name, \
                             arg_type##_##fn_name, \
                             BigQueryType::arg_type, \
                             BigQueryType::arg_type, 1));

// bitwise function group
#define DEFINE_BITWISE_FUNCTION_GROUP(arg_type) \
    DEFINE_BINARY_BITWISE_FUNCTION(arg_type, BitwiseAnd, &, kBitwiseAnd) \
    DEFINE_BINARY_BITWISE_FUNCTION(arg_type, BitwiseOr, |, kBitwiseOr) \
    DEFINE_BINARY_BITWISE_FUNCTION(arg_type, BitwiseXor, ^, kBitwiseXor) \
    DEFINE_BINARY_BITWISE_FUNCTION( \
        arg_type, BitwiseLeftShift, <<, kBitwiseLeftShift) \
    DEFINE_BINARY_BITWISE_FUNCTION( \
        arg_type, BitwiseRightShift, >>, kBitwiseRightShift) \
    DEFINE_UNARY_BITWISE_FUNCTION(arg_type, BitwiseNot, ~, kBitwiseNot)

DEFINE_BITWISE_FUNCTION_GROUP(INT32)
DEFINE_BITWISE_FUNCTION_GROUP(UINT32)
DEFINE_BITWISE_FUNCTION_GROUP(INT64)
DEFINE_BITWISE_FUNCTION_GROUP(UINT64)


// *******************************Compare Functions*****************************
// compare function for integers
#define DEFINE_INTEGER_COMPARE_FUNCTION(arg_type, fn_name, \
                                        comparator, bq_operator) \
    inline void arg_type##_##fn_name(BQFunctionInfo* info) { \
        Datum* datum = info->return_datum; \
        if (info->is_arg_null[0] || info->is_arg_null[1]) { \
            datum->_BOOL = false; \
            return; \
        } \
        datum->_BOOL = (info->arg[0]._##arg_type comparator \
                        info->arg[1]._##arg_type); \
    } \
    ADD_OPERATOR_FUNCTION(arg_type, fn_name, \
        OperatorFunctionInfo(bq_operator, \
                             #arg_type"_"#fn_name, \
                             arg_type##_##fn_name, \
                             BigQueryType::BOOL, \
                             BigQueryType::arg_type, 2));

// compare function for floating
// float < >
#define DEFINE_FLOAT_COMPARE_FUNCTION(arg_type, fn_name, \
                                      comparator, bq_operator) \
    inline void arg_type##_##fn_name(BQFunctionInfo* info) { \
        Datum* datum = info->return_datum; \
        if (info->is_arg_null[0] || info->is_arg_null[1]) { \
            info->is_null = true; \
            return; \
        } \
        datum->_BOOL = (info->arg[0]._##arg_type - info->arg[1]._##arg_type \
                        comparator kFloatCompareThreshold); \
    } \
    ADD_OPERATOR_FUNCTION(arg_type, fn_name, \
        OperatorFunctionInfo(bq_operator, \
                             #arg_type"_"#fn_name, \
                             arg_type##_##fn_name, \
                             BigQueryType::BOOL, \
                             BigQueryType::arg_type, 2));

// float <= >=
#define DEFINE_FLOAT_COMPARE_EQUAL_FUNCTION(arg_type, fn_name, \
                                            comparator, bq_operator) \
    inline void arg_type##_##fn_name(BQFunctionInfo* info) { \
        Datum* datum = info->return_datum; \
        if (info->is_arg_null[0] || info->is_arg_null[1]) { \
            info->is_null = true; \
            return; \
        } \
        datum->_BOOL = (info->arg[0]._##arg_type - info->arg[1]._##arg_type \
                        comparator -1 * kFloatCompareThreshold); \
    } \
    ADD_OPERATOR_FUNCTION(arg_type, fn_name, \
        OperatorFunctionInfo(bq_operator, \
                             #arg_type"_"#fn_name, \
                             arg_type##_##fn_name, \
                             BigQueryType::BOOL, \
                             BigQueryType::arg_type, 2));

// float == !=
#define DEFINE_FLOAT_EQUAL_FUNCTION(arg_type, fn_name, \
                                    comparator, bq_operator) \
    inline void arg_type##_##fn_name(BQFunctionInfo* info) { \
        Datum* datum = info->return_datum; \
        if (info->is_arg_null[0] || info->is_arg_null[1]) { \
            info->is_null = true; \
            return; \
        } \
        datum->_BOOL = \
            (fabs(info->arg[0]._##arg_type - info->arg[1]._##arg_type) \
             comparator kFloatCompareThreshold); \
    } \
    ADD_OPERATOR_FUNCTION(arg_type, fn_name, \
        OperatorFunctionInfo(bq_operator, \
                             #arg_type"_"#fn_name, \
                             arg_type##_##fn_name, \
                             BigQueryType::BOOL, \
                             BigQueryType::arg_type, 2));

// compare function for complicate types
#define DEFINE_COMPLICATE_COMPARE_FUNCTION(arg_type, fn_name, \
                                           comparator, bq_operator) \
    inline void arg_type##_##fn_name(BQFunctionInfo* info) { \
        Datum* datum = info->return_datum; \
        if (info->is_arg_null[0] || info->is_arg_null[1]) { \
            info->is_null = true; \
            return; \
        } \
        datum->_BOOL = (*info->arg[0]._##arg_type comparator \
                        *info->arg[1]._##arg_type); \
    } \
    ADD_OPERATOR_FUNCTION(arg_type, fn_name, \
        OperatorFunctionInfo(bq_operator, \
                             #arg_type"_"#fn_name, \
                             arg_type##_##fn_name, \
                             BigQueryType::BOOL, \
                             BigQueryType::arg_type, 2));

// integer compare function group
#define DEFINE_INTEGER_COMPARE_FUNCTION_GROUP(arg_type) \
    DEFINE_INTEGER_COMPARE_FUNCTION(arg_type, Less, < , kLess) \
    DEFINE_INTEGER_COMPARE_FUNCTION(arg_type, LessEqual, <= , kLessEqual) \
    DEFINE_INTEGER_COMPARE_FUNCTION(arg_type, Greater, > , kGreater) \
    DEFINE_INTEGER_COMPARE_FUNCTION(arg_type, GreaterEqual, >= , kGreaterEqual)\
    DEFINE_INTEGER_COMPARE_FUNCTION(arg_type, Equal, == , kEqual) \
    DEFINE_INTEGER_COMPARE_FUNCTION(arg_type, NotEqual, != , kNotEqual)

// float compare function group
#define DEFINE_FLOAT_COMPARE_FUNCTION_GROUP(arg_type) \
    DEFINE_FLOAT_COMPARE_FUNCTION(arg_type, Less, < , kLess) \
    DEFINE_FLOAT_COMPARE_FUNCTION(arg_type, Greater, > , kGreater) \
    DEFINE_FLOAT_COMPARE_EQUAL_FUNCTION(arg_type, LessEqual, < , kLessEqual) \
    DEFINE_FLOAT_COMPARE_EQUAL_FUNCTION( \
        arg_type, GreaterEqual, > , kGreaterEqual) \
    DEFINE_FLOAT_EQUAL_FUNCTION(arg_type, Equal, < , kEqual) \
    DEFINE_FLOAT_EQUAL_FUNCTION(arg_type, NotEqual, > , kNotEqual)

// complicate compare function group
#define DEFINE_COMPLICATE_COMPARE_FUNCTION_GROUP(arg_type) \
    DEFINE_COMPLICATE_COMPARE_FUNCTION(arg_type, Less, < , kLess) \
    DEFINE_COMPLICATE_COMPARE_FUNCTION(arg_type, LessEqual, <= , kLessEqual) \
    DEFINE_COMPLICATE_COMPARE_FUNCTION(arg_type, Greater, > , kGreater) \
    DEFINE_COMPLICATE_COMPARE_FUNCTION( \
        arg_type, GreaterEqual, >= , kGreaterEqual) \
    DEFINE_COMPLICATE_COMPARE_FUNCTION(arg_type, Equal, == , kEqual) \
    DEFINE_COMPLICATE_COMPARE_FUNCTION(arg_type, NotEqual, != , kNotEqual)

DEFINE_INTEGER_COMPARE_FUNCTION_GROUP(INT32)
DEFINE_INTEGER_COMPARE_FUNCTION_GROUP(UINT32)
DEFINE_INTEGER_COMPARE_FUNCTION_GROUP(INT64)
DEFINE_INTEGER_COMPARE_FUNCTION_GROUP(UINT64)

DEFINE_FLOAT_COMPARE_FUNCTION_GROUP(FLOAT)
DEFINE_FLOAT_COMPARE_FUNCTION_GROUP(DOUBLE)

DEFINE_COMPLICATE_COMPARE_FUNCTION_GROUP(STRING)


// ***************************Logic Functions***********************************
// binary logic function
#define DEFINE_BINARY_LOGIC_FUNCTION(arg_type, fn_name, \
                                     logic_operator, bq_operator) \
    inline void arg_type##_##fn_name(BQFunctionInfo* info) { \
        Datum* datum = info->return_datum; \
        if (info->is_arg_null[0] || info->is_arg_null[1]) { \
            info->is_null = true; \
            return; \
        } \
        datum->_##arg_type = (info->arg[0]._##arg_type logic_operator \
                              info->arg[1]._##arg_type); \
    } \
    ADD_OPERATOR_FUNCTION(arg_type, fn_name, \
        OperatorFunctionInfo(bq_operator, \
                             #arg_type"_"#fn_name, \
                             arg_type##_##fn_name, \
                             BigQueryType::BOOL, \
                             BigQueryType::arg_type, 2));

// unary logic function
#define DEFINE_UNARY_LOGIC_FUNCTION(arg_type, fn_name, \
                                    logic_operator, bq_operator) \
    inline void arg_type##_##fn_name(BQFunctionInfo* info) { \
        Datum* datum = info->return_datum; \
        if (info->is_arg_null[0]) { \
            info->is_null = true; \
            return; \
        } \
        datum->_##arg_type = logic_operator(info->arg[0]._##arg_type); \
    } \
    ADD_OPERATOR_FUNCTION(arg_type, fn_name, \
        OperatorFunctionInfo(bq_operator, \
                             #arg_type"_"#fn_name, \
                             arg_type##_##fn_name, \
                             BigQueryType::BOOL, \
                             BigQueryType::arg_type, 1));

DEFINE_BINARY_LOGIC_FUNCTION(BOOL, LogicAnd, && , kLogicalAnd)
DEFINE_BINARY_LOGIC_FUNCTION(BOOL, LogicOr, || , kLogicalOr)
DEFINE_UNARY_LOGIC_FUNCTION(BOOL, LogicNot, !, kLogicalNot)


// ***********************Arithmetical FUNCTIONS********************************
// arithmetic function for pod types, + - *
// TODO(codingliu): return types are double, change to proper type next version
#define DEFINE_ARITHMETICAL_FUNCTION(arg_type, fn_name, \
                                     arithmetical_operator, bq_operator) \
    inline void arg_type##_##fn_name(BQFunctionInfo* info) { \
        Datum* datum = info->return_datum; \
        if (info->is_arg_null[0] || info->is_arg_null[1]) { \
            info->is_null = true; \
            return; \
        } \
        datum->_DOUBLE = (info->arg[0]._##arg_type arithmetical_operator \
                          info->arg[1]._##arg_type); \
    } \
    ADD_OPERATOR_FUNCTION(arg_type, fn_name, \
        OperatorFunctionInfo(bq_operator, \
                             #arg_type"_"#fn_name, \
                             arg_type##_##fn_name, \
                             BigQueryType::DOUBLE, \
                             BigQueryType::arg_type, 2));

// for div and remainder, second param != 0
#define DEFINE_ARITHMETICAL_NONZERO_FUNCTION(arg_type, fn_name, \
                                             arithmetical_operator, \
                                             bq_operator) \
    inline void arg_type##_##fn_name(BQFunctionInfo* info) { \
        Datum* datum = info->return_datum; \
        if (info->is_arg_null[0] || info->is_arg_null[1]) { \
            info->is_null = true; \
            return; \
        } \
        if (fabs(info->arg[1]._##arg_type) < kFloatCompareThreshold) { \
            datum->_DOUBLE = NAN; \
            return; \
        } \
        datum->_DOUBLE = (info->arg[0]._##arg_type arithmetical_operator \
                          info->arg[1]._##arg_type); \
    } \
    ADD_OPERATOR_FUNCTION(arg_type, fn_name, \
        OperatorFunctionInfo(bq_operator, \
                             #arg_type"_"#fn_name, \
                             arg_type##_##fn_name, \
                             BigQueryType::DOUBLE, \
                             BigQueryType::arg_type, 2));

// arithmetic function group
#define DEFINE_ARITHMETICAL_FUNCTION_GROUP(arg_type) \
    DEFINE_ARITHMETICAL_FUNCTION(arg_type, NumericAdd, + , kAdd) \
    DEFINE_ARITHMETICAL_FUNCTION(arg_type, NumericSub, - , kSub) \
    DEFINE_ARITHMETICAL_FUNCTION(arg_type, NumericMul, * , kMul) \
    DEFINE_ARITHMETICAL_NONZERO_FUNCTION(arg_type, NumericDiv, / , kDiv)

DEFINE_ARITHMETICAL_NONZERO_FUNCTION(INT32, NumericRemainder, % , kRemainder)
DEFINE_ARITHMETICAL_NONZERO_FUNCTION(UINT32, NumericRemainder, % , kRemainder)
DEFINE_ARITHMETICAL_NONZERO_FUNCTION(INT64, NumericRemainder, % , kRemainder)
DEFINE_ARITHMETICAL_NONZERO_FUNCTION(UINT64, NumericRemainder, % , kRemainder)

DEFINE_ARITHMETICAL_FUNCTION_GROUP(INT32)
DEFINE_ARITHMETICAL_FUNCTION_GROUP(UINT32)
DEFINE_ARITHMETICAL_FUNCTION_GROUP(INT64)
DEFINE_ARITHMETICAL_FUNCTION_GROUP(UINT64)
DEFINE_ARITHMETICAL_FUNCTION_GROUP(FLOAT)
DEFINE_ARITHMETICAL_FUNCTION_GROUP(DOUBLE)

// String Operator functions
#define DEFINE_STRING_CONCAT_FUNCTION(arg_type, fn_name, \
                                      arithmetical_operator, bq_operator) \
    inline void arg_type##_##fn_name(BQFunctionInfo* info) { \
        Datum* datum = info->return_datum; \
        if (info->is_arg_null[0] || info->is_arg_null[1]) { \
            info->is_null = true; \
            return; \
        } \
        *datum->_STRING = (*info->arg[0]._##arg_type arithmetical_operator \
                           *info->arg[1]._##arg_type); \
    } \
    ADD_OPERATOR_FUNCTION(arg_type, fn_name, \
                          OperatorFunctionInfo(bq_operator, \
                          #arg_type"_"#fn_name, \
                          arg_type##_##fn_name, \
                          BigQueryType::STRING, \
                          BigQueryType::arg_type, 2));
DEFINE_STRING_CONCAT_FUNCTION(STRING, StringConcat, +, kAdd)

#define DEFINE_STRING_CONTAINS_FUNCTION(arg_type, fn_name, \
                                        arithmetical_operator, bq_operator) \
    inline void arg_type##_##fn_name(BQFunctionInfo* info) { \
        Datum* datum = info->return_datum; \
        if (info->is_arg_null[0] || info->is_arg_null[1]) { \
            info->is_null = true; \
            return; \
        } \
        const std::string& s1 = *(info->arg[0]._##arg_type); \
        const std::string& s2 = *(info->arg[1]._##arg_type); \
        if (s1.find(s2) != std::string::npos) { \
            datum->_BOOL = true; \
        } else {\
            datum->_BOOL = false; \
        } \
    } \
    ADD_OPERATOR_FUNCTION(arg_type, fn_name, \
                          OperatorFunctionInfo(bq_operator, \
                          #arg_type"_"#fn_name, \
                          arg_type##_##fn_name, \
                          BigQueryType::BOOL, \
                          BigQueryType::arg_type, 2));
DEFINE_STRING_CONTAINS_FUNCTION(STRING, StringContains, CONTAINS, kContains)

} // namespace compiler
} // namespace gunir

