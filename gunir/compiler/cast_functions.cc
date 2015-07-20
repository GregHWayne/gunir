// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/cast_functions.h"

#include "gunir/compiler/add_function_helper.h"
#include "gunir/compiler/big_query_functions.h"

namespace gunir {
namespace compiler {

#define DEFINE_CAST_FUNCTION(BQFromType, BQToType, ToCPPType) \
    void Cast_##BQFromType##_To_##BQToType(BQFunctionInfo* info) { \
        if (info->is_arg_null[0]) { \
            info->is_null = true; \
            return; \
        } \
        Datum* new_value = info->return_datum; \
        new_value->_##BQToType = \
            static_cast<ToCPPType>(info->arg[0]._##BQFromType); \
    } \
    ADD_CAST_FUNCTION(Cast_##BQFromType##_To_##BQToType, CastFunctionInfo( \
        "Cast_"#BQFromType"_To_"#BQToType, Cast_##BQFromType##_To_##BQToType, \
        BigQueryType::BQFromType, BigQueryType::BQToType))

DEFINE_CAST_FUNCTION(INT32, INT64, int64_t);
DEFINE_CAST_FUNCTION(INT32, FLOAT, float);
DEFINE_CAST_FUNCTION(INT32, DOUBLE, double);

DEFINE_CAST_FUNCTION(UINT32, INT64, int64_t);
DEFINE_CAST_FUNCTION(UINT32, UINT64, uint64_t);
DEFINE_CAST_FUNCTION(UINT32, FLOAT, float);
DEFINE_CAST_FUNCTION(UINT32, DOUBLE, double);

DEFINE_CAST_FUNCTION(INT64, FLOAT, float);
DEFINE_CAST_FUNCTION(INT64, DOUBLE, double);

DEFINE_CAST_FUNCTION(UINT64, FLOAT, float);
DEFINE_CAST_FUNCTION(UINT64, DOUBLE, double);

DEFINE_CAST_FUNCTION(FLOAT, DOUBLE, double);

#define DEFINE_VOID_POINTER_CAST_FUNCTION(BQFromType, BQToType) \
    void Cast_##BQFromType##_To_##BQToType(BQFunctionInfo* info) { \
        if (info->is_arg_null[0]) { \
            info->is_null = true; \
            return; \
        } \
    } \
    ADD_CAST_FUNCTION(Cast_##BQFromType##_To_##BQToType, CastFunctionInfo( \
        "Cast_"#BQFromType"_To_"#BQToType, Cast_##BQFromType##_To_##BQToType, \
        BigQueryType::BQFromType, BigQueryType::BQToType))

DEFINE_VOID_POINTER_CAST_FUNCTION(INT32, BYTES);
DEFINE_VOID_POINTER_CAST_FUNCTION(UINT32, BYTES);
DEFINE_VOID_POINTER_CAST_FUNCTION(INT64, BYTES);
DEFINE_VOID_POINTER_CAST_FUNCTION(UINT64, BYTES);
DEFINE_VOID_POINTER_CAST_FUNCTION(FLOAT, BYTES);
DEFINE_VOID_POINTER_CAST_FUNCTION(DOUBLE, BYTES);
DEFINE_VOID_POINTER_CAST_FUNCTION(BOOL, BYTES);
DEFINE_VOID_POINTER_CAST_FUNCTION(STRING, BYTES);

} // namespace compiler
} // namespace gunir

