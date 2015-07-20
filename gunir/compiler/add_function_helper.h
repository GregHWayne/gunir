// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_ADD_FUNCTION_HELPER_H
#define  GUNIR_COMPILER_ADD_FUNCTION_HELPER_H

#include "toft/base/singleton.h"

#include "gunir/compiler/aggregate_functions.h"
#include "gunir/compiler/function_resolver.h"
#include "gunir/compiler/operator_functions.h"

namespace gunir {
namespace compiler {

class AddFunctionHelper {
public:
    inline static AddFunctionHelper* HelpAddOperatorFunction(
        OperatorFunctionInfo info);

    inline static AddFunctionHelper* HelpAddAggregateFunction(
        AggregateFunctionInfo info);

    inline static AddFunctionHelper* HelpAddPlainFunction(
        PlainFunctionInfo info);

    inline static AddFunctionHelper* HelpAddCastFunction(
        CastFunctionInfo cast_function_info);
};

AddFunctionHelper* AddFunctionHelper::HelpAddOperatorFunction(
    OperatorFunctionInfo info) {

    FunctionResolver* resolver = toft::Singleton<FunctionResolver>::Instance();
    resolver->AddOperatorFunction(info.m_op, info);
    return NULL;
}

AddFunctionHelper* AddFunctionHelper::HelpAddAggregateFunction(
    AggregateFunctionInfo info) {

    FunctionResolver* resolver = toft::Singleton<FunctionResolver>::Instance();
    resolver->AddAggregateFunction(info.m_fn_name, info);
    return NULL;
}

AddFunctionHelper* AddFunctionHelper::HelpAddPlainFunction(
    PlainFunctionInfo info) {

    FunctionResolver* resolver = toft::Singleton<FunctionResolver>::Instance();
    resolver->AddPlainFunction(info.m_fn_name, info);
    return NULL;
}

AddFunctionHelper* AddFunctionHelper::HelpAddCastFunction(
    CastFunctionInfo info) {

    FunctionResolver* resolver = toft::Singleton<FunctionResolver>::Instance();
    resolver->AddCastFunction(info.m_arg_type, info);
    return NULL;
}

// we use type_fnname to represent a function
// e.g. the less function of int32 is INT32_Less
#define ADD_OPERATOR_FUNCTION(arg_type, fn_name, operator_function_info) \
    static AddFunctionHelper* operator_function_helper_##arg_type##_##fn_name = \
        AddFunctionHelper::HelpAddOperatorFunction(operator_function_info)

#define ADD_AGGREGATE_FUNCTION(fn_name, aggregate_function_info) \
    static AddFunctionHelper* aggregate_function_helper_##fn_name = \
        AddFunctionHelper::HelpAddAggregateFunction(aggregate_function_info)

#define ADD_PLAIN_FUNCTION(fn_name, plain_function_info) \
    static AddFunctionHelper* plain_function_helper_##fn_name = \
        AddFunctionHelper::HelpAddPlainFunction(plain_function_info)

#define ADD_CAST_FUNCTION(fn_name, cast_function_info) \
    static AddFunctionHelper* cast_function_helper_##fn_name = \
        AddFunctionHelper::HelpAddCastFunction(cast_function_info)

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_ADD_FUNCTION_HELPER_H
