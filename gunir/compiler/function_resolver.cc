// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/function_resolver.h"

#include "toft/base/singleton.h"

#include "gunir/compiler/type_resolver.h"

#include "thirdparty/glog/logging.h"

namespace gunir {
namespace compiler {

FunctionResolver::FunctionResolver() {}
FunctionResolver::~FunctionResolver() {}

// this function will be call before main,
// donot use any global variables(e.g. glog)
void FunctionResolver::AddOperatorFunction(
    Operators op, OperatorFunctionInfo info) {

    std::vector<OperatorFunctionInfo>& finfo_list = m_op_functions[op];
    finfo_list.push_back(info);
}

void FunctionResolver::AddAggregateFunction(
    const std::string& fn_name, AggregateFunctionInfo info) {

    m_agg_functions[fn_name].push_back(info);
}

void FunctionResolver::AddPlainFunction(
    const std::string& fn_name, PlainFunctionInfo info) {

    m_plain_functions.insert(std::make_pair(fn_name, info));
}

void FunctionResolver::AddCastFunction(
    BQType from, CastFunctionInfo info) {

    std::vector<CastFunctionInfo>& finfo_list = m_cast_functions[from];
    finfo_list.push_back(info);
}

const PlainFunctionInfo* FunctionResolver::ResolvePlainFunction(
    const std::string& fn_name, const std::vector<BQType>& arg_types,
    std::vector<BQType>* cast_info) const {

    std::map<std::string, PlainFunctionInfo>::const_iterator iter;

    iter = m_plain_functions.find(fn_name);
    if (iter == m_plain_functions.end()) {
        return NULL;
    }

    const PlainFunctionInfo* plain_fn = &iter->second;
    if (plain_fn->m_arg_types.size() != arg_types.size()) {
        return NULL;
    }

    for (size_t i = 0; i < arg_types.size(); ++i) {
        if (!toft::Singleton<TypeResolver>::Instance()->
            IsTypeMatch(arg_types[i], plain_fn->m_arg_types[i])) {
            return NULL;
        }
        cast_info->push_back(plain_fn->m_arg_types[i]);
    }
    return plain_fn;
}

const AggregateFunctionInfo* FunctionResolver::ResolveAggregateFunction(
    const std::string& fn_name, BQType arg_type,
    std::vector<BQType>* cast_info) const {
    typedef std::vector<AggregateFunctionInfo> VectorAggInfo;
    std::map<std::string, VectorAggInfo>::const_iterator iter;

    iter = m_agg_functions.find(fn_name);
    if (iter == m_agg_functions.end()) {
        return NULL;
    }

    const std::vector<AggregateFunctionInfo>& agg_infos = iter->second;

    for (size_t i = 0; i < agg_infos.size(); ++i) {
        const AggregateFunctionInfo* agg_fn = &agg_infos[i];

        if (toft::Singleton<TypeResolver>::Instance()->
            IsTypeMatch(arg_type, agg_fn->m_input_type)) {
            cast_info->push_back(agg_fn->m_input_type);
            return agg_fn;
        }
    }
    return NULL;
}

const OperatorFunctionInfo* FunctionResolver::ResolveOperatorFunction(
    Operators op, const std::vector<BQType>& arg_type_list,
    std::vector<BQType>* cast_info) const {
    std::map<Operators, std::vector<OperatorFunctionInfo> >::const_iterator iter;

    iter = m_op_functions.find(op);
    if (iter == m_op_functions.end()) {
        return NULL;
    }

    const std::vector<OperatorFunctionInfo>& op_fn_list = iter->second;
    std::vector<OperatorFunctionInfo>::const_iterator fn_iter;

    for (fn_iter = op_fn_list.begin(); fn_iter != op_fn_list.end(); ++fn_iter) {
        const OperatorFunctionInfo* op_fn = &(*fn_iter);

        cast_info->clear();
        if (IsArgumentMatches(arg_type_list, op_fn->m_arg_type, cast_info)) {
            return op_fn;
        }
    }
    return NULL;
}


const CastFunctionInfo* FunctionResolver::ResolveCastFunction(
    BQType from, BQType to) const {
    std::map<BQType, std::vector<CastFunctionInfo> >::const_iterator iter;

    iter = m_cast_functions.find(from);
    if (iter == m_cast_functions.end()) {
        return NULL;
    }

    const std::vector<CastFunctionInfo>& cast_fn_list = iter->second;
    std::vector<CastFunctionInfo>::const_iterator fn_iter;

    for (fn_iter = cast_fn_list.begin();
         fn_iter != cast_fn_list.end();
         ++fn_iter) {
        const CastFunctionInfo* cast_fn = &(*fn_iter);

        if (cast_fn->m_return_type == to)
            return cast_fn;
    }
    return NULL;
}

bool FunctionResolver::IsArgumentMatches(
    const std::vector<BQType>& arg_type_list, BQType fn_arg_type,
    std::vector<BQType>* cast_info) const {

    std::vector<BQType>::const_iterator iter;
    const TypeResolver* type_resolver = toft::Singleton<TypeResolver>::Instance();

    for (iter = arg_type_list.begin(); iter != arg_type_list.end(); ++iter) {
        if (!type_resolver->IsTypeMatch(*iter, fn_arg_type)) {
            return false;
        }
        cast_info->push_back(fn_arg_type);
    }
    return true;
}

} // namespace compiler
} // namespace gunir

