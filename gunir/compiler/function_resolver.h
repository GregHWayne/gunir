// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_FUNCTION_RESOLVER_H
#define  GUNIR_COMPILER_FUNCTION_RESOLVER_H

#include <map>
#include <string>
#include <vector>

#include "gunir/compiler/aggregate_functions.h"
#include "gunir/compiler/cast_functions.h"
#include "gunir/compiler/operator_functions.h"
#include "gunir/compiler/parser/big_query_types.h"
#include "gunir/compiler/plain_functions.h"

namespace gunir {
namespace compiler {

class FunctionResolver {
public:
    FunctionResolver();
    ~FunctionResolver();

    const OperatorFunctionInfo* ResolveOperatorFunction(
        Operators op, const std::vector<BQType>& arg_type_list,
        std::vector<BQType>* cast_info) const;

    const PlainFunctionInfo* ResolvePlainFunction(
        const std::string& fn_name, const std::vector<BQType>& arg_type,
        std::vector<BQType>* cast_info) const;

    const AggregateFunctionInfo* ResolveAggregateFunction(
        const std::string& fn_name, BQType arg_type,
        std::vector<BQType>* cast_info) const;

    const CastFunctionInfo* ResolveCastFunction(BQType from, BQType to) const;

    void AddOperatorFunction(Operators op, OperatorFunctionInfo info);

    void AddAggregateFunction(const std::string& fn_name,
                              AggregateFunctionInfo info);

    void AddPlainFunction(const std::string& fn_name,
                          PlainFunctionInfo info);

    void AddCastFunction(
        BQType from, CastFunctionInfo info);

    bool IsArgumentMatches(
        const std::vector<BQType>& arg_type_list, BQType fn_arg_type,
        std::vector<BQType>* cast_info) const;

private:
    std::map<Operators, std::vector<OperatorFunctionInfo> > m_op_functions;
    std::map<std::string, std::vector<AggregateFunctionInfo> > m_agg_functions;
    std::map<std::string, PlainFunctionInfo> m_plain_functions;
    std::map<BQType, std::vector<CastFunctionInfo> > m_cast_functions;
};

} // namespace compiler
} // namespace gunir
#endif  // GUNIR_COMPILER_FUNCTION_RESOLVER_H

