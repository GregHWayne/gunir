// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_FUNCTION_EXPRESSION_H
#define  GUNIR_COMPILER_FUNCTION_EXPRESSION_H

#include <string>
#include <vector>

#include "toft/base/shared_ptr.h"

#include "gunir/compiler/aggregate_functions.h"
#include "gunir/compiler/cast_functions.h"
#include "gunir/compiler/expression.h"
#include "gunir/compiler/parser/big_query_types.h"
#include "gunir/compiler/plain_functions.h"

namespace gunir {
namespace compiler {

class FunctionExpression : public Expression {
private:
enum FunctionType {
    kAgg,
    kCast,
    kPlain
};

public:
    explicit FunctionExpression(bool is_distinct)
             : m_agg_fn(NULL), m_plain_fn(NULL),
               m_is_distinct(is_distinct) {}

    explicit FunctionExpression(const ExpressionProto& proto,
                                ExpressionInitializer* initializer) {
        ParseFromProto(proto, initializer);
    }

    ~FunctionExpression() {}

    virtual void CopyToProto(ExpressionProto* proto) const;

    virtual void ParseFromProto(const ExpressionProto& proto,
                                ExpressionInitializer* initializer);

    bool Evaluate(const std::vector<DatumBlock*>& datum_block_row,
                  DatumBlock* datum_block) const;

    std::shared_ptr<DatumBlock> GetOperand(
        const std::shared_ptr<Expression>& expr,
        const std::vector<DatumBlock*>& datum_block_row,
        BQFunctionInfo* info,
        size_t index) const;

    virtual bool GetDeepestColumn(DeepestColumnInfo* info) const;
    virtual void GetAffectedColumns(
        std::vector<AffectedColumnInfo>* affected_columns) const;

    virtual BQType GetReturnType() const;

    virtual bool HasAggregate() const;

    virtual bool IsSingleColumn() const { return false; }

    inline void SetAggFunction(const AggregateFunctionInfo* fn_info);
    inline void SetCastFunction(const CastFunctionInfo* cast_info);
    inline void SetPlainFunction(const PlainFunctionInfo* fn_info);

    void SetArguments(
        const std::vector<std::shared_ptr<Expression> >& expr_list) {
        m_expr_list = expr_list;
    }

    virtual bool IsDistinct() const { return m_is_distinct;}

protected:
    FunctionType m_fn_type;
    std::vector<std::shared_ptr<Expression> > m_expr_list;

    const AggregateFunctionInfo* m_agg_fn;
    const PlainFunctionInfo* m_plain_fn;
    const CastFunctionInfo* m_cast_fn;
    bool m_is_distinct;
};

void FunctionExpression::SetAggFunction(
    const AggregateFunctionInfo* fn_info) {
    m_agg_fn = fn_info;
    m_fn_type = kAgg;
}

void FunctionExpression::SetPlainFunction(
    const PlainFunctionInfo* fn_info) {
    m_plain_fn = fn_info;
    m_fn_type = kPlain;
}

void FunctionExpression::SetCastFunction(
    const CastFunctionInfo* cast_info) {
    m_cast_fn = cast_info;
    m_fn_type = kCast;
}

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_FUNCTION_EXPRESSION_H

