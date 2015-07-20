// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_COMPILER_AGG_EXPR_INFO_H
#define  GUNIR_COMPILER_AGG_EXPR_INFO_H

#include <set>
#include <vector>

// #include "toft/base/scoped_ptr.h"
#include "toft/base/shared_ptr.h"

#include "gunir/compiler/aggregate_function_expression.h"
#include "gunir/compiler/big_query_functions.h"

namespace gunir {
namespace compiler {

class AggExprInfo {

protected:
    typedef std::vector<DatumBlock*> TupleType;
    typedef std::shared_ptr<AggregateFunctionExpression> ShrAggExpr;

public:
    AggExprInfo(const ShrAggExpr& expr,
                std::vector<int> affected_ids);

    virtual ~AggExprInfo();

    virtual void TransferTuple(const TupleType& tuple);

    virtual void MergeTuple(const TupleType& tuple, size_t agg_start);

    virtual void SetUp();

    void TearDownExpr();

    virtual void EndThisGroup();

    void FinalizeAndEndGroup();

    virtual bool UpdateNextTuple(const TupleType& tuple,
                                 size_t agg_index);

    bool operator < (const AggExprInfo& that) const {
        return m_expr->GetAggId() < that.m_expr->GetAggId();
    }

    virtual bool IsDistinct() const {
        return false;
    }

protected:
    bool IsAggExprAffected(const TupleType& tuple);

    void AppendResultBlocks();

protected:
    ShrAggExpr m_expr;
    bool m_is_distinct;
    std::vector<int> m_affected_ids;
    std::vector<DatumBlock> m_result_blocks;
    size_t m_result_index;
};

}  // namespace compiler
}  // namespace gunir

#endif  // GUNIR_COMPILER_AGG_EXPR_INFO_H

