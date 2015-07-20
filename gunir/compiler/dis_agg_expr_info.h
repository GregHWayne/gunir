// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_COMPILER_DIS_AGG_EXPR_INFO_H
#define  GUNIR_COMPILER_DIS_AGG_EXPR_INFO_H

#include <set>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/shared_ptr.h"

#include "gunir/compiler/aggregate_function_expression.h"
#include "gunir/compiler/big_query_functions.h"

#include "gunir/compiler/agg_expr_info.h"

namespace gunir {
namespace compiler {

class DisAggExprInfo : public AggExprInfo {

public:
    DisAggExprInfo(const ShrAggExpr& expr,
                   std::vector<int> affected_ids,
                   bool final_agg)
        : AggExprInfo(expr, affected_ids),
          m_final_agg(final_agg) {}

    ~DisAggExprInfo() {}

    void TransferTuple(const TupleType& tuple);

    void MergeTuple(const TupleType& tuple, size_t agg_start);

    void SetUp();

    void EndThisGroup();

    bool UpdateNextTuple(const TupleType& tuple,
                         size_t agg_index);

    bool IsDistinct() const {
        return true;
    }

private:
    bool InsertDistinctBlocks(const TupleType& tuple);

private:
    bool m_final_agg;
    std::set<DatumBlock> m_distinct_blocks;
    std::set<DatumBlock> :: iterator m_it;
};

}  // namespace compiler
}  // namespace gunir



#endif  // GUNIR_COMPILER_DIS_AGG_EXPR_INFO_H

