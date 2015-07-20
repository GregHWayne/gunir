// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <set>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/shared_ptr.h"

#include "gunir/compiler/aggregate_function_expression.h"
#include "gunir/compiler/big_query_functions.h"

#include "gunir/compiler/agg_expr_info.h"

namespace gunir {
namespace compiler {


AggExprInfo::AggExprInfo(const ShrAggExpr& expr,
                         std::vector<int> affected_ids)
    : m_expr(expr),
      m_affected_ids(affected_ids),
      m_result_index(0) {
}

AggExprInfo::~AggExprInfo() {
    m_expr->TearDown();
}

void AggExprInfo::TransferTuple(const TupleType& tuple) {
    if (!IsAggExprAffected(tuple)) {
        return;
    }
    m_expr->Transfer(tuple);
}

void AggExprInfo::MergeTuple(const TupleType& tuple, size_t agg_start) {
    size_t agg_id = m_expr->GetAggId();
    m_expr->Merge(tuple[agg_start + agg_id]);
}

void AggExprInfo::SetUp() {
    m_result_index = 0;
    m_result_blocks.clear();
    m_expr->Setup();
}

void AggExprInfo::TearDownExpr() {
    m_expr->TearDown();
}

void AggExprInfo::EndThisGroup() {
    m_result_blocks.push_back(m_expr->GetTransBlock());
}

void AggExprInfo::FinalizeAndEndGroup() {
    m_expr->Finalize();
}

bool AggExprInfo::UpdateNextTuple(const TupleType& tuple,
                                  size_t agg_index) {

    if (m_result_index < m_result_blocks.size()) {
        *tuple[agg_index] = m_result_blocks[m_result_index];
        tuple[agg_index]->m_has_block = true;
        m_result_index++;
        return true;
    }
    return false;
}

bool AggExprInfo::IsAggExprAffected(const TupleType& tuple) {
    for (size_t i = 0; i < m_affected_ids.size(); ++i) {
        int column_id = m_affected_ids[i];
        if (!tuple[column_id]->m_is_null && tuple[column_id]->m_has_block) {
            return true;
        }
    }
    return false;
}

void AggExprInfo::AppendResultBlocks() {
    m_result_blocks.push_back(m_expr->GetTransBlock());
    m_expr->TearDown();
    m_expr->Setup();
}

}  // namespace compiler
}  // namespace gunir





