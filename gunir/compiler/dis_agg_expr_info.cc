// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include <set>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/shared_ptr.h"

#include "gunir/compiler/aggregate_function_expression.h"
#include "gunir/compiler/big_query_functions.h"

#include "gunir/compiler/dis_agg_expr_info.h"

namespace gunir {
namespace compiler {


void DisAggExprInfo::TransferTuple(const TupleType& tuple) {
    if (!IsAggExprAffected(tuple)) {
        return;
    }
    // If the block is exist before, skip it!
    if (InsertDistinctBlocks(tuple)) {
        return;
    }
    m_expr->Transfer(tuple);
    // when is no final_agg, need to save each distinct blocks
    if (!m_final_agg) {
        AppendResultBlocks();
    }
}

void DisAggExprInfo::MergeTuple(const TupleType& tuple, size_t agg_start) {
    size_t agg_id = m_expr->GetAggId();

    // If the block is exist before, skip it!
    if (InsertDistinctBlocks(tuple)) {
        return;
    }
    m_expr->Merge(tuple[agg_start + agg_id]);
    // when is no final_agg, need to save each distinct blocks
    if (!m_final_agg) {
        AppendResultBlocks();
    }
}

void DisAggExprInfo::SetUp() {
    AggExprInfo::SetUp();
    m_distinct_blocks.clear();
}

void DisAggExprInfo::EndThisGroup() {
    // when in final agg , need push back last block
    // or the block has push back already before
    if (m_final_agg) {
        m_result_blocks.push_back(m_expr->GetTransBlock());
    }
}

bool DisAggExprInfo::UpdateNextTuple(const TupleType& tuple,
                                     size_t agg_index) {

    // Init the iterator when update first tuple
    if (m_result_index == 0U) {
        m_it = m_distinct_blocks.begin();
    }

    if (AggExprInfo::UpdateNextTuple(tuple, agg_index)) {

        // update the affect distinct block
        // in final_agg, update affected block is unnecessary
        if (!m_final_agg) {

            // make sure distinct field/expression is after projection
            CHECK_EQ(1U, m_affected_ids.size());

            if (m_it == m_distinct_blocks.end()) {
                LOG(FATAL) << " m_it is invaild "
                    << " result_index = "<< m_result_index
                    << " set size = " <<  m_distinct_blocks.size()
                    << " block size = " << m_result_blocks.size();
            }

            *tuple[m_affected_ids[0]]  = *m_it;
            m_it++;
        }
        return true;
    } else {
        return false;
    }
}

bool DisAggExprInfo::InsertDistinctBlocks(const TupleType& tuple) {
    // make sure distinct field/expression is after projection
    CHECK_EQ(1U, m_affected_ids.size());
    const DatumBlock& block = *tuple[m_affected_ids[0]];
    if (m_distinct_blocks.find(block) == m_distinct_blocks.end()) {
        m_distinct_blocks.insert(block);
        return false;
    } else {
        return true;
    }
}

}  // namespace compiler
}  // namespace gunir




