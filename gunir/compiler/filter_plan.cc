// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <algorithm>

#include "gunir/compiler/expression_initializer.h"
#include "gunir/compiler/filter_plan.h"

namespace gunir {
namespace compiler {

bool FilterPlan::GetNextTuple(
    const std::vector<DatumBlock*>& tuple, uint32_t* select_level) {

    uint32_t raw_select_level;
    DatumBlock datum_block(BigQueryType::BOOL);

    bool has_more = m_subplan->GetNextTuple(tuple, &raw_select_level);

    // last tuple evaluates to true, select_level should be raw_select_level
    m_select_level = raw_select_level;

    while (has_more) {
        bool is_succeed = m_filter->Evaluate(tuple, &datum_block);
        CHECK(is_succeed) << "Evaluate filter expression failed";

        // null is take as false
        if (!datum_block.m_is_null && datum_block.m_datum._BOOL) {
            *select_level = m_select_level;
            SetRepLevel(tuple, raw_select_level);
            return true;
        }

        has_more = m_subplan->GetNextTuple(tuple, &raw_select_level);

        // last tuple evaluates to false,
        // select_level = min(select_level, fetch_level)
        m_select_level = std::min(m_select_level, raw_select_level);
    }
    return false;
}

void FilterPlan::SetRepLevel(const std::vector<DatumBlock*>& tuple,
                             uint32_t old_select_level) {
    for (size_t i = 0; i < tuple.size(); ++i) {
        if (tuple[i]->m_rep_level >= m_select_level) {
            tuple[i]->m_rep_level = m_select_level;

            // by old_select_level > m_select_level, means we only set these
            // fields that is duplicated and their first valid is filtered by
            // evaluation to be valid
            if (old_select_level > m_select_level) {
                tuple[i]->m_has_block = true;
            }
        }
    }
}

void FilterPlan::CopyToProto(PlanProto* proto) const {
    proto->set_type(PlanProto::kFilter);

    FilterPlanProto* filter_proto = proto->mutable_filter();
    ExpressionProto* expr_proto = filter_proto->mutable_filter_expr();

    m_filter->CopyToProto(expr_proto);

    PlanProto* subplan_proto = filter_proto->mutable_subplan();
    m_subplan->CopyToProto(subplan_proto);
}

void FilterPlan::ParseFromProto(const PlanProto& proto) {
    CHECK(proto.type() == PlanProto::kFilter)
        << "Proto:" << proto.DebugString()
        << " is not filter plan proto";

    const FilterPlanProto filter_proto = proto.filter();
    ExpressionInitializer initializer;
    m_filter = initializer.InitExpressionFromProto(filter_proto.filter_expr());
    m_subplan.reset(
        Plan::InitPlanFromProto(filter_proto.subplan()));
}

} // namespace compiler
} // namespace gunir

