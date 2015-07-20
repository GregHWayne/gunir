// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/limit_plan.h"

namespace gunir {
namespace compiler {

// print range is [m_start, m_start + m_number - 1]
bool LimitPlan::GetNextTuple(const std::vector<DatumBlock*>& tuple,
                             uint32_t* select_level) {
    do {
        if (!m_subplan->GetNextTuple(tuple, select_level)) {
            return false;
        }

        if (*select_level == 0) {
            m_current++;
        }
    } while (m_current < m_start);

    return (m_current < m_start + m_number);
}

void LimitPlan::CopyToProto(PlanProto* proto) const {
    proto->set_type(PlanProto::kLimit);
    LimitPlanProto* limit_proto = proto->mutable_limit();

    limit_proto->set_start(m_start);
    limit_proto->set_number(m_number);

    PlanProto* subplan_proto = limit_proto->mutable_subplan();
    m_subplan->CopyToProto(subplan_proto);
}

void LimitPlan::ParseFromProto(const PlanProto& proto) {
    CHECK(proto.type() == PlanProto::kLimit) << "Proto:" << proto.DebugString()
        << " is not limit plan proto";

    const LimitPlanProto& limit_proto = proto.limit();
    m_start = limit_proto.start();
    m_number = limit_proto.number();

    m_subplan.reset(Plan::InitPlanFromProto(limit_proto.subplan()));
}

} // namespace compiler
} // namespace gunir

