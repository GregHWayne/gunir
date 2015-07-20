// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/union_plan.h"

namespace gunir {
namespace compiler {

UnionPlan::UnionPlan(const std::vector<Plan*>& subplans)
    : m_subplans(subplans) {
}

UnionPlan::UnionPlan(const PlanProto& proto) {
    ParseFromProto(proto);
}

UnionPlan::~UnionPlan() {
    for (size_t i = 0; i < m_subplans.size(); ++i) {
        delete m_subplans[i];
    }
}

void UnionPlan::SetScanner(const std::vector<io::Scanner*>& scanners) {
    std::vector<io::Scanner*> temp_vector;

    CHECK(scanners.size() == m_subplans.size())
        << "union scanner size:" << scanners.size()
        << " != " << m_subplans.size();

    temp_vector.resize(1);
    for (size_t i = 0; i < m_subplans.size(); ++i) {
        temp_vector[0] = scanners[i];
        m_subplans[i]->SetScanner(temp_vector);
    }
}

bool UnionPlan::GetNextTuple(const std::vector<DatumBlock*>& tuple,
                             uint32_t* select_level) {
    while (m_subplans.size() > 0) {
        Plan* subplan = m_subplans[0];

        bool has_more = subplan->GetNextTuple(tuple, select_level);
        if (has_more)
            return true;

        delete m_subplans[0];
        m_subplans.erase(m_subplans.begin());
    }

    return false;
}

void UnionPlan::CopyToProto(PlanProto* proto) const {
    proto->set_type(PlanProto::kUnion);

    UnionPlanProto* union_proto = proto->mutable_union_();
    for (size_t i = 0; i < m_subplans.size(); ++i) {
        PlanProto* subplan = (union_proto->mutable_subplan_list())->Add();
        m_subplans[i]->CopyToProto(subplan);
    }
}

void UnionPlan::ParseFromProto(const PlanProto& proto) {
    CHECK(proto.type() == PlanProto::kUnion)
        << "Proto is not union proto:" << proto.DebugString();

    const UnionPlanProto& union_proto = proto.union_();
    for (int i = 0; i < union_proto.subplan_list_size(); ++i) {
        const PlanProto& subplan_proto = union_proto.subplan_list(i);
        m_subplans.push_back(Plan::InitPlanFromProto(subplan_proto));
    }
}

} // namespace compiler
} // namespace gunir

