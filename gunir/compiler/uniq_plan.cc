// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/uniq_plan.h"

#include <algorithm>
#include <stdint.h>

namespace gunir {
namespace compiler {

UniqPlan::UniqPlan(Plan* subplan, std::vector<size_t> distinct_columns)
    : m_subplan(subplan),
      m_distinct_columns(distinct_columns),
      m_buffered_tuple(NULL) {
    InitComparator();
}

UniqPlan::UniqPlan(const PlanProto& proto)
    : m_buffered_tuple(NULL) {
    ParseFromProto(proto);
    InitComparator();
}

UniqPlan::~UniqPlan() {
    if (m_buffered_tuple == NULL) {
        return;
    }

    for (size_t i = 0; i < m_buffered_tuple->size(); ++i) {
        delete (*m_buffered_tuple)[i];
    }
    delete m_buffered_tuple;
}

void UniqPlan::InitComparator() {
    std::vector<TupleOrderByInfo> orderby_infos;

    for (size_t i = 0; i < m_distinct_columns.size(); ++i) {
        TupleOrderByInfo info;

        info.m_affect_id = m_distinct_columns[i];
        info.m_type = kAsc;
        orderby_infos.push_back(info);
    }

    m_comparator.reset(new TupleComparator(orderby_infos));
}

bool UniqPlan::GetNextTuple(const TupleType& tuple,
                            uint32_t* select_level) {
    if (m_buffered_tuple == NULL) {
        return ReturnFirstTuple(tuple, select_level);
    }

    while (m_subplan->GetNextTuple(tuple, select_level)) {
        if (ReturnThisTuple(tuple, *select_level)) {
            return true;
        }
    }
    return false;
}

bool UniqPlan::ReturnThisTuple(const TupleType& tuple, uint32_t select_level) {
    // select_level != 0, this tuple is not the first tuple of record,
    // return this tuple or not depends on whether the first tuple of
    // this record is returned
    if (select_level != 0) {
        return m_returned_first_tuple;
    }

    // select_level == 0, encounter first tuple of a new record
    if (m_comparator->IsEqual(*m_buffered_tuple, tuple)) {
        m_returned_first_tuple = false;
        return false;
    }
    m_returned_first_tuple = true;
    BufferThisTuple(tuple);
    return true;
}

bool UniqPlan::ReturnFirstTuple(const TupleType& tuple,
                                uint32_t* select_level) {
    if (!m_subplan->GetNextTuple(tuple, select_level)) {
        m_returned_first_tuple = false;
        return false;
    }

    m_buffered_tuple = new TupleType();
    m_buffered_tuple->resize(tuple.size());

    for (size_t i = 0; i < m_buffered_tuple->size(); ++i) {
        (*m_buffered_tuple)[i] = new DatumBlock(*tuple[i]);
    }
    m_returned_first_tuple = true;
    return true;
}

void UniqPlan::BufferThisTuple(const TupleType& tuple) {
    for (size_t i = 0; i < m_distinct_columns.size(); ++i) {
        size_t offset = m_distinct_columns[i];

        *(*m_buffered_tuple)[offset] = *tuple[offset];
    }
}

void UniqPlan::CopyToProto(PlanProto* proto) const {
    proto->set_type(PlanProto::kUniq);

    UniqPlanProto* uniq_proto = proto->mutable_uniq();
    PlanProto* subplan_proto = uniq_proto->mutable_subplan();
    m_subplan->CopyToProto(subplan_proto);

    for (size_t i = 0; i < m_distinct_columns.size(); ++i) {
        uniq_proto->add_distinct_column(m_distinct_columns[i]);
    }
}

void UniqPlan::ParseFromProto(const PlanProto& proto ) {
    CHECK(proto.type() == PlanProto::kUniq)
        << "Proto:" << proto.DebugString()
        << " is not uniq plan proto";

    const UniqPlanProto uniq_proto = proto.uniq();
    m_subplan.reset(
        Plan::InitPlanFromProto(uniq_proto.subplan()));

    for (int i = 0; i < uniq_proto.distinct_column_size(); ++i) {
        m_distinct_columns.push_back(uniq_proto.distinct_column(i));
    }
}

} // namespace compiler
} // namespace gunir

