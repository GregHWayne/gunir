// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/sort_plan.h"

#include <stdint.h>

#include <algorithm>

namespace gunir {
namespace compiler {

SortPlan::SortPlan(Plan* subplan,
                   const std::vector<OrderByColumn>& orderby_columns)
    : m_subplan(subplan),
      m_next_record(0),
      m_next_valid_tuple_offset(0) {

    for (size_t i = 0; i < orderby_columns.size(); ++i) {
        TupleOrderByInfo orderby_info;

        orderby_info.m_affect_id = orderby_columns[i].m_target_id;
        orderby_info.m_type = orderby_columns[i].m_type;

        m_orderby_infos.push_back(orderby_info);
    }
}

SortPlan::SortPlan(const PlanProto& proto)
    : m_next_record(0),
      m_next_valid_tuple_offset(0) {
        ParseFromProto(proto);
}

bool SortPlan::GetNextTuple(const TupleType& tuple,
                            uint32_t* select_level) {
    bool buffered_new_tuple = false;

    while (m_subplan->GetNextTuple(tuple, select_level)) {
        BufferThisTuple(tuple, *select_level);
        buffered_new_tuple = true;
    }

    // No valid tuples or all records are returned
    if (m_buffered_tuples.empty() ||
        m_next_record > m_record_offset.size()) {
        return false;
    }

    // Buffered tuples not sorted, sort them by RECORD
    if (buffered_new_tuple) {
        SortBufferedRecords();
        m_next_valid_tuple_offset = UINT32_MAX;
        m_next_record = -1;
    }

    // Calculate next valid tuple offset
    if (m_next_valid_tuple_offset >= m_buffered_tuples.size() ||
        m_select_levels[m_next_valid_tuple_offset] == 0) {

        m_next_record++;
        if (m_next_record >= m_record_offset.size()) {
            return false;
        }
        m_next_valid_tuple_offset = m_record_offset[m_next_record];
    }

    const TupleType& buffered_tuple =
        *m_buffered_tuples[m_next_valid_tuple_offset];
    CHECK_EQ(tuple.size(), buffered_tuple.size());

    for (size_t i = 0; i < tuple.size(); ++i) {
        *tuple[i] = *buffered_tuple[i];
    }

    *select_level = m_select_levels[m_next_valid_tuple_offset];

    m_next_valid_tuple_offset++;
    return true;
}

void SortPlan::BufferThisTuple(const TupleType& tuple,
                               uint32_t select_level) {
    if (select_level == 0) {
        m_record_offset.push_back(m_buffered_tuples.size());
    }

    m_buffered_tuples.push_back(CopyTuple(tuple));
    m_select_levels.push_back(select_level);
}

std::vector<DatumBlock*>* SortPlan::CopyTuple(const TupleType& tuple) {
    TupleType* new_tuple = new TupleType();
    new_tuple->resize(tuple.size());

    for (size_t i = 0; i < tuple.size(); ++i) {
        (*new_tuple)[i] = new DatumBlock(*tuple[i]);
    }
    return new_tuple;
}

void SortPlan::ReleaseBufferedTuple() {
    for (size_t i = 0; i < m_buffered_tuples.size(); ++i) {
        const TupleType& tuple = *m_buffered_tuples[i];

        for (size_t d = 0; d < tuple.size(); ++d) {
            delete tuple[d];
        }
        delete m_buffered_tuples[i];
    }
}

void SortPlan::SortBufferedRecords() {
    std::stable_sort(
        m_record_offset.begin(), m_record_offset.end(),
        RecordComparator(TupleComparator(m_orderby_infos),
                         m_buffered_tuples));
}

void SortPlan::CopyToProto(PlanProto* proto) const {
    proto->set_type(PlanProto::kSort);

    SortPlanProto* sort_proto = proto->mutable_sort();
    PlanProto* subplan_proto = sort_proto->mutable_subplan();
    m_subplan->CopyToProto(subplan_proto);

    for (size_t i = 0; i < m_orderby_infos.size(); ++i) {
        OrderByInfo* info_proto =
            (sort_proto->mutable_orderby_info_list())->Add();
        info_proto->set_affect_id(m_orderby_infos[i].m_affect_id);
        info_proto->set_type(m_orderby_infos[i].m_type);
    }
}

void SortPlan::ParseFromProto(const PlanProto& proto ) {
    CHECK(proto.type() == PlanProto::kSort)
        << "Proto:" << proto.DebugString()
        << " is not sort plan proto";

    const SortPlanProto sort_proto = proto.sort();
    m_subplan.reset(
        Plan::InitPlanFromProto(sort_proto.subplan()));

    for (int i = 0; i < sort_proto.orderby_info_list_size(); ++i) {
        const OrderByInfo& info_proto = sort_proto.orderby_info_list(i);
        TupleOrderByInfo info;

        info.m_affect_id = info_proto.affect_id();
        info.m_type = info_proto.type();
        m_orderby_infos.push_back(info);
    }
}

} // namespace compiler
} // namespace gunir

