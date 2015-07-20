// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/merge_plan.h"

namespace gunir {
namespace compiler {

MergePlan::MergePlan(const std::vector<Plan*>& subplans,
                     const std::vector<OrderByColumn>& orderby_columns)
    : m_subplans(subplans),
      m_curr_subplan(NULL) {

    for (size_t i = 0; i < orderby_columns.size(); ++i) {
        TupleOrderByInfo orderby_info;

        orderby_info.m_affect_id = orderby_columns[i].m_target_id;
        orderby_info.m_type = orderby_columns[i].m_type;

        m_orderby_infos.push_back(orderby_info);
    }

    m_tuple_heap.reset(
        new TupleHeap(LessTupleItem(TupleComparator(m_orderby_infos))));
}

MergePlan::MergePlan(const PlanProto& proto)
    : m_curr_subplan(NULL) {
    ParseFromProto(proto);
    m_tuple_heap.reset(
        new TupleHeap(LessTupleItem(TupleComparator(m_orderby_infos))));
}

MergePlan::~MergePlan() {
    for (size_t i = 0; i < m_subplans.size(); ++i) {
        delete m_subplans[i];
        m_subplans[i] = NULL;
    }

    while (!m_tuple_heap->empty()) {
        const TupleItem& t = m_tuple_heap->top();
        ReleaseTupleInItem(t);
        m_tuple_heap->pop();
    }
}

void MergePlan::SetScanner(const std::vector<io::Scanner*>& scanners) {
    std::vector<io::Scanner*> temp_vector;

    CHECK(scanners.size() == m_subplans.size())
        << "merge scanner size:" << scanners.size()
        << " != " << m_subplans.size();

    temp_vector.resize(1);
    for (size_t i = 0; i < m_subplans.size(); ++i) {
        temp_vector[0] = scanners[i];
        m_subplans[i]->SetScanner(temp_vector);
    }
}

bool MergePlan::GetNextTuple(const TupleType& tuple,
                             uint32_t* select_level) {
    while (HasMoreTuple(tuple)) {
        if (GetNextValidTuple(tuple, select_level)) {
            return true;
        }
    }
    return false;
}

bool MergePlan::HasMoreTuple(const TupleType& tuple) {
    if (!m_tuple_heap->empty()) {
        return true;
    }
    return BuildHeap(tuple);
}

bool MergePlan::GetNextValidTuple(const TupleType& tuple,
                                  uint32_t* select_level) {
    if (m_curr_subplan == NULL) {
        ReturnTopTupleInHeap(tuple, select_level);

        // Now read the following tuples in same reocrd
        uint32_t id = m_tuple_heap->top().m_id;
        m_curr_subplan = m_subplans[id];
        return true;
    }

    if (m_curr_subplan->GetNextTuple(tuple, select_level)) {
        if (*select_level > 0) {
            return true;
        }

        // select_level == 0 indicates a new record
        NewRecordForSubplan(tuple);
    } else {
        EndSubplan();
    }

    m_curr_subplan = NULL;
    return false;
}

void MergePlan::ReturnTopTupleInHeap(const TupleType& tuple,
                                     uint32_t* select_level) {
    const TupleItem& item = m_tuple_heap->top();

    CopyTuple(*item.m_tuple, tuple);
    *select_level = 0;
}

void MergePlan::NewRecordForSubplan(const TupleType& tuple) {
    const TupleItem& item = m_tuple_heap->top();
    uint32_t subplan_id = item.m_id;

    ReleaseTupleInItem(item);
    m_tuple_heap->pop();

    TupleType* new_tuple = AllocateAndCopyTuple(tuple);
    m_tuple_heap->push(TupleItem(new_tuple, subplan_id));
}

void MergePlan::EndSubplan() {
    const TupleItem& item = m_tuple_heap->top();
    size_t subplan_id = item.m_id;

    ReleaseTupleInItem(item);
    m_tuple_heap->pop();

    delete m_curr_subplan;
    m_curr_subplan = NULL;
    m_subplans[subplan_id] = NULL;
}

bool MergePlan::BuildHeap(const TupleType& tuple) {
    uint32_t select_level;

    for (size_t i = 0; i < m_subplans.size(); ++i) {
        Plan* subplan = m_subplans[i];

        if (subplan == NULL) {
            continue;
        }

        if (!subplan->GetNextTuple(tuple, &select_level)) {
            delete subplan;
            m_subplans[i] = NULL;
            continue;
        }

        CHECK_EQ(0U, select_level);

        TupleType* new_tuple = AllocateAndCopyTuple(tuple);
        m_tuple_heap->push(TupleItem(new_tuple, i));
    }
    return (!m_tuple_heap->empty());
}

void MergePlan::CopyTuple(const TupleType& src_tuple,
                          const TupleType& dest_tuple) {
    for (size_t i = 0; i < src_tuple.size(); ++i) {
        *dest_tuple[i] = *src_tuple[i];
    }
}

std::vector<DatumBlock*>* MergePlan::AllocateAndCopyTuple(
    const TupleType& tuple) {
    TupleType* new_tuple = new TupleType();
    new_tuple->resize(tuple.size());

    for (size_t i = 0; i < tuple.size(); ++i) {
        (*new_tuple)[i] = new DatumBlock(*tuple[i]);
    }
    return new_tuple;
}

void MergePlan::ReleaseTupleInItem(const TupleItem& item) {
    const TupleType& tuple = *(item.m_tuple);

    for (size_t d = 0; d < tuple.size(); ++d) {
        delete tuple[d];
    }
    delete item.m_tuple;
}

void MergePlan::CopyToProto(PlanProto* proto) const {
    proto->set_type(PlanProto::kMerge);

    MergePlanProto* merge_proto = proto->mutable_merge();
    for (size_t i = 0; i < m_subplans.size(); ++i) {
        PlanProto* subplan_proto = (merge_proto->mutable_subplan_list())->Add();
        m_subplans[i]->CopyToProto(subplan_proto);
    }

    for (size_t i = 0; i < m_orderby_infos.size(); ++i) {
        merge_proto->add_affect_id(m_orderby_infos[i].m_affect_id);
        merge_proto->add_type(m_orderby_infos[i].m_type);
    }
}

void MergePlan::ParseFromProto(const PlanProto& proto) {
    CHECK(proto.type() == PlanProto::kMerge)
        << "Proto is not merge proto:" << proto.DebugString();

    const MergePlanProto& merge_proto = proto.merge();
    for (int i = 0; i < merge_proto.subplan_list_size(); ++i) {
        const PlanProto& subplan_proto = merge_proto.subplan_list(i);
        m_subplans.push_back(Plan::InitPlanFromProto(subplan_proto));
    }

    CHECK_EQ(merge_proto.affect_id_size(), merge_proto.type_size());

    for (int i = 0; i < merge_proto.affect_id_size(); ++i) {
        TupleOrderByInfo info;

        info.m_affect_id = merge_proto.affect_id(i);
        info.m_type = merge_proto.type(i);
        m_orderby_infos.push_back(info);
    }
}

} // namespace compiler
} // namespace gunir

