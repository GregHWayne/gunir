// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/compiler/expression_initializer.h"
#include "gunir/compiler/join_plan.h"

namespace gunir {
namespace compiler {

JoinPlan::JoinPlan(const std::vector<Plan*>& subplans,
                   const std::shared_ptr<Expression>& e,
                   const JoinOperator& type,
                   const std::vector<uint64_t>& left_affect_id,
                   const std::vector<uint64_t>& right_affect_id)
    : m_subplans(subplans),
      m_condition(e),
      m_type(type),
      m_left_affect_id(left_affect_id),
      m_right_affect_id(right_affect_id),
      m_last_pos(-1),
      m_has_more(false) {
}

JoinPlan::JoinPlan(const PlanProto& proto)
    : m_last_pos(-1),
      m_has_more(false) {
    ParseFromProto(proto);
}

JoinPlan::~JoinPlan() {
    for (size_t i = 0; i < m_subplans.size(); ++i) {
        delete m_subplans[i];
    }
    ReleaseBufferedTuple();
}

bool JoinPlan::GetNextTuple(const TupleType& tuple,
                            uint32_t* select_level) {

    // read right table first
    while (m_subplans[1]->GetNextTuple(tuple, select_level)) {
        BufferThisTuple(tuple);
    }

    DatumBlock datum_block(BigQueryType::BOOL);


    // ensure the tuple r is always 0 , d is 1 or 0

    if (m_last_pos == -1) {
        m_has_more = m_subplans[0]->GetNextTuple(tuple, select_level);
        if (m_has_more) {
            m_last_tuple = CopyTuple(tuple);
        }
    }

    while (m_has_more) {
        for (size_t i = m_last_pos + 1 ; i < m_buffered_tuples.size(); ++i) {

            MergeTuples(tuple, i);

            bool is_succeed = m_condition->Evaluate(tuple, &datum_block);
            CHECK(is_succeed) << "Evaluate filter expression failed";

            // null is take as false
            if (!datum_block.m_is_null && datum_block.m_datum._BOOL) {
                m_last_pos = i;
                return true;
            }
        }

        // non condition satisfied
        if ((m_last_pos == -1) && (m_type == kLeftOuter)) {

            // set m_buffer_tuples non and release the right tuples
            for (size_t j = 0; j < m_right_affect_id.size(); ++j) {
                uint64_t affect_pos = m_right_affect_id[j];
                tuple[affect_pos]->m_def_level = 0U;
                tuple[affect_pos]->m_is_null = true;
            }

            m_last_pos = -1;
            ReleaseTuple(*m_last_tuple);
            delete m_last_tuple;
            return true;
        }

        ReleaseTuple(*m_last_tuple);
        delete m_last_tuple;

        m_has_more = m_subplans[0]->GetNextTuple(tuple, select_level);
        if (m_has_more)
            m_last_tuple = CopyTuple(tuple);

        m_last_pos = -1;
    }

    return false;
}

void JoinPlan::MergeTuples(const TupleType& tuple, size_t buffer_pos) {
    // copy left tuples into tuples
    for (size_t j = 0; j < m_left_affect_id.size(); ++j) {
        uint64_t affect_pos = m_left_affect_id[j];
        *tuple[affect_pos] = *((*m_last_tuple)[affect_pos]);
        tuple[affect_pos]->m_def_level = 1;
    }

    // copy right tuples into tuples
    for (size_t j = 0; j < m_right_affect_id.size(); ++j) {
        uint64_t affect_pos = m_right_affect_id[j];
        *tuple[affect_pos] = *((*(m_buffered_tuples[buffer_pos]))[affect_pos]);
        tuple[affect_pos]->m_def_level = 1;
    }
}

void JoinPlan::BufferThisTuple(const TupleType& tuple) {
    m_buffered_tuples.push_back(CopyTuple(tuple));
}

std::vector<DatumBlock*>* JoinPlan::CopyTuple(const TupleType& tuple) {
    TupleType* new_tuple = new TupleType();
    new_tuple->resize(tuple.size());

    for (size_t i = 0; i < tuple.size(); ++i) {
        (*new_tuple)[i] = new DatumBlock(*tuple[i]);
    }
    return new_tuple;
}

void JoinPlan::ReleaseTuple(const TupleType& tuple) {
    for (size_t d = 0; d < tuple.size(); ++d) {
        delete tuple[d];
    }
}

void JoinPlan::ReleaseBufferedTuple() {
    for (size_t i = 0; i < m_buffered_tuples.size(); ++i) {
        const TupleType& tuple = *m_buffered_tuples[i];
        ReleaseTuple(tuple);
        delete m_buffered_tuples[i];
    }
}

void JoinPlan::CopyToProto(PlanProto* proto) const {

    proto->set_type(PlanProto::kJoin);

    JoinPlanProto* join_proto = proto->mutable_join();

    join_proto->set_type(m_type);

    ExpressionProto* expr_proto = join_proto->mutable_join_expr();
    m_condition->CopyToProto(expr_proto);

    JoinTable* left_table = join_proto->mutable_left_table();
    JoinTable* right_table = join_proto->mutable_right_table();

    for (size_t i = 0; i < m_left_affect_id.size(); ++i) {
        left_table->add_affect_id(m_left_affect_id[i]);
    }

    m_subplans[0]->CopyToProto(left_table->mutable_subplan());

    for (size_t i = 0; i < m_right_affect_id.size(); ++i) {
        right_table->add_affect_id(m_right_affect_id[i]);
    }

    m_subplans[1]->CopyToProto(right_table->mutable_subplan());
}

void JoinPlan::ParseFromProto(const PlanProto& proto) {
    CHECK(proto.type() == PlanProto::kJoin)
        << "Proto is not join proto:" << proto.DebugString();

    const JoinPlanProto& join_proto = proto.join();

    ExpressionInitializer initializer;
    m_type = join_proto.type();
    m_condition = initializer.InitExpressionFromProto(join_proto.join_expr());
    m_subplans.push_back(Plan::InitPlanFromProto(join_proto.left_table().subplan()));
    m_subplans.push_back(Plan::InitPlanFromProto(join_proto.right_table().subplan()));

    const JoinTable& left_table = join_proto.left_table();
    for (int i = 0; i < left_table.affect_id_size(); ++i) {
        m_left_affect_id.push_back(left_table.affect_id(i));
    }

    const JoinTable& right_table = join_proto.right_table();
    for (int i = 0; i < right_table.affect_id_size(); ++i) {
        m_right_affect_id.push_back(right_table.affect_id(i));
    }
}

}  // namespace compiler
}  // namespace gunir

