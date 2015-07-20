// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/aggregate_plan.h"

#include <algorithm>

#include "gunir/compiler/dis_agg_expr_info.h"
#include "gunir/compiler/expression_initializer.h"

namespace gunir {
namespace compiler {

AggregatePlan::AggregatePlan(
    Plan* subplan,
    AggregatePlanProto::AggregateMode mode,
    const std::vector<ShrTarget>& final_targets,
    const std::vector<GroupByColumn>& m_groupby_columns,
    const std::vector<BQType>& subplan_tuple_types)
    : m_subplan(subplan), m_mode(mode),
    m_subplan_tuple_types(subplan_tuple_types),
    m_inited(false), m_has_more_in_current_group(false) {

    for (size_t i = 0; i < final_targets.size(); ++i) {
        const std::shared_ptr<Target>& t = final_targets[i];

        m_final_target_exprs.push_back(t->GetExpression());
        AddAggregateExprInfo(t->GetAggregateFunctionExpression());
    }

    for (size_t i = 0; i < m_groupby_columns.size(); ++i) {
        TupleOrderByInfo info;
        info.m_affect_id = m_groupby_columns[i].m_affect_id;
        info.m_type = kAsc;
        m_orderby_infos.push_back(info);
    }

    // Sort aggs first, to match the tuples supplied by executor
    std::sort(m_aggs.begin(), m_aggs.end(), LessCompare());
    m_comparator.reset(new TupleComparator(m_orderby_infos));
}

AggregatePlan::AggregatePlan(const PlanProto& proto)
    : m_inited(false), m_has_more_in_current_group(false) {
        ParseFromProto(proto);
        // Sort aggs first, to match the tuples supplied by executor
        std::sort(m_aggs.begin(), m_aggs.end(), LessCompare());
        m_comparator.reset(new TupleComparator(m_orderby_infos));
    }

AggregatePlan::~AggregatePlan() {
    for (size_t i = 0; i < m_aggs.size(); i++) {
        delete m_aggs[i];
    }
}

void AggregatePlan::SetupAggregate() {
    m_has_more_in_current_group = false;
    m_has_next_tuple = true;
    m_group_start_tuple = new TupleType();
    m_group_start_tuple->resize(m_subplan_tuple_types.size());
    m_save_start_tuple = NULL;

    for (size_t i = 0; i < m_subplan_tuple_types.size(); ++i) {
        (*m_group_start_tuple)[i] = new DatumBlock(m_subplan_tuple_types[i]);
    }

    uint32_t select_level;
    if (!m_subplan->GetNextTuple(*m_group_start_tuple, &select_level)) {
        m_has_next_tuple = false;
        return;
    }

    for (size_t i = 0; i < m_aggs.size(); ++i) {
        m_aggs[i]->SetUp();
    }
}

bool AggregatePlan::GetNextTuple(const TupleType& tuple,
                                 uint32_t* select_level) {
    if (!m_inited) {
        SetupAggregate();
        m_inited = true;
    }

    if (m_has_more_in_current_group) {
        if (GetFromCurrentGroup(*m_save_start_tuple, tuple)) {
            return true;
        } else {
            ReleaseTuple(m_save_start_tuple);
            m_save_start_tuple = NULL;
        }
    }

    if (!GetFromNewGroup(tuple, select_level)) {
        return false;
    }
    return true;
}

bool AggregatePlan::GetFromNewGroup(const TupleType& tuple, uint32_t* select_level) {

    if (m_group_start_tuple == NULL) {
        return false;
    }

    if (m_has_next_tuple) {
        // transfer last tuple and copy to save_tuple buffer
        TransferThisTuple(*m_group_start_tuple);
        m_save_start_tuple = CopyTuple(*m_group_start_tuple);
    } else {
        // no any more tuples in subplan
        ReleaseTuple(m_group_start_tuple);
        m_group_start_tuple = NULL;
        return false;
    }

    while (m_has_next_tuple &&
           m_subplan->GetNextTuple(*m_group_start_tuple, select_level)) {
        // current group is end, only when current tuple is a new record
        // and the groupby items is not equal
        if ((*select_level == 0) &&
             !m_comparator->IsEqual(*m_save_start_tuple, *m_group_start_tuple)) {
            CHECK(GetFromCurrentGroup(*m_save_start_tuple, tuple));
            return true;
        }
        TransferThisTuple(*m_group_start_tuple);
    }

    *select_level = 0;
    GetFromCurrentGroup(*m_save_start_tuple, tuple);
    m_has_next_tuple = false;
    return true;
}

void AggregatePlan::TransferThisTuple(const TupleType& tuple) {
    switch (m_mode) {
    case AggregatePlanProto::kLocalAgg:
        TransferToThisGroup(tuple);
        break;

    case AggregatePlanProto::kMergeAgg:
        MergeToIntermediateGroup(tuple);
        break;

    case AggregatePlanProto::kFinalAgg:
        MergeToIntermediateGroup(tuple);
        break;

    case AggregatePlanProto::kNotParallel:
        TransferToThisGroup(tuple);
        break;
    }
}

bool AggregatePlan::GetFromCurrentGroup(const TupleType& this_group_start_tuple,
                                        const TupleType& tuple) {
    switch (m_mode) {
    case AggregatePlanProto::kLocalAgg:
        return GenerateIntermediateGroup(this_group_start_tuple, tuple);
        break;

    case AggregatePlanProto::kMergeAgg:
        return GenerateIntermediateGroup(this_group_start_tuple, tuple);
        break;

    case AggregatePlanProto::kFinalAgg:
        return FinalizeAndEvaluateThisGroup(this_group_start_tuple, tuple);
        break;

    case AggregatePlanProto::kNotParallel:
        return FinalizeAndEvaluateThisGroup(this_group_start_tuple, tuple);
        break;
    default:
        LOG(FATAL) << "has not such kind of mode " << m_mode;
        return false;
    }
}

void AggregatePlan::TransferToThisGroup(const TupleType& tuple) {
    for (size_t i = 0; i < m_aggs.size(); ++i) {
        m_aggs[i]->TransferTuple(tuple);
    }
}

bool AggregatePlan::FinalizeAndEvaluateThisGroup(
    const TupleType& this_group_start_tuple,
    const TupleType& tuple) {

    for (size_t i = 0; i < m_aggs.size(); ++i) {
        m_aggs[i]->FinalizeAndEndGroup();
    }

    for (size_t i = 0; i < m_final_target_exprs.size(); ++i) {
        m_final_target_exprs[i]->Evaluate(this_group_start_tuple, tuple[i]);
        if (m_final_target_exprs[i]->HasAggregate()) {
            tuple[i]->m_rep_level = 0;
            tuple[i]->m_def_level = 0;

            if (!tuple[i]->m_is_null) {
                tuple[i]->m_def_level = 1;
            }
            tuple[i]->m_has_block = true;
        }
    }

    for (size_t i = 0; i < m_aggs.size(); ++i) {
        m_aggs[i]->TearDownExpr();
        m_aggs[i]->SetUp();
    }

    ReleaseTuple(m_save_start_tuple);
    m_save_start_tuple = NULL;

    return true;
}

void AggregatePlan::MergeToIntermediateGroup(const TupleType& tuple) {
    size_t agg_start = m_subplan_tuple_types.size() - m_aggs.size();
    for (size_t i = 0; i < m_aggs.size(); ++i) {
        m_aggs[i]->MergeTuple(tuple, agg_start);
    }
}

bool AggregatePlan::GenerateIntermediateGroup(
    const TupleType& this_group_start_tuple,
    const TupleType& tuple) {
    size_t agg_start = tuple.size() - m_aggs.size();

    if (!m_has_more_in_current_group) {
        // End This Group at first time
        for (size_t i = 0; i < m_aggs.size(); ++i) {
            m_aggs[i]->EndThisGroup();
        }
    }

    m_has_more_in_current_group = false;

    for (size_t i = 0; i < agg_start; ++i) {
        *tuple[i] = *this_group_start_tuple[i];
    }

    // if all aggregate functions have no NextTuple that means
    // there is no any more tuples in current group
    // The number of all tuple in current group will keep in same with the
    // largest number of tuple among these aggregate functions
    for (size_t i = agg_start; i < tuple.size(); ++i) {
        if (m_aggs[i - agg_start]->UpdateNextTuple(tuple, i)) {
            m_has_more_in_current_group = true;
        } else {
            tuple[i]->m_is_null = true;
        }
    }

    // cause no more in curent group, reset the m_agg
    if (!m_has_more_in_current_group) {
        for (size_t i = 0; i < m_aggs.size(); ++i) {
            m_aggs[i]->TearDownExpr();
            m_aggs[i]->SetUp();
        }
    }
    return m_has_more_in_current_group;
}

void AggregatePlan::AddAggregateExprInfo(
    const std::vector<ShrAggExpr>& agg_exprs) {

    for (size_t i = 0; i < agg_exprs.size(); ++i) {
        std::vector<int> affected_ids;

        std::vector<AffectedColumnInfo> affected_columns;
        agg_exprs[i]->GetAffectedColumns(&affected_columns);

        for (size_t c = 0; c < affected_columns.size(); ++c) {
            const AffectedColumnInfo& column = affected_columns[c];
            affected_ids.push_back(column.m_affect_id);
        }

        bool final_agg = (m_mode == AggregatePlanProto::kNotParallel)
                         || (m_mode == AggregatePlanProto::kFinalAgg);

        AggExprInfo *info;
        if (agg_exprs[i]->IsDistinct()) {
            info = new DisAggExprInfo(agg_exprs[i],
                                      affected_ids,
                                      final_agg);
        } else {
            info = new AggExprInfo(agg_exprs[i], affected_ids);
        }
        m_aggs.push_back(info);
    }
}

std::vector<DatumBlock*>* AggregatePlan::CopyTuple(const TupleType& tuple) {
    TupleType* new_tuple = new TupleType();
    new_tuple->resize(tuple.size());

    for (size_t i = 0; i < tuple.size(); ++i) {
        (*new_tuple)[i] = new DatumBlock(*tuple[i]);
    }
    return new_tuple;
}

void AggregatePlan::ReleaseTuple(TupleType* tuple) {
    for (size_t i = 0; i < tuple->size(); ++i) {
        delete (*tuple)[i];
    }
    delete tuple;
}

void AggregatePlan::CopyToProto(PlanProto* proto) const {
    proto->set_type(PlanProto::kAggregate);
    AggregatePlanProto* aggregate_proto = proto->mutable_aggregate();

    PlanProto* subplan_proto = aggregate_proto->mutable_subplan();
    m_subplan->CopyToProto(subplan_proto);

    aggregate_proto->set_mode(m_mode);

    for (size_t i = 0; i < m_final_target_exprs.size(); ++i) {
        ExpressionProto* expr_proto =
            (aggregate_proto->mutable_target_expr_list())->Add();
        m_final_target_exprs[i]->CopyToProto(expr_proto);
    }

    for (size_t i = 0; i < m_subplan_tuple_types.size(); ++i) {
        aggregate_proto->add_subplan_tuple_types(
            BigQueryType::ConvertBQTypeToPBProtoType(m_subplan_tuple_types[i]));
    }

    for (size_t i = 0; i < m_orderby_infos.size(); ++i) {
        OrderByInfo* info_proto =
            (aggregate_proto->mutable_orderby_info_list())->Add();
        info_proto->set_affect_id(m_orderby_infos[i].m_affect_id);
        info_proto->set_type(m_orderby_infos[i].m_type);
    }

    for (size_t i = 0; i < m_final_define_levels.size(); ++i) {
        aggregate_proto->add_final_define_level(m_final_define_levels[i]);
    }
}

void AggregatePlan::ParseFromProto(const PlanProto& proto) {
    CHECK_EQ(PlanProto::kAggregate, proto.type())
        << "Proto:" << proto.DebugString()
        << " is not aggregate plan proto";

    const AggregatePlanProto& aggregate_proto = proto.aggregate();

    m_subplan.reset(Plan::InitPlanFromProto(aggregate_proto.subplan()));
    m_mode = aggregate_proto.mode();

    for (int i = 0; i < aggregate_proto.subplan_tuple_types_size(); ++i) {
        m_subplan_tuple_types.push_back(
            BigQueryType::ConvertPBProtoTypeToBQType(
                aggregate_proto.subplan_tuple_types(i)));
    }

    for (int i = 0; i < aggregate_proto.target_expr_list_size(); ++i) {
        ExpressionInitializer initializer;
        const ExpressionProto& expr_proto =
            aggregate_proto.target_expr_list(i);
        ShrExpr e = initializer.InitExpressionFromProto(expr_proto);

        m_final_target_exprs.push_back(e);
        AddAggregateExprInfo(initializer.GetAggregateFunctionExpression());
    }

    for (int i = 0; i < aggregate_proto.orderby_info_list_size(); ++i) {
        const OrderByInfo& info_proto = aggregate_proto.orderby_info_list(i);
        TupleOrderByInfo info;

        info.m_affect_id = info_proto.affect_id();
        info.m_type = info_proto.type();
        m_orderby_infos.push_back(info);
    }

    for (int i = 0; i < aggregate_proto.final_define_level_size(); ++i) {
        m_final_define_levels.push_back(aggregate_proto.final_define_level(i));
    }
}

} // namespace compiler
} // namespace gunir

