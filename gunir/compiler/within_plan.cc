// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/within_plan.h"

#include <algorithm>

#include "gunir/compiler/expression_initializer.h"
#include "gunir/compiler/target.h"

namespace gunir {
namespace compiler {

WithinPlan::WithinPlan(
    const std::vector<std::shared_ptr<Target> >& target_list,
    const std::vector<AffectedColumnInfo>& subplan_affect_column_infos,
    Plan* subplan)
    : m_subplan(subplan), m_new_input_tuple(NULL) {

    // m_subplan_tuple_types
    for (size_t i = 0; i < subplan_affect_column_infos.size(); ++i) {
        m_subplan_tuple_types.push_back(
            subplan_affect_column_infos[i].m_column_info.m_type);
    }

    // m_target_expr_list, m_target_define_level, m_target_within_level
    m_target_expr_list.resize(target_list.size());
    m_target_define_level.resize(target_list.size());
    m_target_within_level.resize(target_list.size());
    for (size_t i = 0; i < target_list.size(); ++i) {
        m_target_expr_list[i] = target_list[i]->GetExpression();
        m_target_define_level[i] =
            target_list[i]->GetResultColumnInfo().m_define_level;
        m_target_within_level[i] = target_list[i]->GetWithinLevel();
    }

    // m_deepest_column_affect_id
    m_deepest_column_affect_id.resize(target_list.size());
    DeepestColumnInfo deepest_info;
    for (size_t i = 0; i < target_list.size(); ++i) {
        if (!m_target_expr_list[i]->GetDeepestColumn(&deepest_info)) {
            m_deepest_column_affect_id[i] = -1;
        } else {
            m_deepest_column_affect_id[i] = deepest_info.m_affect_id;
        }
    }

    // m_within_agg_info_list, m_min_within_level
    m_min_within_level = UINT_MAX;
    for (size_t i = 0; i < target_list.size(); ++i) {
        std::vector<ShrAggExpr> agg_exprs =
            target_list[i]->GetAggregateFunctionExpression();

        for (size_t agg = 0; agg < agg_exprs.size(); ++agg) {
            InitWithinAggExpr(i,
                              target_list[i]->GetWithinLevel(),
                              agg_exprs[agg]);
        }
    }

    InitExpressionAffectedColumnInfo();
    SetupAggregates();
}

WithinPlan::WithinPlan(const PlanProto& proto)
    : m_new_input_tuple(NULL) {
    ParseFromProto(proto);

    InitExpressionAffectedColumnInfo();
    SetupAggregates();
}

void WithinPlan::InitExpressionAffectedColumnInfo() {
    m_target_affected_columns.resize(m_target_expr_list.size());

    for (size_t i = 0; i < m_target_expr_list.size(); ++i) {
        std::vector<AffectedColumnInfo> affected_columns;
        m_target_expr_list[i]->GetAffectedColumns(&affected_columns);

        for (size_t j = 0; j < affected_columns.size(); ++j) {
            m_target_affected_columns[i].push_back(
                affected_columns[j].m_affect_id);
        }
    }
}

bool WithinPlan::HasBlock(const TupleType& tuple, size_t target_index) {
    const std::vector<int> columns = m_target_affected_columns[target_index];

    for (size_t i = 0; i < columns.size(); ++i) {
        if (tuple[i]->m_has_block) {
            return true;
        }
    }
    return false;
}

void WithinPlan::InitWithinAggExpr(size_t target_index,
                                   uint32_t within_level,
                                   ShrAggExpr agg_expr) {
    WithinInfo info;
    info.m_within_level = within_level;
    info.m_agg_expr = agg_expr;
    info.m_target_index = target_index;

    std::vector<AffectedColumnInfo> affected_columns;
    agg_expr->GetAffectedColumns(&affected_columns);
    for (size_t i = 0; i < affected_columns.size(); ++i) {
        info.m_affected_column_id.push_back(affected_columns[i].m_affect_id);
    }

    m_within_agg_info_list.push_back(info);
    if (info.m_within_level < m_min_within_level) {
        m_min_within_level = info.m_within_level;
    }
}

void WithinPlan::SetupAggregates() {
    m_unevaluated_tuple_start.resize(m_target_expr_list.size());
    for (size_t i = 0; i < m_within_agg_info_list.size(); ++i) {
        m_within_agg_info_list[i].m_agg_expr->Setup();
    }
}

void WithinPlan::TearDownAggregates() {
    for (size_t i = 0; i < m_within_agg_info_list.size(); ++i) {
        m_within_agg_info_list[i].m_agg_expr->TearDown();
    }
}

bool WithinPlan::GetNextTuple(const std::vector<DatumBlock*>& tuple,
                              uint32_t* select_level) {
    while (!GetValidTuple(tuple, select_level)) {
        if (!GenerateNewGroup()) {
            return false;
        }
    }
    return true;
}

bool WithinPlan::GetValidTuple(const std::vector<DatumBlock*>& tuple,
                               uint32_t* select_level) {
    while (!m_buffered_output_tuple_list.empty()) {
        const TupleType& buffered_tuple
            = *m_buffered_output_tuple_list[0];
        CHECK_EQ(buffered_tuple.size(), tuple.size()) << "tuple size not match";

        bool is_tuple_valid = false;

        *select_level = 0;
        for (size_t i = 0; i < tuple.size(); ++i) {
            *tuple[i] = *buffered_tuple[i];
            if (tuple[i]->m_rep_level > *select_level) {
                *select_level = tuple[i]->m_rep_level;
            }

            if (tuple[i]->m_has_block) {
                is_tuple_valid = true;
            }
        }

        ReleaseTupleStorage(m_buffered_input_tuple_list[0]);
        ReleaseTupleStorage(m_buffered_output_tuple_list[0]);
        m_buffered_input_tuple_list.erase(m_buffered_input_tuple_list.begin());
        m_buffered_output_tuple_list.erase(
            m_buffered_output_tuple_list.begin());

        if (is_tuple_valid) {
            return true;
        }
    }
    return false;
}

void WithinPlan::ReleaseTupleStorage(TupleType* tuple) {
    for (size_t i = 0; i < tuple->size(); ++i) {
        delete (*tuple)[i];
    }
    delete tuple;
}

bool WithinPlan::GenerateNewGroup() {
    ResetUnevaluateTupleStart();

    m_is_within_target_affected.resize(m_target_expr_list.size());
    for (size_t i = 0; i < m_is_within_target_affected.size(); ++i) {
        m_is_within_target_affected[i] = false;
    }

    uint32_t input_tuple_select_level;

    // transfer one tuple first
    if (!GetNewTuple(&input_tuple_select_level)) {
        return false;
    }
    ProjectAndTransferNewTuple();

    while (GetNewTuple(&input_tuple_select_level)) {
        // judge where group end base on select_level of this tuple
        EndGroup(input_tuple_select_level);

        if (IsAllGroupEnd(input_tuple_select_level)) {
            break;
        } else {
            ProjectAndTransferNewTuple();
        }
    }

    // can't get any tuple from subplan, end this group
    if (m_new_input_tuple == NULL) {
        EndGroup(0);
    }

    // check input tuple size == output tuple size
    CHECK_EQ(m_buffered_input_tuple_list.size(),
             m_buffered_output_tuple_list.size());

    return (!m_buffered_output_tuple_list.empty());
}

bool WithinPlan::GetNewTuple(uint32_t* new_tuple_select_level) {
    if (m_new_input_tuple != NULL) {
        *new_tuple_select_level = m_new_input_tuple_select_level;
        return true;
    }

    m_new_input_tuple = AllocateNewInputTupleStorage();
    if (m_subplan->GetNextTuple(*m_new_input_tuple,
                                &m_new_input_tuple_select_level)) {
        *new_tuple_select_level = m_new_input_tuple_select_level;
        return true;
    }

    // can't get tuple from subplan
    ReleaseTupleStorage(m_new_input_tuple);
    m_new_input_tuple = NULL;
    return false;
}

void WithinPlan::ProjectAndTransferNewTuple() {
    TupleType* new_output_tuple =
        EvaluatePlainExpression(
            *m_new_input_tuple, m_new_input_tuple_select_level);

    TransAllAgg(*m_new_input_tuple, m_new_input_tuple_select_level);

    if (new_output_tuple != NULL) {
        m_buffered_input_tuple_list.push_back(m_new_input_tuple);
        m_buffered_output_tuple_list.push_back(new_output_tuple);
    } else {
        ReleaseTupleStorage(m_new_input_tuple);
    }

    m_new_input_tuple = NULL;
}

void WithinPlan::ResetUnevaluateTupleStart() {
    for (size_t i = 0; i < m_unevaluated_tuple_start.size(); ++i) {
        m_unevaluated_tuple_start[i] = 0;
    }
}

bool WithinPlan::IsAllGroupEnd(uint32_t select_level) {
    return m_min_within_level >= select_level;
}

void WithinPlan::TransAllAgg(
    const TupleType& new_tuple, uint32_t select_level) {
    for (size_t i = 0; i < m_within_agg_info_list.size(); ++i) {
        if (IsAggregateExpressionAffected(new_tuple, select_level, i)) {
            m_within_agg_info_list[i].m_agg_expr->Transfer(new_tuple);
            size_t target_index = m_within_agg_info_list[i].m_target_index;
            m_is_within_target_affected[target_index] = true;
        }
    }
}

bool WithinPlan::IsAggregateExpressionAffected(const TupleType& new_tuple,
                                               uint32_t select_level,
                                               size_t within_agg_index) {
    const WithinInfo& within_info = m_within_agg_info_list[within_agg_index];

    for (size_t i = 0; i < within_info.m_affected_column_id.size(); ++i) {
        int affected_id = within_info.m_affected_column_id[i];

        // has block indicate that this block is not duplicate because of
        // 1. not equal length array
        // 2. lower select level
        if (new_tuple[affected_id]->m_has_block) {
            return true;
        }
    }
    return false;
}

void WithinPlan::EndGroup(uint32_t select_level) {
    // finalize aggregates
    for (size_t i = 0; i < m_within_agg_info_list.size(); ++i) {
        if (m_within_agg_info_list[i].m_within_level >= select_level) {
            m_within_agg_info_list[i].m_agg_expr->Finalize();
        }
    }

    // evaluate the corresponding targets
    for (size_t i = 0; i < m_target_within_level.size(); ++i) {
        if (TargetHasWithin(i) && m_target_within_level[i] >= select_level) {
            EvaluateWithinTarget(i);
            m_is_within_target_affected[i] = false;
        }
    }

    // tear down and resetup aggregates
    for (size_t i = 0; i < m_within_agg_info_list.size(); ++i) {
        if (m_within_agg_info_list[i].m_within_level >= select_level) {
            m_within_agg_info_list[i].m_agg_expr->TearDown();
            m_within_agg_info_list[i].m_agg_expr->Setup();
        }
    }
}

std::vector<DatumBlock*>* WithinPlan::EvaluatePlainExpression(
    const TupleType& input_tuple, uint32_t select_level) {

    // first make sure we need a new output tuple
    size_t i = 0;
    for (i = 0; i < m_target_expr_list.size(); ++i) {
        if (GetRepetitionLevelForTarget(i, input_tuple) >= select_level) {
            break;
        }
    }

    // all target are duplicate
    if (i >= m_target_expr_list.size()) {
        return NULL;
    }

    TupleType* output_tuple = new TupleType();
    output_tuple->resize(m_target_expr_list.size());

    for (i = 0; i < m_target_expr_list.size(); ++i) {
        (*output_tuple)[i] =
            new DatumBlock(m_target_expr_list[i]->GetReturnType());

        if (TargetHasWithin(i)) {
            continue;
        }

        // evaluate expressions that is not agg
        m_target_expr_list[i]->Evaluate(input_tuple, (*output_tuple)[i]);
        SetProjectionOutputLevel(i, input_tuple, output_tuple);
        (*output_tuple)[i]->m_has_block = HasBlock(input_tuple, i);
    }
    return output_tuple;
}

void WithinPlan::SetProjectionOutputLevel(
    size_t target_index,
    const TupleType& input_tuple,
    TupleType* output_tuple) {
    DatumBlock* datum = (*output_tuple)[target_index];

    datum->m_rep_level =
        GetRepetitionLevelForTarget(target_index, input_tuple);
    datum->m_def_level =
        GetDefinitionLevelForTarget(target_index, input_tuple, *output_tuple);
}

void WithinPlan::EvaluateWithinTarget(size_t target_index) {
    size_t unevaluated_start = m_unevaluated_tuple_start[target_index];

    // no output tuple need to evaluate
    if (unevaluated_start >= m_buffered_output_tuple_list.size()) {
        return;
    }

    const std::vector<DatumBlock*>& input_tuple
        = *m_buffered_input_tuple_list[unevaluated_start];
    const std::vector<DatumBlock*>& output_tuple
        = *m_buffered_output_tuple_list[unevaluated_start];

    m_target_expr_list[target_index]->Evaluate(
        input_tuple, output_tuple[target_index]);
    output_tuple[target_index]->m_has_block =
        m_is_within_target_affected[target_index];

    // Set repetition level and definition level
    output_tuple[target_index]->m_rep_level =
        GetRepetitionLevelForTarget(target_index, input_tuple);
    output_tuple[target_index]->m_def_level =
        GetDefinitionLevelForTarget(target_index, input_tuple, output_tuple);

    // Set unevaluated aggregate target
    for (size_t i = unevaluated_start + 1;
         i < m_buffered_output_tuple_list.size(); ++i) {
        *((*m_buffered_output_tuple_list[i])[target_index]) =
            *((*m_buffered_output_tuple_list[unevaluated_start])[target_index]);
        ((*m_buffered_output_tuple_list[i])[target_index])->m_has_block = false;
    }

    m_unevaluated_tuple_start[target_index] =
        m_buffered_input_tuple_list.size();
}

uint32_t WithinPlan::GetRepetitionLevelForTarget(
    size_t target_index, const TupleType& input_tuple) {
    int affect_id = m_deepest_column_affect_id[target_index];

    if (TargetHasWithin(target_index)) {
        if (input_tuple[affect_id]->m_rep_level <
            m_target_within_level[target_index]) {
            return input_tuple[affect_id]->m_rep_level;
        }
        return m_target_within_level[target_index];
    }

    // plain target without within
    if (affect_id == -1) {
        return 0;
    } else {
        return input_tuple[affect_id]->m_rep_level;
    }
}

uint32_t WithinPlan::GetDefinitionLevelForTarget(
    size_t target_index,
    const TupleType& input_tuple,
    const TupleType& output_tuple) {
    if (TargetHasWithin(target_index)) {
        if (!output_tuple[target_index]->m_is_null) {
            return m_target_define_level[target_index];
        } else {
            int affect_id = m_deepest_column_affect_id[target_index];
            return std::min(input_tuple[affect_id]->m_def_level,
                            m_target_define_level[target_index] - 1);
        }
    }

    // plain target without within
    int affect_id = m_deepest_column_affect_id[target_index];
    DatumBlock* datum = output_tuple[target_index];

    if (!datum->m_is_null) {
        return m_target_define_level[target_index];
    }

    if (input_tuple[affect_id]->m_is_null) {
        return input_tuple[affect_id]->m_def_level;
    } else {
        return m_target_define_level[target_index] - 1;
    }
}

std::vector<DatumBlock*>* WithinPlan::AllocateNewInputTupleStorage() {
    TupleType* new_tuple = new TupleType();
    new_tuple->resize(m_subplan_tuple_types.size());

    for (size_t i = 0; i < m_subplan_tuple_types.size(); ++i) {
        (*new_tuple)[i] = new DatumBlock(m_subplan_tuple_types[i]);
    }
    return new_tuple;
}

void WithinPlan::CopyToProto(PlanProto* proto) const {
    proto->set_type(PlanProto::kWithin);

    // subplan
    WithinPlanProto* within_proto = proto->mutable_within();
    PlanProto* subplan_proto = within_proto->mutable_subplan();
    m_subplan->CopyToProto(subplan_proto);

    // subplan_tuple_type
    for (size_t i = 0; i < m_subplan_tuple_types.size(); ++i) {
        int* type =
            (within_proto->mutable_subplan_tuple_types())->Add();
        *type =
            BigQueryType::ConvertBQTypeToPBProtoType(m_subplan_tuple_types[i]);
    }

    // target_expr_list, target_define_level, target_within_level
    // deepest_column_affect_id
    for (size_t i = 0; i < m_target_expr_list.size(); i++) {
        ExpressionProto* expr_proto =
            (within_proto->mutable_target_expr_list())->Add();
        m_target_expr_list[i]->CopyToProto(expr_proto);

        uint32_t* target_define_level =
            (within_proto->mutable_target_define_level())->Add();

        *target_define_level = m_target_define_level[i];

        uint32_t* target_within_level =
            (within_proto->mutable_target_within_level())->Add();
        *target_within_level = m_target_within_level[i];

        int* affect_id =
            (within_proto->mutable_deepest_column_affect_id())->Add();
        *affect_id = m_deepest_column_affect_id[i];
    }
}

void WithinPlan::ParseFromProto(const PlanProto& proto) {
    CHECK(proto.type() == PlanProto::kWithin)
        << "Proto:" << proto.DebugString() << " is not within plan proto";
    const WithinPlanProto& within_proto = proto.within();

    // subplan, subplan_tuple_types
    m_subplan.reset(Plan::InitPlanFromProto(within_proto.subplan()));
    for (int i = 0; i < within_proto.subplan_tuple_types_size(); ++i) {
        m_subplan_tuple_types.push_back(
            BigQueryType::ConvertPBProtoTypeToBQType(
                within_proto.subplan_tuple_types(i)));
    }

    CHECK(within_proto.target_expr_list_size() ==
          within_proto.target_define_level_size())
        << "Projection Plan is possibly damaged";

    // target_expr_list, target_define_level, target_within_level
    // deepest_column_affect_id
    // within_info_list, min_within_level
    m_min_within_level = UINT_MAX;
    for (int i = 0; i < within_proto.target_expr_list_size(); ++i) {
        const ExpressionProto& expr_proto =
            within_proto.target_expr_list(i);

        ExpressionInitializer initializer;
        std::shared_ptr<Expression> expr =
            initializer.InitExpressionFromProto(expr_proto);

        m_target_expr_list.push_back(expr);
        m_target_define_level.push_back(within_proto.target_define_level(i));
        m_target_within_level.push_back(within_proto.target_within_level(i));
        m_deepest_column_affect_id.push_back(
            within_proto.deepest_column_affect_id(i));

        SetWithinInfo(initializer, i, m_target_within_level[i]);
    }
}

void WithinPlan::SetWithinInfo(const ExpressionInitializer& initializer,
                               size_t target_index,
                               uint32_t within_level) {
    // within_info_list, m_min_within_level
    const std::vector<ShrAggExpr>& agg_exprs =
        initializer.GetAggregateFunctionExpression();

    for (size_t i = 0; i < agg_exprs.size(); ++i) {
        InitWithinAggExpr(target_index, within_level, agg_exprs[i]);
    }
}

} // namespace compiler
} // namespace gunir

