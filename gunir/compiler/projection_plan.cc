// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/projection_plan.h"

#include "gunir/compiler/expression_initializer.h"

namespace gunir {
namespace compiler {

ProjectionPlan::ProjectionPlan(
    const std::vector<std::shared_ptr<Target> >& target_list,
    const std::vector<AffectedColumnInfo>& subplan_affect_column_infos,
    Plan* subplan)
    : m_subplan(subplan) {

    // init expr_list
    for (size_t i = 0; i < target_list.size(); i++) {
        m_target_expr_list.push_back(target_list[i]->GetExpression());
    }

    // init deepest column
    DeepestColumnInfo deepest_info;
    for (size_t i = 0; i < target_list.size(); ++i) {
        if (!m_target_expr_list[i]->GetDeepestColumn(&deepest_info)) {
            m_deepest_column_affect_id.push_back(-1);
        } else {
            m_deepest_column_affect_id.push_back(deepest_info.m_affect_id);
        }

        const ColumnInfo& column_info = target_list[i]->GetResultColumnInfo();
        m_target_define_level.push_back(column_info.m_define_level);
    }

    for (size_t i = 0; i < subplan_affect_column_infos.size(); ++i) {
        m_subplan_tuple_types.push_back(
            subplan_affect_column_infos[i].m_column_info.m_type);
    }

    InitExpressionAffectedColumnInfo();
    AllocateSubPlanStorage();
}

ProjectionPlan::ProjectionPlan(const PlanProto& proto) {
    ParseFromProto(proto);

    InitExpressionAffectedColumnInfo();
    AllocateSubPlanStorage();
}

void ProjectionPlan::InitExpressionAffectedColumnInfo() {
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

void ProjectionPlan::AllocateSubPlanStorage() {
    // init subplan datum block tuple
    m_datum_row.resize(m_subplan_tuple_types.size());
    for (size_t i = 0; i < m_datum_row.size(); ++i) {
        m_datum_row[i] = new DatumBlock(m_subplan_tuple_types[i]);
    }
}

ProjectionPlan::~ProjectionPlan() {
    for (size_t i = 0; i < m_datum_row.size(); ++i) {
        delete m_datum_row[i];
    }
}

bool ProjectionPlan::GetNextTuple(
    const std::vector<DatumBlock*>& tuple, uint32_t* select_level) {
    CHECK(tuple.size() == m_target_expr_list.size())
        << "Tuple size not match,"
        << " target_expr_list.size():" << m_target_expr_list.size()
        << ", tuple_size:" << tuple.size();

    while (m_subplan->GetNextTuple(m_datum_row, select_level)) {
        bool is_tuple_valid = false;

        for (size_t i = 0; i < m_target_expr_list.size(); i++) {
            tuple[i]->m_has_block = HasBlock(i);
            if (tuple[i]->m_has_block) {
                is_tuple_valid = true;
            }

            bool is_succeed =
                m_target_expr_list[i]->Evaluate(m_datum_row, tuple[i]);
            CHECK(is_succeed) << "Evaluate is not succeed";
            SetResultLevel(i, tuple[i]);
        }

        if (is_tuple_valid) {
            return true;
        }
    }
    return false;
}

bool ProjectionPlan::HasBlock(size_t target_index) {
    const std::vector<int>& columns = m_target_affected_columns[target_index];

    for (size_t i = 0; i < columns.size(); ++i) {
        if (m_datum_row[columns[i]]->m_has_block) {
            return true;
        }
    }
    return false;
}

void ProjectionPlan::SetResultLevel(size_t target_id,
                                    DatumBlock* block) {
    // 1. get deepest column(deepest column is the column show in result
    // schema) check whether it's null
    int affect_id = m_deepest_column_affect_id[target_id];

    // 2. set rep level
    if (affect_id == -1) {
        // target is a const expression
        block->m_rep_level = 0;
    } else {
        // target contains column, use the rep level of deepest column
        block->m_rep_level = m_datum_row[affect_id]->m_rep_level;
    }

    // 3. result is not null, def level is max level of target
    if (!block->m_is_null) {
        block->m_def_level = m_target_define_level[target_id];
        return;
    }

    // 4. deepest column is null, then use it's def level
    // 5. deepest column is not null, max_def - 1
    if (m_datum_row[affect_id]->m_is_null) {
        block->m_def_level = m_datum_row[affect_id]->m_def_level;
    } else {
        block->m_def_level = m_target_define_level[target_id] - 1;
    }
}

void ProjectionPlan::CopyToProto(PlanProto* proto) const {
    proto->set_type(PlanProto::kProjection);

    ProjectionPlanProto* projection_proto = proto->mutable_projection();

    for (size_t i = 0; i < m_subplan_tuple_types.size(); ++i) {
        int* type =
            (projection_proto->mutable_subplan_tuple_types())->Add();
        *type =
            BigQueryType::ConvertBQTypeToPBProtoType(m_subplan_tuple_types[i]);
    }

    for (size_t i = 0; i < m_target_expr_list.size(); i++) {
        ExpressionProto* expr_proto =
            (projection_proto->mutable_target_expr_list())->Add();
        uint32_t* target_define_level =
            (projection_proto->mutable_target_define_level())->Add();
        int* affect_id =
            (projection_proto->mutable_deepest_column_affect_id())->Add();

        m_target_expr_list[i]->CopyToProto(expr_proto);
        *target_define_level = m_target_define_level[i];
        *affect_id = m_deepest_column_affect_id[i];
    }

    PlanProto* subplan_proto = projection_proto->mutable_subplan();
    m_subplan->CopyToProto(subplan_proto);
}

void ProjectionPlan::ParseFromProto(const PlanProto& proto) {
    CHECK(proto.type() == PlanProto::kProjection)
        << "Proto:" << proto.DebugString()
        << " is not projection plan proto";

    const ProjectionPlanProto& projection_proto = proto.projection();

    CHECK(projection_proto.target_expr_list_size() ==
          projection_proto.target_define_level_size())
        << "Projection Plan is possibly damaged";
    CHECK(projection_proto.target_expr_list_size() ==
          projection_proto.deepest_column_affect_id_size())
        << "Projection Plan is possibly damaged";

    ExpressionInitializer initializer;
    // 1. parse target expr list, define level and affect id
    for (int i = 0; i < projection_proto.target_expr_list_size(); ++i) {
        const ExpressionProto& expr_proto =
            projection_proto.target_expr_list(i);

        std::shared_ptr<Expression> expr =
            initializer.InitExpressionFromProto(expr_proto);

        m_target_expr_list.push_back(expr);
        m_target_define_level.push_back(projection_proto.target_define_level(i));
        m_deepest_column_affect_id.push_back(
            projection_proto.deepest_column_affect_id(i));
    }

    // 2. init subplan and storage
    // Allocate storage depends on subplan tuple type
    m_subplan.reset(Plan::InitPlanFromProto(projection_proto.subplan()));

    for (int i = 0; i < projection_proto.subplan_tuple_types_size(); ++i) {
        m_subplan_tuple_types.push_back(
            BigQueryType::ConvertPBProtoTypeToBQType(
                projection_proto.subplan_tuple_types(i)));
    }
}

} // namespace compiler
} // namespace gunir

