// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_WITHIN_PLAN_H
#define  GUNIR_COMPILER_WITHIN_PLAN_H

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/shared_ptr.h"

#include "gunir/compiler/aggregate_function_expression.h"
#include "gunir/compiler/big_query_functions.h"
#include "gunir/compiler/plan.h"
#include "gunir/compiler/target.h"

namespace gunir {
namespace compiler {

class WithinPlan : public Plan {
public:
    typedef std::shared_ptr<Expression> ShrExpr;
    typedef std::shared_ptr<AggregateFunctionExpression> ShrAggExpr;
    typedef std::vector<DatumBlock*> TupleType;

private:
    struct WithinInfo {
        uint32_t m_within_level; // e.g. WITHIN RECORD, within_level = 0
        ShrAggExpr m_agg_expr;
        std::vector<int> m_affected_column_id;
        size_t m_target_index;
    };

public:
    WithinPlan(
        const std::vector<std::shared_ptr<Target> >& target_list,
        const std::vector<AffectedColumnInfo>& subplan_affect_column_infos,
        Plan* subplan);
    explicit WithinPlan(const PlanProto& proto);

    ~WithinPlan() { TearDownAggregates(); }

    bool GetNextTuple(const TupleType& tuple, uint32_t* select_level);

    void CopyToProto(PlanProto* proto) const;
    void ParseFromProto(const PlanProto& proto);

    void SetScanner(const std::vector<io::Scanner*>& scanners) {
        m_subplan->SetScanner(scanners);
    }

private:
    void InitWithinAggExpr(size_t target_index,
                           uint32_t within_level,
                           ShrAggExpr agg_expr);
    void InitExpressionAffectedColumnInfo();

    void SetupAggregates();
    void TearDownAggregates();

    bool GetValidTuple(const std::vector<DatumBlock*>& tuple,
                       uint32_t* select_level);
    bool GenerateNewGroup();
    void EndGroup(uint32_t select_level);
    void ResetUnevaluateTupleStart();
    bool IsAllGroupEnd(uint32_t select_level);

    bool GetNewTuple(uint32_t* new_tuple_select_level);
    void ProjectAndTransferNewTuple();

    TupleType* AllocateNewInputTupleStorage();
    void ReleaseTupleStorage(TupleType* tuple);

    void TransAllAgg(const TupleType& new_tuple, uint32_t select_level);

    bool IsAggregateExpressionAffected(
        const TupleType& new_tuple,
        uint32_t select_level,
        size_t within_agg_index);

    void SetProjectionOutputLevel(
        size_t target_index,
        const TupleType& input_tuple,
        TupleType* output_tuple);

    uint32_t GetDefinitionLevelForTarget(
        size_t target_index,
        const TupleType& input_tuple,
        const TupleType& output_tuple);
    uint32_t GetRepetitionLevelForTarget(
        size_t target_index, const TupleType& input_tuple);

    TupleType* EvaluatePlainExpression(const TupleType& input_tuple,
                                       uint32_t select_level);
    void EvaluateWithinTarget(size_t target_index);

    void SetWithinInfo(const ExpressionInitializer& initializer,
                       size_t target_index,
                       uint32_t within_level);
    bool TargetHasWithin(size_t i) {
        // FIXME(codingliu): identify whether this target have within
        return m_target_within_level[i] != UINT_MAX;
    }

    bool HasBlock(const TupleType& tuple, size_t target_index);

private:
    toft::scoped_ptr<Plan> m_subplan;
    // the types of input tuple from subplan
    std::vector<BQType> m_subplan_tuple_types;

    std::vector<ShrExpr> m_target_expr_list;
    std::vector<uint32_t> m_target_define_level;
    std::vector<uint32_t> m_target_within_level;
    std::vector<int> m_deepest_column_affect_id;

    std::vector<WithinInfo> m_within_agg_info_list;
    uint32_t m_min_within_level;

    // Used At Runtime
    std::vector<TupleType*> m_buffered_output_tuple_list;
    std::vector<TupleType*> m_buffered_input_tuple_list;

    TupleType* m_new_input_tuple;
    uint32_t m_new_input_tuple_select_level;
    std::vector<size_t> m_unevaluated_tuple_start;
    std::vector<std::vector<int> > m_target_affected_columns;
    std::vector<bool> m_is_within_target_affected;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_WITHIN_PLAN_H

