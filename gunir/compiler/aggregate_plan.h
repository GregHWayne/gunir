// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_AGGREGATE_PLAN_H
#define  GUNIR_COMPILER_AGGREGATE_PLAN_H

#include <vector>

#include "toft/base/scoped_ptr.h"
// #include "toft/base/shared_ptr.h"

#include "gunir/compiler/agg_expr_info.h"
#include "gunir/compiler/big_query_functions.h"
#include "gunir/compiler/plan.h"
#include "gunir/compiler/target.h"
#include "gunir/compiler/tuple_comparator.h"

namespace gunir {
namespace compiler {

class AggregatePlan : public Plan {
private:
    typedef std::vector<DatumBlock*> TupleType;
    typedef std::shared_ptr<Expression> ShrExpr;
    typedef std::shared_ptr<Target> ShrTarget;
    typedef std::shared_ptr<AggregateFunctionExpression> ShrAggExpr;

public:
    AggregatePlan(
        Plan* subplan,
        AggregatePlanProto::AggregateMode mode,
        const std::vector<ShrTarget>& final_targets,
        const std::vector<GroupByColumn>& m_groupby_columns,
        const std::vector<BQType>& subplan_tuple_types);

    explicit AggregatePlan(const PlanProto& proto);
    ~AggregatePlan();

    bool GetNextTuple(const std::vector<DatumBlock*>& tuple,
                      uint32_t* select_level);

    virtual void CopyToProto(PlanProto* proto) const;

    virtual void ParseFromProto(const PlanProto& proto);

    virtual void SetScanner(const std::vector<io::Scanner*>& scanners) {
        m_subplan->SetScanner(scanners);
    }

    struct LessCompare {
        bool operator() (AggExprInfo* left, AggExprInfo* right) {
            return *left < *right;
        }
    };

private:
    void SetupAggregate();

    bool GetFromNewGroup(const TupleType& tuple, uint32_t* select_level);

    void TransferThisTuple(const TupleType& tuple);

    bool GetFromCurrentGroup(const TupleType& this_group_start_tuple,
                             const TupleType& tuple);

    void TransferToThisGroup(const TupleType& tuple);

    void MergeToIntermediateGroup(const TupleType& tuple);

    bool GenerateIntermediateGroup(
        const TupleType& this_group_start_tuple,
        const TupleType& tuple);

    bool FinalizeAndEvaluateThisGroup(
        const TupleType& this_group_start_tuple,
        const TupleType& tuple);

    void AddAggregateExprInfo(
        const std::vector<ShrAggExpr>& agg_exprs);

    TupleType* CopyTuple(const TupleType& tuple);

    void ReleaseTuple(TupleType* tuple);

private:
    toft::scoped_ptr<Plan> m_subplan;
    AggregatePlanProto::AggregateMode m_mode;
    std::vector<ShrExpr> m_final_target_exprs;
    std::vector<BQType> m_subplan_tuple_types;
    std::vector<TupleOrderByInfo> m_orderby_infos;
    std::vector<int> m_final_define_levels;

    // Used at runtime
    TupleType* m_group_start_tuple;
    TupleType* m_save_start_tuple;
    toft::scoped_ptr<TupleComparator> m_comparator;
    std::vector<AggExprInfo*> m_aggs;
    bool m_inited;
    bool m_has_more_in_current_group;
    bool m_has_next_tuple;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_AGGREGATE_PLAN_H
