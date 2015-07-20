// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_PROJECTION_PLAN_H
#define  GUNIR_COMPILER_PROJECTION_PLAN_H

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/shared_ptr.h"

#include "gunir/compiler/big_query_functions.h"
#include "gunir/compiler/plan.h"
#include "gunir/compiler/target.h"

namespace gunir {
namespace compiler {

class ProjectionPlan : public Plan {
public:
    ProjectionPlan(
        const std::vector<std::shared_ptr<Target> >& target_list,
        const std::vector<AffectedColumnInfo>& subplan_column_infos,
        Plan* subplan);

    explicit ProjectionPlan(const PlanProto& proto);

    ~ProjectionPlan();

    bool GetNextTuple(const std::vector<DatumBlock*>& tuple,
                      uint32_t* select_level);

    virtual void CopyToProto(PlanProto* proto) const;

    virtual void ParseFromProto(const PlanProto& proto);

    virtual void SetScanner(const std::vector<io::Scanner*>& scanners) {
        m_subplan->SetScanner(scanners);
    }

private:
    void SetResultLevel(size_t target_id, DatumBlock* block);
    bool HasBlock(size_t target_index);
    void InitExpressionAffectedColumnInfo();

    void AllocateSubPlanStorage();

private:
    toft::scoped_ptr<Plan> m_subplan;
    std::vector<std::shared_ptr<Expression> > m_target_expr_list;
    std::vector<uint32_t> m_target_define_level;
    std::vector<int> m_deepest_column_affect_id;
    std::vector<std::vector<int> > m_target_affected_columns;
    std::vector<BQType> m_subplan_tuple_types;

    std::vector<DatumBlock*> m_datum_row;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_PROJECTION_PLAN_H

