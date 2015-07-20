// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_UNION_PLAN_H
#define  GUNIR_COMPILER_UNION_PLAN_H

#include <vector>
#include "gunir/compiler/plan.h"

namespace gunir {
namespace compiler {

class UnionPlan : public Plan {
public:
    explicit UnionPlan(const std::vector<Plan*>& subplans);
    explicit UnionPlan(const PlanProto& proto);

    ~UnionPlan();

    bool GetNextTuple(const std::vector<DatumBlock*>& tuple,
                      uint32_t* select_level);

    void CopyToProto(PlanProto* proto) const;

    void ParseFromProto(const PlanProto& proto);

    void SetScanner(const std::vector<io::Scanner*>& scanners);

private:
    std::vector<Plan*> m_subplans;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_UNION_PLAN_H

