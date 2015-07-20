// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_LIMIT_PLAN_H
#define  GUNIR_COMPILER_LIMIT_PLAN_H

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "gunir/compiler/plan.h"

namespace gunir {
namespace compiler {

class LimitPlan : public Plan {
public:
    LimitPlan(Plan* subplan, int64_t start, int64_t number)
        : m_start(start),
          m_number(number),
          m_current(-1),
          m_subplan(subplan) {
    }

    explicit LimitPlan(const PlanProto& proto)
        : m_current(-1) {
        ParseFromProto(proto);
    }

    ~LimitPlan() {}

    bool GetNextTuple(const std::vector<DatumBlock*>& tuple,
                      uint32_t* select_level);

    virtual void CopyToProto(PlanProto* proto) const;

    virtual void ParseFromProto(const PlanProto& proto);

    virtual void SetScanner(const std::vector<io::Scanner*>& scanners) {
        m_subplan->SetScanner(scanners);
    }

private:
    int64_t m_start;
    int64_t m_number;

    int64_t m_current;
    toft::scoped_ptr<Plan> m_subplan;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_LIMIT_PLAN_H

