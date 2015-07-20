// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_FILTER_PLAN_H
#define  GUNIR_COMPILER_FILTER_PLAN_H

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/shared_ptr.h"

#include "gunir/compiler/expression.h"
#include "gunir/compiler/plan.h"

namespace gunir {
namespace compiler {

class FilterPlan : public Plan {
public:
    FilterPlan(Plan* subplan, const std::shared_ptr<Expression>& e)
        : m_subplan(subplan),
          m_filter(e),
          m_select_level(0) {
    }

    explicit FilterPlan(const PlanProto& proto)
        : m_select_level(0) {
        ParseFromProto(proto);
    }

    ~FilterPlan() {}

    bool GetNextTuple(const std::vector<DatumBlock*>& tuple,
                      uint32_t* select_level);

    void CopyToProto(PlanProto* proto) const;

    void ParseFromProto(const PlanProto& proto);

    virtual void SetScanner(const std::vector<io::Scanner*>& scanners) {
        m_subplan->SetScanner(scanners);
    }

private:
    void SetRepLevel(const std::vector<DatumBlock*>& tuple,
                     uint32_t old_select_level);

private:
    toft::scoped_ptr<Plan> m_subplan;
    std::shared_ptr<Expression> m_filter;

    uint32_t m_select_level;
};

} // namespace compiler
} // namespace gunir


#endif  // GUNIR_COMPILER_FILTER_PLAN_H

