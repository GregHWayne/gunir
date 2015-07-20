// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_PLAN_H
#define  GUNIR_COMPILER_PLAN_H

#include <vector>

#include "gunir/compiler/big_query_functions.h"
#include "gunir/compiler/parser/plan.pb.h"
#include "gunir/io/scanner.h"

namespace gunir {
namespace compiler {

class Plan {
public:
    Plan() {}
    virtual ~Plan() {}

    virtual bool GetNextTuple(
        const std::vector<DatumBlock*>& tuple, uint32_t* select_level) = 0;

    virtual void CopyToProto(PlanProto* proto) const = 0;
    virtual void ParseFromProto(const PlanProto& proto) = 0;

    virtual void SetScanner(const std::vector<io::Scanner*>& scanners) = 0;

    static Plan* InitPlanFromProto(const PlanProto& proto);
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_PLAN_H

