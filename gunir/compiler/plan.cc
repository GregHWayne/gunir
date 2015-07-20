// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/plan.h"

#include "gunir/compiler/aggregate_plan.h"
#include "gunir/compiler/filter_plan.h"
#include "gunir/compiler/join_plan.h"
#include "gunir/compiler/limit_plan.h"
#include "gunir/compiler/merge_plan.h"
#include "gunir/compiler/projection_plan.h"
#include "gunir/compiler/scan_plan.h"
#include "gunir/compiler/sort_plan.h"
#include "gunir/compiler/union_plan.h"
#include "gunir/compiler/uniq_plan.h"
#include "gunir/compiler/within_plan.h"

namespace gunir {
namespace compiler {

Plan* Plan::InitPlanFromProto(const PlanProto& proto) {
    switch (proto.type()) {
    case PlanProto::kScan:
        return new ScanPlan(proto);

    case PlanProto::kFilter:
        return new FilterPlan(proto);

    case PlanProto::kProjection:
        return new ProjectionPlan(proto);

    case PlanProto::kWithin:
        return new WithinPlan(proto);

    case PlanProto::kLimit:
        return new LimitPlan(proto);

    case PlanProto::kUnion:
        return new UnionPlan(proto);

    case PlanProto::kSort:
        return new SortPlan(proto);

    case PlanProto::kUniq:
        return new UniqPlan(proto);

    case PlanProto::kMerge:
        return new MergePlan(proto);

    case PlanProto::kAggregate:
        return new AggregatePlan(proto);

//     case PlanProto::kJoin:
//         return new JoinPlan(proto);
    default:
        LOG(FATAL) << "Not handled plan proto type:"
            << proto.DebugString();
        return NULL;
    }
}

} // namespace compiler
} // namespace gunir

