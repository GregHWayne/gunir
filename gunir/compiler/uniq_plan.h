// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_UNIQ_PLAN_H
#define  GUNIR_COMPILER_UNIQ_PLAN_H

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/shared_ptr.h"

#include "gunir/compiler/expression.h"
#include "gunir/compiler/plan.h"
#include "gunir/compiler/tuple_comparator.h"

namespace gunir {
namespace compiler {

class UniqPlan : public Plan {
private:
    typedef std::vector<DatumBlock*> TupleType;

public:
    UniqPlan(Plan* subplan,
             std::vector<size_t> distinct_columns);
    explicit UniqPlan(const PlanProto& proto);
    ~UniqPlan();

    bool GetNextTuple(
        const std::vector<DatumBlock*>& tuple, uint32_t* select_level);

    void CopyToProto(PlanProto* proto) const;
    void ParseFromProto(const PlanProto& proto);

    void SetScanner(const std::vector<io::Scanner*>& scanners) {
        m_subplan->SetScanner(scanners);
    }

private:
    void InitComparator();

    bool ReturnFirstTuple(const TupleType& tuple, uint32_t* select_level);
    bool ReturnThisTuple(const TupleType& tuple, uint32_t select_level);
    void BufferThisTuple(const TupleType& tuple);

private:
    toft::scoped_ptr<Plan> m_subplan;

    // The element of m_distinct_columns is affect_id
    std::vector<size_t> m_distinct_columns;
    toft::scoped_ptr<TupleComparator> m_comparator;

    // Used at runtime
    TupleType* m_buffered_tuple;
    bool m_returned_first_tuple;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_UNIQ_PLAN_H

