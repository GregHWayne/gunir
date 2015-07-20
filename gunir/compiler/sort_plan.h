// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_SORT_PLAN_H
#define  GUNIR_COMPILER_SORT_PLAN_H

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/shared_ptr.h"

#include "gunir/compiler/expression.h"
#include "gunir/compiler/plan.h"
#include "gunir/compiler/tuple_comparator.h"

namespace gunir {
namespace compiler {

class SortPlan : public Plan {
private:
    typedef std::vector<DatumBlock*> TupleType;

    class RecordComparator {
    public:
        RecordComparator(TupleComparator tuple_comparator,
                         const std::vector<TupleType*>& tuples)
            : m_comparator(tuple_comparator),
            m_buffered_tuples(tuples) {
        }

        bool operator()(size_t i, size_t j) const {
            return m_comparator.Compare(
                *m_buffered_tuples[i], *m_buffered_tuples[j]);
        }

    private:
        TupleComparator m_comparator;
        const std::vector<TupleType*>& m_buffered_tuples;
    };

public:
    SortPlan(Plan* subplan,
             const std::vector<OrderByColumn>& orderby_columns);
    explicit SortPlan(const PlanProto& proto);
    ~SortPlan() { ReleaseBufferedTuple(); }

    bool GetNextTuple(
        const std::vector<DatumBlock*>& tuple, uint32_t* select_level);

    void CopyToProto(PlanProto* proto) const;
    void ParseFromProto(const PlanProto& proto);

    void SetScanner(const std::vector<io::Scanner*>& scanners) {
        m_subplan->SetScanner(scanners);
    }

private:
    void BufferThisTuple(const TupleType& tuple, uint32_t select_level);
    void SortBufferedRecords();

    void ReleaseBufferedTuple();
    std::vector<DatumBlock*>* CopyTuple(const TupleType& tuple);

private:
    toft::scoped_ptr<Plan> m_subplan;
    std::vector<TupleOrderByInfo> m_orderby_infos;

    // used at runtime
    std::vector<TupleType*> m_buffered_tuples;
    std::vector<uint32_t> m_select_levels;

    // the offset of records in m_buffered_tuples
    std::vector<size_t> m_record_offset;

    size_t m_next_record;
    size_t m_next_valid_tuple_offset;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_SORT_PLAN_H

