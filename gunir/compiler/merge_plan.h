// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_MERGE_PLAN_H
#define  GUNIR_COMPILER_MERGE_PLAN_H

#include <queue>
#include <vector>

#include "toft/base/scoped_ptr.h"

#include "gunir/compiler/parser/column_info.h"
#include "gunir/compiler/plan.h"
#include "gunir/compiler/tuple_comparator.h"

namespace gunir {
namespace compiler {

class MergePlan : public Plan {
private:
    typedef std::vector<DatumBlock*> TupleType;

    struct TupleItem {
        TupleType* m_tuple;
        uint32_t m_id;

        TupleItem(TupleType* t, size_t i)
            : m_tuple(t), m_id(i) {
        }
    };

    struct LessTupleItem {
        explicit LessTupleItem(TupleComparator comparator)
            : m_comparator(comparator) {
        }

        TupleComparator m_comparator;

        // min heap
        bool operator()(const TupleItem& t1, const TupleItem& t2) const {
            return (!m_comparator.Compare(*t1.m_tuple, *t2.m_tuple));
        }
    };

public:
    MergePlan(const std::vector<Plan*>& subplans,
              const std::vector<OrderByColumn>& orderby_columns);
    explicit MergePlan(const PlanProto& proto);

    ~MergePlan();

    bool GetNextTuple(const std::vector<DatumBlock*>& tuple,
                      uint32_t* select_level);

    void CopyToProto(PlanProto* proto) const;

    void ParseFromProto(const PlanProto& proto);

    void SetScanner(const std::vector<io::Scanner*>& scanners);

private:
    bool BuildHeap(const TupleType& tuple);
    bool GetNextValidTuple(const TupleType& tuple,
                           uint32_t* select_level);

    void ReturnTopTupleInHeap(const TupleType& tuple, uint32_t* select_level);
    void NewRecordForSubplan(const TupleType& tuple);
    void EndSubplan();

    TupleType* AllocateAndCopyTuple(const TupleType& tuple);
    void ReleaseTupleInItem(const TupleItem& item);
    void CopyTuple(const TupleType& src_tuple,
                   const TupleType& dest_tuple);

    bool HasMoreTuple(const TupleType& tuple);

private:
    std::vector<Plan*> m_subplans;
    std::vector<TupleOrderByInfo> m_orderby_infos;

    // Used at runtime
    Plan* m_curr_subplan;

    typedef std::priority_queue<TupleItem,
            std::vector<TupleItem>, LessTupleItem > TupleHeap;
    toft::scoped_ptr<TupleHeap> m_tuple_heap;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_MERGE_PLAN_H

