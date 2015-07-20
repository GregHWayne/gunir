// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
#ifndef  GUNIR_COMPILER_JOIN_PLAN_H
#define  GUNIR_COMPILER_JOIN_PLAN_H

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/shared_ptr.h"

#include "gunir/compiler/expression.h"
#include "gunir/compiler/parser/select_stmt.pb.h"
#include "gunir/compiler/plan.h"

namespace gunir {
namespace compiler {

class JoinPlan : public Plan {
private:
    typedef std::vector<DatumBlock*> TupleType;

public:
    JoinPlan(const std::vector<Plan*>& subplans,
             const std::shared_ptr<Expression>& e,
             const JoinOperator& type,
             const std::vector<uint64_t>& left_affect_id,
             const std::vector<uint64_t>& right_affect_id);

    explicit JoinPlan(const PlanProto& proto);

    ~JoinPlan();

    bool GetNextTuple(const std::vector<DatumBlock*>& tuple,
                      uint32_t* select_level);

    void CopyToProto(PlanProto* proto) const;

    void ParseFromProto(const PlanProto& proto);

    virtual void SetScanner(const std::vector<io::Scanner*>& scanners) {
        CHECK_EQ(m_subplans.size(), scanners.size());
        for (size_t i = 0; i < m_subplans.size(); ++i) {
            std::vector<io::Scanner*> temp_scanner;
            temp_scanner.push_back(scanners[i]);
            m_subplans[i]->SetScanner(temp_scanner);
        }
    }

private:
    void BufferThisTuple(const TupleType& tuple);

    void ReleaseBufferedTuple();

    void ReleaseTuple(const TupleType& tuple);

    std::vector<DatumBlock*>* CopyTuple(const TupleType& tuple);

    void MergeTuples(const TupleType& tuple, size_t buffer_pos);

private:
    std::vector<Plan*> m_subplans;

    std::shared_ptr<Expression> m_condition;

    JoinOperator m_type;

    std::vector<uint64_t> m_left_affect_id;

    std::vector<uint64_t> m_right_affect_id;

    std::vector<TupleType*> m_buffered_tuples;

    TupleType* m_last_tuple;

    int32_t m_last_pos;

    bool m_has_more;
};
} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_JOIN_PLAN_H

