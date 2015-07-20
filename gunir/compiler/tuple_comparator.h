// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_TUPLE_COMPARATOR_H
#define  GUNIR_COMPILER_TUPLE_COMPARATOR_H

#include <vector>

#include "gunir/compiler/big_query_functions.h"
#include "gunir/compiler/parser/select_stmt.pb.h"

namespace gunir {
namespace compiler {

struct TupleOrderByInfo {
    size_t m_affect_id;
    OrderType m_type;
};

class TupleComparator {
public:
    explicit TupleComparator(
        const std::vector<TupleOrderByInfo>& orderby_infos)
        : m_tuple_orderby_infos(orderby_infos) {
    }

    bool IsEqual(const std::vector<DatumBlock*>& tuple1,
                 const std::vector<DatumBlock*>& tuple2) const {
        for (size_t i = 0; i < m_tuple_orderby_infos.size(); ++i) {
            const TupleOrderByInfo& info = m_tuple_orderby_infos[i];

            const DatumBlock* b1 = tuple1[info.m_affect_id];
            const DatumBlock* b2 = tuple2[info.m_affect_id];

            if (b1->CompareDatumWith(*b2) != 0) {
                return false;
            }
        }
        return true;
    }

    // return:true, tuple1 should be in front of tuple2
    //       :false, tuple2 should be in front of tuple1
    bool Compare(const std::vector<DatumBlock*>& tuple1,
                 const std::vector<DatumBlock*>& tuple2) const {

        for (size_t i = 0; i < m_tuple_orderby_infos.size(); ++i) {
            const TupleOrderByInfo& info = m_tuple_orderby_infos[i];

            const DatumBlock* b1 = tuple1[info.m_affect_id];
            const DatumBlock* b2 = tuple2[info.m_affect_id];

            int r = b1->CompareDatumWith(*b2);
            if (r == 0) {
                continue;
            }

            switch (info.m_type) {
            case kAsc:
                return r < 0;

            case kDesc:
                return r > 0;
            }
        }
        return true;
    }

private:
    std::vector<TupleOrderByInfo> m_tuple_orderby_infos;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_TUPLE_COMPARATOR_H

