// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_AGGREGATE_FUNCTIONS_H
#define  GUNIR_COMPILER_AGGREGATE_FUNCTIONS_H

#include <string>

#include "gunir/compiler/big_query_functions.h"

namespace gunir {
namespace compiler {

struct AggregateFunctionInfo {
    std::string m_fn_name;

    BQFunction m_trans_fn;
    BQFunction m_merge_fn;
    BQFunction m_final_fn;

    BQType m_input_type;
    BQType m_final_type;

    // trans_type_size is valid only when trans_type is BYTES
    // allocated memory for COMPLICATE_TYPE will be init to zero
    int m_trans_type_size;
    BQType m_trans_type;

    AggregateFunctionInfo(const std::string& fn_name,
                          BQFunction trans_fn,
                          BQFunction merge_fn,
                          BQFunction final_fn,
                          BQType input_type,
                          BQType trans_type,
                          BQType final_type) {
        m_fn_name = fn_name;

        m_trans_fn = trans_fn;
        m_merge_fn = merge_fn;
        m_final_fn = final_fn;

        m_input_type = input_type;
        m_trans_type = trans_type;
        m_final_type = final_type;
    }

    AggregateFunctionInfo(const std::string& fn_name,
                          BQFunction trans_fn,
                          BQFunction merge_fn,
                          BQFunction final_fn,
                          BQType input_type,
                          int trans_type_size,
                          BQType final_type) {
        m_fn_name = fn_name;

        m_trans_fn = trans_fn;
        m_merge_fn = merge_fn;
        m_final_fn = final_fn;

        m_input_type = input_type;
        m_trans_type = BigQueryType::BYTES;
        m_trans_type_size = trans_type_size;
        m_final_type = final_type;
    }
};

} // namespace compiler
} // namespace gunir
#endif  // GUNIR_COMPILER_AGGREGATE_FUNCTIONS_H
