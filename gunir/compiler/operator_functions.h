// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_OPERATOR_FUNCTIONS_H
#define  GUNIR_COMPILER_OPERATOR_FUNCTIONS_H

#include <string>

#include "gunir/compiler/big_query_functions.h"

namespace gunir {
namespace compiler {

struct OperatorFunctionInfo {
    Operators m_op;

    BQFunction m_fn_addr;
    std::string m_fn_name;

    BQType m_return_type;
    BQType m_arg_type;
    int m_arg_number;

    OperatorFunctionInfo(Operators op,
                         const std::string& fn_name,
                         BQFunction fn_addr,
                         BQType return_type,
                         BQType arg_type,
                         int arg_number) {
        m_op = op;
        m_fn_name = fn_name;
        m_fn_addr = fn_addr;
        m_return_type = return_type;
        m_arg_type = arg_type;
        m_arg_number = arg_number;
    }
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_OPERATOR_FUNCTIONS_H
