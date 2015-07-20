// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_CAST_FUNCTIONS_H
#define  GUNIR_COMPILER_CAST_FUNCTIONS_H

#include <string>
#include <vector>

#include "gunir/compiler/big_query_functions.h"
#include "gunir/compiler/parser/big_query_types.h"

namespace gunir {
namespace compiler {

struct CastFunctionInfo {
    std::string m_fn_name;
    BQFunction m_fn_addr;

    BQType m_arg_type;
    BQType m_return_type;

    CastFunctionInfo(const std::string& fn_name,
                     BQFunction fn_addr,
                     BQType arg_type,
                     BQType return_type) {
        m_fn_name = fn_name;
        m_fn_addr = fn_addr;
        m_arg_type = arg_type;
        m_return_type = return_type;
    }
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_CAST_FUNCTIONS_H
