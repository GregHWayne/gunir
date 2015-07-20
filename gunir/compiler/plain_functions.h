// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_PLAIN_FUNCTIONS_H
#define  GUNIR_COMPILER_PLAIN_FUNCTIONS_H

#include <string>
#include <vector>

#include "gunir/compiler/big_query_functions.h"
#include "gunir/compiler/parser/big_query_types.h"

namespace gunir {
namespace compiler {

struct PlainFunctionInfo {
    std::string m_fn_name;
    BQFunction m_fn_addr;

    BQType m_return_type;
    std::vector<BQType> m_arg_types;

    PlainFunctionInfo(const std::string& fn_name,
                      BQFunction fn_addr,
                      BQType* arg_types,
                      BQType return_type,
                      int arg_number) {
        m_fn_name = fn_name;
        m_fn_addr = fn_addr;
        m_return_type = return_type;

        for (int i = 0; i < arg_number; ++i)
            m_arg_types.push_back(arg_types[i]);
    }
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_PLAIN_FUNCTIONS_H
