// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_BIG_QUERY_FUNCTIONS_H
#define  GUNIR_COMPILER_BIG_QUERY_FUNCTIONS_H

#include "gunir/compiler/datum_block.h"

namespace gunir {
namespace compiler {

struct BQFunctionInfo {
    static const int kMaxArgumentNumber = 3;

    Datum arg[kMaxArgumentNumber];
    BQType arg_type[kMaxArgumentNumber];
    bool is_arg_null[kMaxArgumentNumber];

    Datum* return_datum;
    bool is_null; // return

    BQFunctionInfo() {
        memset(static_cast<void*>(this), 0, sizeof(BQFunctionInfo));
        for (int i = 0; i < kMaxArgumentNumber; i++)
            is_arg_null[i] = true;
        is_null = false;
    }
};

typedef void (* BQFunction) (BQFunctionInfo* info);

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_BIG_QUERY_FUNCTIONS_H
