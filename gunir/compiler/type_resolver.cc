// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/type_resolver.h"

namespace gunir {
namespace compiler {

TypeResolver::TypeResolver() {
    for (int i = 0; i <= MAX_TYPE_RANGE; ++i)
        for (int j = 0; j <= MAX_TYPE_RANGE; ++j)
            m_cast_rules[i][j] = false;

    LoadCastRules();
}

TypeResolver::~TypeResolver() {}

void TypeResolver::LoadCastRules() {
    // cast int32
    AddRule(BigQueryType::INT32, BigQueryType::INT64);
    AddRule(BigQueryType::INT32, BigQueryType::FLOAT);
    AddRule(BigQueryType::INT32, BigQueryType::DOUBLE);

    // cast uint32
    AddRule(BigQueryType::UINT32, BigQueryType::INT64);
    AddRule(BigQueryType::UINT32, BigQueryType::UINT64);
    AddRule(BigQueryType::UINT32, BigQueryType::FLOAT);
    AddRule(BigQueryType::UINT32, BigQueryType::DOUBLE);

    // cast int64
    AddRule(BigQueryType::INT64, BigQueryType::FLOAT);
    AddRule(BigQueryType::INT64, BigQueryType::DOUBLE);

    // cast uint64
    AddRule(BigQueryType::UINT64, BigQueryType::FLOAT);
    AddRule(BigQueryType::UINT64, BigQueryType::DOUBLE);

    // cast float
    AddRule(BigQueryType::FLOAT, BigQueryType::DOUBLE);

    // All types can be cast to BYTES
    AddRule(BigQueryType::BOOL, BigQueryType::BYTES);
    AddRule(BigQueryType::INT32, BigQueryType::BYTES);
    AddRule(BigQueryType::UINT32, BigQueryType::BYTES);
    AddRule(BigQueryType::INT64, BigQueryType::BYTES);
    AddRule(BigQueryType::UINT64, BigQueryType::BYTES);
    AddRule(BigQueryType::FLOAT, BigQueryType::BYTES);
    AddRule(BigQueryType::DOUBLE, BigQueryType::BYTES);
    AddRule(BigQueryType::STRING, BigQueryType::BYTES);
    AddRule(BigQueryType::ENUM, BigQueryType::BYTES);
}

} // namespace compiler
} // namespace gunir

