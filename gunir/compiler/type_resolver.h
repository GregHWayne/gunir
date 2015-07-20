// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_TYPE_RESOLVER_H
#define  GUNIR_COMPILER_TYPE_RESOLVER_H

#include "gunir/compiler/parser/big_query_types.h"

namespace gunir {
namespace compiler {

class TypeResolver {
public:
    TypeResolver();
    ~TypeResolver();

    inline bool CanCast(BQType from, BQType to) const;
    inline bool IsTypeMatch(BQType type1, BQType type2) const;

private:
    void LoadCastRules();
    inline void AddRule(BQType from, BQType to);

private:
    static const int MAX_TYPE_RANGE = BigQueryType::MAX_TYPE;

    bool m_cast_rules[MAX_TYPE_RANGE + 1][MAX_TYPE_RANGE + 1];
};

bool TypeResolver::CanCast(BQType from, BQType to) const {
    CHECK(from >= 0 && from <= MAX_TYPE_RANGE) <<
        "BQType from is invalid:" << from;
    CHECK(to >= 0 && to <= MAX_TYPE_RANGE) <<
        "BQType to is invalid:" << to;

    return m_cast_rules[from][to];
}

bool TypeResolver::IsTypeMatch(BQType type1, BQType type2) const {
    return (type1 == type2 || CanCast(type1, type2));
}

void TypeResolver::AddRule(BQType from, BQType to) {
    m_cast_rules[from][to] = true;
}

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_TYPE_RESOLVER_H
