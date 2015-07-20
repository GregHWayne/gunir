// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_COMPILER_QUERY_CONVERTOR_H
#define  GUNIR_COMPILER_QUERY_CONVERTOR_H

#include <string>

#include "gunir/compiler/parser/select_stmt.pb.h"

namespace gunir {
namespace compiler {

class QueryConvertor {
public:
    QueryConvertor() {}
    ~QueryConvertor() {}
    bool Convert(SelectStmt* stmt, std::string* err);

private:
    bool CheckHasTopFunction(SelectStmt* stmt);

    bool ConvertTopFunction(SelectStmt* stmt, std::string* err);

    void SetGroupBy(SelectStmt* stmt, const ColumnPath& column);

    void SetOrderBy(SelectStmt* stmt, const ColumnPath& column, const OrderType& type);

    void SetLimit(SelectStmt* stmt, int32_t start, int32_t length);
private:
    static const char* kTopFunctionName;
    static const char* kCountFunctionName;
    RawAtomicExpression* m_atomic_top_expr;
    RawAtomicExpression* m_atomic_count_expr;
    RawTarget* m_count_target;
};

}  // namespace compiler
}  // namespace gunir


#endif  // GUNIR_COMPILER_QUERY_CONVERTOR_H

