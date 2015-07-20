// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_OPERATOR_EXPRESSION_H
#define  GUNIR_COMPILER_OPERATOR_EXPRESSION_H

#include <string>
#include <vector>

#include "toft/base/shared_ptr.h"

#include "gunir/compiler/expression.h"
#include "gunir/compiler/operator_functions.h"
#include "gunir/compiler/parser/big_query_types.h"
#include "gunir/compiler/parser/select_stmt.pb.h"

namespace gunir {
namespace compiler {

class OperatorExpression : public Expression {
public:
    OperatorExpression() : m_op_fn(NULL), m_is_distinct(false)  {}

    explicit OperatorExpression(const ExpressionProto& proto,
                                ExpressionInitializer* initializer) {
        ParseFromProto(proto, initializer);
    }
    ~OperatorExpression() {}

    virtual void CopyToProto(ExpressionProto* proto) const;

    virtual void ParseFromProto(const ExpressionProto& proto,
                                ExpressionInitializer* initializer);

    bool Evaluate(const std::vector<DatumBlock*>& datum_block_row,
                  DatumBlock* datum_block) const;

    std::shared_ptr<DatumBlock> GetOperand(
        const std::vector<DatumBlock*>& datum_block_row,
        const std::shared_ptr<Expression>& expr,
        BQFunctionInfo* info,
        int index) const;

    virtual bool GetDeepestColumn(DeepestColumnInfo* info) const;

    virtual BQType GetReturnType() const {
        return m_op_fn->m_return_type;
    }

    void SetOperator(Operators op, const OperatorFunctionInfo* fn);

    virtual void GetAffectedColumns(
        std::vector<AffectedColumnInfo>* affected_columns) const;
    virtual bool HasAggregate() const;

    virtual bool IsSingleColumn() const { return false; }

    void SetLeft(std::shared_ptr<Expression> expr) { m_left = expr; }

    void SetRight(std::shared_ptr<Expression> expr) { m_right = expr; }

    void SetDistinct(bool is_distinct) { m_is_distinct = is_distinct; }

    virtual bool IsDistinct() const { return m_is_distinct; }

private:
    std::shared_ptr<Expression> m_left;
    std::shared_ptr<Expression> m_right;

    Operators m_op;
    const OperatorFunctionInfo* m_op_fn;
    bool m_is_distinct;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_OPERATOR_EXPRESSION_H

