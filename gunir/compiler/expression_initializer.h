// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_EXPRESSION_INITIALIZER_H
#define  GUNIR_COMPILER_EXPRESSION_INITIALIZER_H

#include <vector>

#include "toft/base/shared_ptr.h"

#include "gunir/compiler/aggregate_function_expression.h"
#include "gunir/compiler/expression.h"
#include "gunir/compiler/function_expression.h"
#include "gunir/compiler/operator_expression.h"
#include "gunir/compiler/parser/plan.pb.h"

namespace gunir {
namespace compiler {
class ExpressionInitializer {
private:
    typedef std::shared_ptr<Expression> ShrExpr;
    typedef std::shared_ptr<AggregateFunctionExpression> ShrAggExpr;

public:
    ShrExpr InitExpressionFromProto(
        const ExpressionProto& expr_proto) {
        switch (expr_proto.type()) {

        case ExpressionProto::kConst:
            return ShrExpr(new ConstExpression(expr_proto));

        case ExpressionProto::kColumn:
            return ShrExpr(new ColumnExpression(expr_proto));

        case ExpressionProto::kOperator:
            return ShrExpr(new OperatorExpression(expr_proto, this));

        case ExpressionProto::kFunction:
            return ShrExpr(new FunctionExpression(expr_proto, this));

        case ExpressionProto::kAggFunction: {
            ShrAggExpr expr(new AggregateFunctionExpression(expr_proto, this));
            m_aggs.push_back(expr);
            return expr;
        }

        default:
            LOG(FATAL) << "expression can't parse from proto:"
                << expr_proto.DebugString();
            return ShrExpr(static_cast<Expression*>(NULL));
        }
    }

    const std::vector<ShrAggExpr>& GetAggregateFunctionExpression() const {
        return m_aggs;
    }

private:
    std::vector<ShrAggExpr> m_aggs;
};
}  // compiler
}  // gunir

#endif  // GUNIR_COMPILER_EXPRESSION_INITIALIZER_H
