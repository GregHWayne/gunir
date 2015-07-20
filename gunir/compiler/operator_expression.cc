// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/operator_expression.h"

#include "toft/base/singleton.h"

#include "gunir/compiler/expression_initializer.h"
#include "gunir/compiler/function_resolver.h"
#include "gunir/compiler/parser/plan.pb.h"

namespace gunir {
namespace compiler {

void OperatorExpression::CopyToProto(ExpressionProto* expr_proto) const {
    expr_proto->set_type(ExpressionProto::kOperator);

    OperatorExpressionProto* proto = expr_proto->mutable_operator_();
    proto->set_return_type(
        BigQueryType::ConvertBQTypeToPBProtoType(m_op_fn->m_return_type));

    if (m_left)
        m_left->CopyToProto(proto->mutable_left());
    if (m_right)
        m_right->CopyToProto(proto->mutable_right());

    proto->set_op(m_op);
}

void OperatorExpression::ParseFromProto(const ExpressionProto& proto,
                                        ExpressionInitializer* initializer) {
    CHECK(proto.type() == ExpressionProto::kOperator)
        << "OperatorExpression can't parse from proto:" << proto.DebugString();

    std::vector<BQType> type_list;
    std::vector<BQType> cast_info;
    const OperatorExpressionProto& op_proto = proto.operator_();

    m_left = initializer->InitExpressionFromProto(op_proto.left());

    type_list.push_back(m_left->GetReturnType());
    if (op_proto.has_right()) {
        m_right = initializer->InitExpressionFromProto(op_proto.right());
        type_list.push_back(m_right->GetReturnType());
    }

    m_op = op_proto.op();
    m_op_fn = toft::Singleton<FunctionResolver>::Instance()->
        ResolveOperatorFunction(m_op, type_list, &cast_info);

    CHECK(m_op_fn != NULL) << "Can't resolve operator function for proto:"
        << proto.DebugString();

    BQType proto_return_type =
        BigQueryType::ConvertPBProtoTypeToBQType(op_proto.return_type());

    CHECK(m_op_fn->m_return_type == proto_return_type)
        << "Can't resolve operator function for proto:"
        << proto.DebugString();
}

bool OperatorExpression::Evaluate(
    const std::vector<DatumBlock*>& datum_block_row,
    DatumBlock* datum_block) const {

    CHECK(m_left != NULL) << "Left operator must not be null";

    BQFunctionInfo info;
    std::shared_ptr<DatumBlock> left_datum_block;
    std::shared_ptr<DatumBlock> right_datum_block;

    if (NULL == (left_datum_block =
                 GetOperand(datum_block_row, m_left, &info, 0))) {
        return false;
    }

    if (m_right != NULL) {
        if (NULL == (right_datum_block =
                     GetOperand(datum_block_row, m_right, &info, 1))) {
            return false;
        }
    }

    info.return_datum = &datum_block->m_datum;

    (*m_op_fn->m_fn_addr)(&info);

    datum_block->m_is_null = info.is_null;

    return true;
}

std::shared_ptr<DatumBlock> OperatorExpression::GetOperand(
    const std::vector<DatumBlock*>& datum_block_row,
    const std::shared_ptr<Expression>& expr,
    BQFunctionInfo* info,
    int index) const {

    DatumBlock* block = new DatumBlock(expr->GetReturnType());

    if (!expr->Evaluate(datum_block_row, block)) {
        delete block;
        return std::shared_ptr<DatumBlock>(static_cast<DatumBlock*>(NULL));
    }

    info->arg[index] = block->m_datum;
    info->is_arg_null[index] = block->m_is_null;
    info->arg_type[index] = expr->GetReturnType();
    return std::shared_ptr<DatumBlock>(block);
}

void OperatorExpression::SetOperator(
    Operators op, const OperatorFunctionInfo* fn) {
    m_op = op;
    m_op_fn = fn;
}

bool OperatorExpression::HasAggregate() const {
    bool left_has_agg = false;
    bool right_has_agg = false;

    if (m_left != NULL) {
        left_has_agg = m_left->HasAggregate();
    }

    if (!left_has_agg && m_right != NULL) {
        right_has_agg = m_right->HasAggregate();
    }

    return (left_has_agg || right_has_agg);
}

bool OperatorExpression::GetDeepestColumn(DeepestColumnInfo* info) const {
    DeepestColumnInfo left_deepest;
    DeepestColumnInfo right_deepest;
    bool left_has_column = false;
    bool right_has_column = false;

    if (m_left != NULL) {
        left_has_column = m_left->GetDeepestColumn(&left_deepest);
    }

    if (m_right != NULL) {
        right_has_column = m_right->GetDeepestColumn(&right_deepest);
    }

    // both expr do not have column
    if (!left_has_column && !right_has_column) {
        return false;
    }

    // one expr has a column
    if (left_has_column && !right_has_column) {
        *info = left_deepest;
        return true;
    }

    if (!left_has_column && right_has_column) {
        *info = right_deepest;
        return true;
    }

    // both expr have column
    if (left_deepest.m_repeat_level > right_deepest.m_repeat_level) {
        *info = left_deepest;
    } else {
        *info = right_deepest;
    }
    return true;
}

void OperatorExpression::GetAffectedColumns(
    std::vector<AffectedColumnInfo>* affected_columns) const {
    if (m_left != NULL) {
        m_left->GetAffectedColumns(affected_columns);
    }
    if (m_right != NULL) {
        m_right->GetAffectedColumns(affected_columns);
    }
}
} // namespace compiler
} // namespace gunir

