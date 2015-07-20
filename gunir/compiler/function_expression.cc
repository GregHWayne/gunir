// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/function_expression.h"

#include "toft/base/singleton.h"

#include "gunir/compiler/expression_initializer.h"
#include "gunir/compiler/function_resolver.h"
#include "gunir/compiler/parser/plan.pb.h"

namespace gunir {
namespace compiler {

void FunctionExpression::CopyToProto(ExpressionProto* proto) const {
    FunctionExpressionProto* fn_proto = proto->mutable_function();
    std::string fn_name;
    BQType return_type = BigQueryType::BYTES;

    if (m_fn_type == kAgg) {
        proto->set_type(ExpressionProto::kAggFunction);
    } else {
        proto->set_type(ExpressionProto::kFunction);
    }

    if (m_fn_type == kAgg) {
        fn_name = m_agg_fn->m_fn_name;
        return_type = m_agg_fn->m_final_type;

    } else if (m_fn_type == kCast) {
        fn_name = m_cast_fn->m_fn_name;
        return_type = m_cast_fn->m_return_type;

    } else if (m_fn_type == kPlain) {
        fn_name = m_plain_fn->m_fn_name;
        return_type = m_plain_fn->m_return_type;
    }

    fn_proto->set_fn_name(fn_name);
    fn_proto->set_return_type(
        BigQueryType::ConvertBQTypeToPBProtoType(return_type));
    fn_proto->set_is_distinct(m_is_distinct);

    for (size_t i = 0; i < m_expr_list.size(); i++) {
        ExpressionProto* subexpr_proto = (fn_proto->mutable_expr_list())->Add();
        m_expr_list[i]->CopyToProto(subexpr_proto);
    }
}

void FunctionExpression::ParseFromProto(const ExpressionProto& proto,
                                        ExpressionInitializer* initializer) {
    CHECK(proto.type() == ExpressionProto::kFunction ||
          proto.type() == ExpressionProto::kAggFunction)
        << "FunctionExpression can't parse from proto:" << proto.DebugString();

    const FunctionExpressionProto& fn_proto = proto.function();
    std::vector<BQType> type_list;
    std::vector<BQType> cast_info;
    for (int i = 0; i < fn_proto.expr_list_size(); ++i) {
        const ExpressionProto& expr_proto = fn_proto.expr_list(i);
        m_expr_list.push_back(initializer->InitExpressionFromProto(expr_proto));
        type_list.push_back(m_expr_list[i]->GetReturnType());
    }

    FunctionResolver* resolver = toft::Singleton<FunctionResolver>::Instance();
    std::string fn_name = fn_proto.fn_name();
    BQType return_type = BigQueryType::BYTES;
    BQType proto_return_type =
        BigQueryType::ConvertPBProtoTypeToBQType(fn_proto.return_type());

    m_is_distinct = fn_proto.is_distinct();

    do {
        m_plain_fn = resolver->ResolvePlainFunction(
            fn_name, type_list, &cast_info);

        if (m_plain_fn != NULL) {
            m_fn_type = kPlain;
            return_type = m_plain_fn->m_return_type;
            break;
        }

        CHECK_EQ(1, type_list.size())
            << "Can't parse function expression from proto:"
            << proto.DebugString();

        m_agg_fn = resolver->ResolveAggregateFunction(fn_name,
                                                     type_list[0],
                                                     &cast_info);
        if (m_agg_fn != NULL) {
            m_fn_type = kAgg;
            return_type = m_agg_fn->m_final_type;
            break;
        }

        m_cast_fn = resolver->ResolveCastFunction(type_list[0],
                                                 proto_return_type);

        if (m_cast_fn != NULL) {
            m_fn_type = kCast;
            return_type = m_cast_fn->m_return_type;
            break;
        }

        LOG(FATAL) << "Can't parse function expression from proto:"
            << proto.DebugString();
    } while (0);

    CHECK(return_type == proto_return_type)
        << "Can't parse function expression from proto:"
        << proto.DebugString();
}

bool FunctionExpression::Evaluate(
    const std::vector<DatumBlock*>& datum_block_row,
    DatumBlock* datum_block) const {
    BQFunctionInfo function_info;
    std::vector<std::shared_ptr<DatumBlock> > arg_values;

    for (size_t i = 0; i < m_expr_list.size(); i++) {
        std::shared_ptr<DatumBlock> block =
            GetOperand(m_expr_list[i], datum_block_row, &function_info, i);
        if (block == NULL)
            return false;
        arg_values.push_back(block);
    }
    function_info.return_datum = &(datum_block->m_datum);

    CHECK(m_fn_type != kAgg) << "Aggregate Function is not supported";

    if (m_fn_type == kPlain) {
        (*m_plain_fn->m_fn_addr)(&function_info);
    }
    if (m_fn_type == kCast) {
        (*m_cast_fn->m_fn_addr)(&function_info);
    }

    datum_block->m_is_null = function_info.is_null;
    return true;
}

std::shared_ptr<DatumBlock> FunctionExpression::GetOperand(
    const std::shared_ptr<Expression>& expr,
    const std::vector<DatumBlock*>& datum_block_row,
    BQFunctionInfo* info,
    size_t index) const {

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

BQType FunctionExpression::GetReturnType() const {
    if (m_fn_type == kAgg) {
        return m_agg_fn->m_final_type;
    } else if (m_fn_type == kPlain) {
        return m_plain_fn->m_return_type;
    } else {
        return m_cast_fn->m_return_type;
    }
}

bool FunctionExpression::GetDeepestColumn(DeepestColumnInfo* info) const {
    uint32_t max_repeat_level = INT_MAX;
    std::vector<std::shared_ptr<Expression> >::const_iterator iter;
    bool expr_has_column = false;

    for (iter = m_expr_list.begin(); iter != m_expr_list.end(); ++iter) {
        DeepestColumnInfo temp_deepest_column_info;
        bool has_column = (*iter)->GetDeepestColumn(&temp_deepest_column_info);

        if (!has_column) {
            continue;
        }

        if (max_repeat_level == INT_MAX ||
            temp_deepest_column_info.m_repeat_level > max_repeat_level) {

            max_repeat_level = temp_deepest_column_info.m_repeat_level;
            *info = temp_deepest_column_info;
            expr_has_column = true;
        }
    }

    return expr_has_column;
}

bool FunctionExpression::HasAggregate() const {
    if (m_fn_type == kAgg) {
        return true;
    }

    for (size_t i = 0; i < m_expr_list.size(); ++i) {
        if (m_expr_list[i]->HasAggregate()) {
            return true;
        }
    }

    return false;
}

void FunctionExpression::GetAffectedColumns(
    std::vector<AffectedColumnInfo>* affected_columns) const {
    for (size_t i = 0; i < m_expr_list.size(); ++i) {
        m_expr_list[i]->GetAffectedColumns(affected_columns);
    }
}

} // namespace compiler
} // namespace gunir

