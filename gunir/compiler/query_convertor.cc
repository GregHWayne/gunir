// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "toft/base/string/algorithm.h"
#include "thirdparty/glog/logging.h"

#include "gunir/compiler/query_convertor.h"

namespace gunir {
namespace compiler {

const char* QueryConvertor::kTopFunctionName = "TOP";
const char* QueryConvertor::kCountFunctionName = "COUNT";

bool QueryConvertor::Convert(SelectStmt* stmt, std::string* err) {
    if (CheckHasTopFunction(stmt)
        && !ConvertTopFunction(stmt, err)) {
        return false;
    }
    return true;
}

bool QueryConvertor::CheckHasTopFunction(SelectStmt* stmt) {
    RawTargetList* target_list = stmt->mutable_target_list();
    int cnt = 0;
    m_atomic_count_expr = NULL;
    m_atomic_top_expr = NULL;

    for (int i = 0; i < target_list->target_list_size(); ++i) {
        RawTarget* target = target_list->mutable_target_list(i);
        RawExpression* expression = target->mutable_expression();
        if (expression->has_atomic()) {
            RawAtomicExpression* atomic = expression->mutable_atomic();
            if (atomic->has_function()) {
                const RawFunction& func = atomic->function();
                std::string fn_name = func.function_name().char_string();
                toft::StringToUpper(&fn_name);
                if (fn_name == kTopFunctionName) {
                    m_atomic_top_expr = atomic;
                    cnt++;
                } else if (fn_name == kCountFunctionName) {
                    m_count_target = target;
                    m_atomic_count_expr = atomic;
                }
            }
        }
    }
    return (cnt == 1);
}

bool QueryConvertor::ConvertTopFunction(SelectStmt* stmt, std::string* err) {
    const RawFunction& func_top = m_atomic_top_expr->function();
    const RawArguments& args = func_top.args();
    const int kLegalArgsSize = 2;
    const RawTargetList& target_list = stmt->target_list();
    const char* func_count_alias = "Count";

    ColumnPath column;
    int32_t top_k;
    RawExpression first;
    RawExpression second;

    if (!args.has_arg_list() ||
        args.arg_list().expr_list_size() != kLegalArgsSize) {
        *err += "Top Function args size is illegal";
        LOG(ERROR) << *err;
        return false;
    }

    first = args.arg_list().expr_list(0);
    second = args.arg_list().expr_list(1);

    if (first.has_atomic()
        && second.has_atomic()
        && first.atomic().has_column()
        && second.atomic().has_integer()) {
        column = first.atomic().column();
        top_k = second.atomic().integer();
    } else {
        *err += "Top Function arguments should be (string column_name , int k)";
        LOG(ERROR) << *err;
        return false;
    }

    if (target_list.target_list_size() != 2
        || m_atomic_count_expr == NULL) {
        *err += "Top Function should only be used with count(*)";
        LOG(ERROR) << *err;
        return false;
    }

    const RawFunction& func_count = m_atomic_count_expr->function();
    if (!func_count.args().arg_is_star()) {
        *err += "Top Function should only be used with count(*)";
        LOG(ERROR) << *err;
        return false;
    }

    // recoustruct top(target , top_k) to target
    m_atomic_top_expr->clear_function();
    m_atomic_top_expr->mutable_column()->CopyFrom(column);

    // set default alias
    if (!m_count_target->has_alias()) {
        m_count_target->mutable_alias()->set_char_string(func_count_alias);
    }

    if (stmt->has_groupby()) {
        *err += "Top Function should not be used with groupby";
        LOG(ERROR) << *err;
        return false;
    } else {
        SetGroupBy(stmt, column);
    }

    if (stmt->has_orderby()) {
        *err += "Top Function should not be used with orderby";
        LOG(ERROR) << *err;
        return false;
    } else {
        ColumnPath orderby_colunm;
        orderby_colunm.add_field_list()->set_char_string(
            m_count_target->alias().char_string());
        SetOrderBy(stmt, orderby_colunm, kDesc);
    }

    if (stmt->has_limit()) {
        *err += "Top Function should not be used with limit";
        LOG(ERROR) << *err;
        return false;
    } else {
        SetLimit(stmt, 0, top_k);
    }

    return true;
}

void QueryConvertor::SetGroupBy(SelectStmt* stmt, const ColumnPath& column) {
    stmt->mutable_groupby()->add_path_list()->CopyFrom(column);
}

void QueryConvertor::SetOrderBy(SelectStmt* stmt, const ColumnPath& column,
                                const OrderType& type) {
    OrderColumnPath orderby_column;
    orderby_column.mutable_path()->CopyFrom(column);
    orderby_column.set_type(type);
    stmt->mutable_orderby()->add_path_list()->CopyFrom(orderby_column);
}

void QueryConvertor::SetLimit(SelectStmt* stmt, int32_t start, int32_t number) {
    stmt->mutable_limit()->set_start(start);
    stmt->mutable_limit()->set_number(number);
}

}  // namespace compiler
}  // namespace gunir

