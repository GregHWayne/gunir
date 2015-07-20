// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_TARGET_H
#define  GUNIR_COMPILER_TARGET_H

#include <climits>
#include <string>
#include <vector>

#include "toft/base/shared_ptr.h"

#include "gunir/compiler/aggregate_function_expression.h"
#include "gunir/compiler/expression.h"
#include "gunir/compiler/parser/big_query_types.h"

namespace gunir {
namespace compiler {

class Target {
private:
    typedef std::shared_ptr<AggregateFunctionExpression> ShrAggExpr;

public:
    Target() : m_has_agg(false), m_within_level(UINT_MAX) {}

    BQType GetTargetType() { return m_expr->GetReturnType(); }

    bool HasAggregate() { return m_has_agg; }
    bool IsCompatibleWithGroupBy(
        const std::vector<GroupByColumn>& groupby_columns) const;

    void SetExpression(const std::shared_ptr<Expression>& e) {
        m_expr = e;
        m_has_agg = e->HasAggregate();
    }
    const std::shared_ptr<Expression>& GetExpression() const {
        return m_expr;
    }

    void SetAlias(const std::string& alias) {
        m_alias.reset(new std::string(alias));
    }

    const std::shared_ptr<std::string> GetAlias() {
        return m_alias;
    }

    bool GetResultPath(std::vector<std::string>* column_path, bool has_groupby) {
        const std::shared_ptr<Expression>& e = this->GetExpression();
        if (!has_groupby || e->IsSingleColumn()) {
            DeepestColumnInfo deepest_column_info;
            // target is const expression
            if (!e->GetDeepestColumn(&deepest_column_info)) {
                return false;
            }

            *column_path = *(deepest_column_info.m_column_path);
            if (this->GetAlias() != NULL) {
                *(column_path->rbegin()) = *(this->GetAlias());
            }
        } else {
            column_path->clear();
            column_path->push_back(*(this->GetAlias()));
        }
        return true;
    }

    bool IsSingleColumn() const { return m_expr->IsSingleColumn(); }
    void SetResultColumnInfo(const ColumnInfo& column_info) {
        m_result_column_info = column_info;
    }
    const ColumnInfo& GetResultColumnInfo() const {
        return m_result_column_info;
    }

    void SetWithinLevel(uint32_t level) { m_within_level = level; }
    uint32_t GetWithinLevel() const { return m_within_level; }
    bool HasWithin() const { return m_within_level != UINT_MAX; }

    void SetWithinMessagePath(const std::vector<std::string>& within_path) {
        m_within_message_path = within_path;
    }
    const std::vector<std::string>& GetWithinMessagePath() const {
        return m_within_message_path;
    }

    void SetWithinTableName(const std::string& name) {
        m_within_table_name = name;
    }

    const std::string& GetWithinTableName() const {
        return m_within_table_name;
    }

    void SetAggregateFunctionExpression(
        const std::vector<ShrAggExpr>& agg_exprs) {
        m_agg_exprs = agg_exprs;
    }

    const std::vector<ShrAggExpr>& GetAggregateFunctionExpression() const {
        return m_agg_exprs;
    }

    void GetAggAndNotAggColumn(std::vector<AffectedColumnInfo>* in_agg,
                               std::vector<AffectedColumnInfo>* not_in_agg);

private:
    bool IsInGroupByColumns(
        const AffectedColumnInfo& column,
        const std::vector<GroupByColumn>& groupby_columns) const;

    bool IsPathSame(const std::vector<std::string>& left,
                    const std::vector<std::string>& right) const {
        if (left.size() != right.size() || left.size() <= 0) {
            return false;
        }
        for (size_t i = 0 ; i < left.size() ; ++i) {
            if (left[i] != right[i]) {
                return false;
            }
        }
        return true;
    }

    void AddNotInAggregate(
        const AffectedColumnInfo& column_in_all,
        const std::vector<AffectedColumnInfo>& in_agg,
        std::vector<AffectedColumnInfo>* not_in_agg) const;

private:
    bool m_has_agg;
    uint32_t m_within_level;

    ColumnInfo m_result_column_info;
    std::shared_ptr<Expression> m_expr;
    std::shared_ptr<std::string> m_alias;
    std::vector<ShrAggExpr> m_agg_exprs;
    std::vector<std::string> m_within_message_path;
    std::string m_within_table_name;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_TARGET_H

