// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_EXPRESSION_H
#define  GUNIR_COMPILER_EXPRESSION_H

#include <string>
#include <vector>

// #include "toft/base/shared_ptr.h"

#include "gunir/compiler/aggregate_functions.h"
#include "gunir/compiler/operator_functions.h"
#include "gunir/compiler/parser/big_query_types.h"
#include "gunir/compiler/parser/column_info.h"

namespace gunir {
namespace compiler {

class ExpressionProto;
class ExpressionInitializer;

class Expression {
public:
    Expression() {}
    virtual ~Expression() {}

    virtual bool Evaluate(
        const std::vector<DatumBlock*>& datum_row, DatumBlock* datum) const = 0;

    virtual bool GetDeepestColumn(DeepestColumnInfo* info) const = 0;

    virtual void GetAffectedColumns(
        std::vector<AffectedColumnInfo>* affected_columns) const = 0;

    virtual BQType GetReturnType() const = 0;

    virtual bool IsSingleColumn() const = 0;

    virtual bool HasAggregate() const = 0;

    virtual void CopyToProto(ExpressionProto* proto) const = 0;

    virtual void ParseFromProto(const ExpressionProto& proto,
                                ExpressionInitializer* initializer) = 0;
    virtual bool IsDistinct() const = 0;
};

class ColumnExpression : public Expression {
public:
    ColumnExpression(const ColumnInfo& column_info,
                     const std::string& table_name,
                     size_t affect_id,
                     bool is_distinct) {
        SetColumnInfo(column_info);
        m_table_name = table_name;
        m_affect_id = affect_id;
        m_is_distinct = is_distinct;
    }

    explicit ColumnExpression(const ExpressionProto& proto) {
        ParseFromProto(proto, NULL);
    }

    ~ColumnExpression() {}

    virtual void CopyToProto(ExpressionProto* expr_proto) const;
    virtual void ParseFromProto(const ExpressionProto& proto,
                                ExpressionInitializer* initializer);

    virtual bool Evaluate(
        const std::vector<DatumBlock*>& datum_row,
        DatumBlock* datum_block) const {
        *datum_block = *datum_row[m_affect_id];
        return true;
    }

    virtual bool GetDeepestColumn(DeepestColumnInfo* info) const {
        info->m_repeat_level = m_column_info.m_repeat_level;
        info->m_label = m_column_info.m_label;
        info->m_column_path = &m_column_info.m_column_path;
        info->m_table_name = &m_table_name;
        info->m_affect_id = m_affect_id;
        return true;
    }

    virtual void GetAffectedColumns(
        std::vector<AffectedColumnInfo>* affected_columns) const {

        for (size_t i = 0; i < affected_columns->size(); ++i) {
            if ((*affected_columns)[i].m_column_info.m_column_path_string ==
                m_column_info.m_column_path_string) {
                (*affected_columns)[i].m_affect_times++;
                return;
            }
        }

        AffectedColumnInfo info(m_column_info, m_affect_id, m_is_distinct);
        affected_columns->push_back(info);
    }

    virtual BQType GetReturnType() const { return m_column_info.m_type; }

    virtual bool HasAggregate() const { return false; }

    virtual bool IsSingleColumn() const { return true; }

    void SetColumnInfo(const ColumnInfo& info) { m_column_info = info; }
    const ColumnInfo& GetColumnInfo() const { return m_column_info; }
    virtual bool IsDistinct() const { return m_is_distinct; }

private:
    ColumnInfo m_column_info;

    std::string m_table_name;
    size_t m_affect_id;
    bool m_is_distinct;
};

class ConstExpression : public Expression {
public:
    explicit ConstExpression(DatumBlock block) : m_datum_block(block) {}

    explicit ConstExpression(const ExpressionProto& proto);

    ~ConstExpression() {}

    virtual void CopyToProto(ExpressionProto* expr_proto) const;

    virtual void ParseFromProto(const ExpressionProto& proto,
                                ExpressionInitializer* initializer);

    bool Evaluate(
        const std::vector<DatumBlock*>& datum_row,
        DatumBlock* datum_block) const {

        *datum_block = m_datum_block;
        return true;
    }

    virtual void GetAffectedColumns(
        std::vector<AffectedColumnInfo>* affected_columns) const {
    }

    virtual bool GetDeepestColumn(DeepestColumnInfo* info) const {
        return false;
    }

    virtual BQType GetReturnType() const { return m_datum_block.GetType(); }

    virtual bool HasAggregate() const { return false; }

    virtual bool IsSingleColumn() const { return false; }

    virtual bool IsDistinct() const { return false; }

private:
    DatumBlock m_datum_block;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_EXPRESSION_H

