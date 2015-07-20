// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_PARSER_COLUMN_INFO_H
#define  GUNIR_COMPILER_PARSER_COLUMN_INFO_H

#include <string>
#include <vector>

#include "gunir/compiler/parser/big_query_types.h"
#include "gunir/compiler/parser/select_stmt.pb.h"

namespace gunir {
namespace compiler {

struct ColumnInfo {
    PBLabel m_label;
    BQType m_type;
    int m_column_number;
    uint32_t m_repeat_level;
    uint32_t m_define_level;
    std::string m_column_path_string;

    std::vector<std::string> m_column_path;
};

struct DeepestColumnInfo {
    uint32_t m_repeat_level;
    uint32_t m_define_level;
    int m_affect_id;
    PBLabel m_label;
    const std::string* m_table_name;
    const std::vector<std::string>* m_column_path;
};

struct GroupByColumn {
    ColumnInfo m_column_info;
    size_t m_affect_id;
};

struct DistinctColumn {
    ColumnInfo m_column_info;
    size_t m_affect_id;
};

struct OrderByColumn {
    ColumnInfo m_column_info;
    OrderType m_type;
    size_t m_target_id;

    explicit OrderByColumn(const GroupByColumn& column)
        : m_column_info(column.m_column_info),
          m_type(kAsc),
          m_target_id(column.m_affect_id) {
    }

    explicit OrderByColumn(const DistinctColumn& column)
        : m_column_info(column.m_column_info),
        m_type(kAsc),
        m_target_id(column.m_affect_id) {
    }

    OrderByColumn() {
    }
};

struct AffectedColumnInfo {
    ColumnInfo m_column_info;
    size_t m_affect_id;
    int m_affect_times;
    std::string m_table_name;
    bool m_is_distinct;

    AffectedColumnInfo(const ColumnInfo& info,
                       size_t affect_id,
                       std::string table_name,
                       bool is_distinct)
        : m_column_info(info),
          m_affect_id(affect_id),
          m_affect_times(1),
          m_table_name(table_name),
          m_is_distinct(is_distinct) {
    }

    AffectedColumnInfo(const ColumnInfo& info,
                       size_t affect_id,
                       bool is_distinct)
        : m_column_info(info),
        m_affect_id(affect_id),
        m_affect_times(1),
        m_table_name(""),
        m_is_distinct(is_distinct) {
    }
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_PARSER_COLUMN_INFO_H
