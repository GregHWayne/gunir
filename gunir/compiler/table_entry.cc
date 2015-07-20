// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/table_entry.h"

namespace gunir {
namespace compiler {

bool TableEntry::InitTableEntry(TableManager* table_mgr,
                                const RawTable& raw_table,
                                std::string* err_string) {

    if (raw_table.has_select_stmt()) {
        *err_string = "Nested Select Stmt does not supported yet";
        LOG(ERROR) << *err_string;
        return false;
    }

    if (!raw_table.has_table_name()) {
        *err_string = "TableEntry is neither subselect nor tablename";
        LOG(ERROR) << *err_string;
        return false;
    }

    m_table_name = raw_table.table_name().char_string();
    m_table = table_mgr->GetTable(m_table_name);

    if (m_table == NULL) {
        *err_string = "Table:" + m_table_name + " does not exist";
        LOG(ERROR) << *err_string;
        return false;
    }

    if (raw_table.has_alias()) {
        m_alias.reset(new std::string(raw_table.alias().char_string()));
    }

    m_entry_name.reset(new std::string(m_table->GetEntryName()));

    return true;
}

bool TableEntry::GetAffectColumnInfo(std::vector<std::string> path,
                                     ColumnInfo* info) {
    CHECK(m_table != NULL) << "TableEntry is used before initialized";
    CHECK(!path.empty()) << "GetColumnInfo for path is empty";

    if (*(path.begin()) == m_table_name) {
        path.erase(path.begin());
    } else if (m_alias != NULL && *(path.begin()) == *m_alias) {
        path.erase(path.begin());
    } else if (*(path.begin()) == *m_entry_name) {
        path.erase(path.begin());
    }

    for (size_t i = 0; i < m_affected_column_info.size(); i++) {
        const ColumnInfo& affect_info =
            m_affected_column_info[i].m_column_info;

        if (path == affect_info.m_column_path) {
            *info = affect_info;
            return  true;
        }
    }

    return false;
}

bool TableEntry::GetColumnInfo(std::vector<std::string> path,
                               ColumnInfo* info) {
    CHECK(m_table != NULL) << "TableEntry is used before initialized";
    CHECK(!path.empty()) << "GetColumnInfo for path is empty";

    // prefix with table name
    if (*(path.begin()) == m_table_name) {
        path.erase(path.begin());
        return m_table->GetColumnInfo(path, info);
    }

    // prefix with alias
    if (m_alias != NULL && *(path.begin()) == *m_alias) {
        path.erase(path.begin());
        return m_table->GetColumnInfo(path, info);
    }

    return m_table->GetColumnInfo(path, info);
}

void TableEntry::AddAffectedColumnInfo(
    const ColumnInfo& info, size_t affect_id, bool has_distinct) {

    for (size_t i = 0; i < m_affected_column_info.size(); i++) {
        const std::string& column_path_string =
            m_affected_column_info[i].m_column_info.m_column_path_string;

        if (column_path_string == info.m_column_path_string) {
            m_affected_column_info[i].m_affect_times++;
            return;
        }
    }
    m_affected_column_info.push_back(
        AffectedColumnInfo(info, affect_id, m_table_name, has_distinct));
}

} // namespace compiler
} // namespace gunir

