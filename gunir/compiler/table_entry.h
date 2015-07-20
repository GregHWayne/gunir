// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_TABLE_ENTRY_H
#define  GUNIR_COMPILER_TABLE_ENTRY_H

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/shared_ptr.h"

#include "gunir/compiler/table_manager.h"

namespace gunir {
namespace compiler {

class TableEntry {
public:
    TableEntry() {}

    bool InitTableEntry(TableManager* table_mgr, const RawTable& raw_table,
                        std::string* err_string);

    const std::string& GetTableName() const { return m_table_name; }
    bool GetTableAliasName(std::string* alias) const {
        if (m_alias != NULL) {
            *alias = *m_alias;
            return true;
        }
        return false;
    }

    bool GetRequiredFieldPath(std::vector<std::string>* path_list) {
        return m_table->GetRequiredFieldPath(path_list);
    }

    bool GetColumnInfo(std::vector<std::string> path, ColumnInfo* info);
    void GetAllColumnInfo(std::vector<ColumnInfo>* column_infos) const {
        return m_table->GetAllColumnInfo(column_infos);
    }

    const std::vector<AffectedColumnInfo>& GetAffectedColumnInfo() const {
        return m_affected_column_info;
    }

    bool GetAffectColumnInfo(std::vector<std::string> path,
                             ColumnInfo* info);

    void AddAffectedColumnInfo(
        const ColumnInfo& info, size_t affect_id, bool has_distinct);

    inline const std::vector<TabletInfo>& GetTabletFile() const;

private:
    toft::scoped_ptr<std::string> m_alias;
    std::shared_ptr<Table> m_table;
    std::string m_table_name;

    std::vector<AffectedColumnInfo> m_affected_column_info;
    toft::scoped_ptr<std::string> m_entry_name;
};

const std::vector<TabletInfo>& TableEntry::GetTabletFile() const {
    return m_table->GetTabletFile();
}

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_TABLE_ENTRY_H

