// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_TABLE_MANAGER_H
#define  GUNIR_COMPILER_TABLE_MANAGER_H

#include <map>
#include <string>
#include <vector>

#include "toft/base/shared_ptr.h"

#include "gunir/proto/table.pb.h"
#include "gunir/compiler/parser/big_query_types.h"
#include "gunir/compiler/parser/column_info.h"
#include "gunir/compiler/parser/table_schema.h"

#include "thirdparty/glog/logging.h"

namespace gunir {
namespace compiler {

class Table {
private:
    typedef ::google::protobuf::FileDescriptor PBFileDescriptor;
    typedef ::google::protobuf::FileDescriptorProto PBFileDescriptorProto;

public:
    Table() {}
    ~Table() {}

    bool Init(const TableInfo& table_info) {
        PBFileDescriptorProto file_desc_proto;
        const std::string& table_name = table_info.table_name();
        const std::string& msg_name = table_info.message_name();
        const std::string schema_string = table_info.table_schema();

        m_schema.reset(new TableSchema());

        m_table_size = table_info.table_size();

        if (!m_schema->InitSchemaFromFileDescriptorProto(
                schema_string, msg_name)) {
            return false;
        }

        m_table_name = table_name;

        for (int i = 0; i < table_info.tablets_size(); ++i) {
            m_tablets.push_back(table_info.tablets(i));
        }

        return true;
    }

    inline int64_t GetTableSize() {
        return m_table_size;
    }

    bool GetRequiredFieldPath(std::vector<std::string>* path_list) {
        return m_schema->GetRequiredFieldPath(path_list);
    }

    void GetAllColumnInfo(std::vector<ColumnInfo>* column_infos) const {
        return m_schema->GetAllColumnInfo(column_infos);
    }

    bool GetColumnInfo(const std::vector<std::string>& path,
                       ColumnInfo* info) {
        std::string table_name;
        return GetColumnInfo(path, info, &table_name);
    }

    std::string GetEntryName() const {
        return m_schema->GetEntryName();
    }

    bool GetColumnInfo(const std::vector<std::string>& path,
                       ColumnInfo* info,
                       std::string* table_name) {
        bool is_succeed = m_schema->GetColumnInfo(path, info);

        if (is_succeed) {
            *table_name = m_table_name;
        }
        return is_succeed;
    }

    const TableSchema& GetTableSchema() const { return *m_schema.get(); }

    const std::vector<TabletInfo>& GetTabletFile() const {
        return m_tablets;
    }

private:
    std::string m_table_name;
    int64_t m_table_size;
    toft::scoped_ptr<TableSchema> m_schema;
    std::vector<TabletInfo> m_tablets;
};

class TableManager {
private:
    typedef std::shared_ptr<Table> TablePtr;

public:
    bool Init(const std::vector<TableInfo>& table_infos,
              std::string* failed_table_name) {
        for (size_t i = 0; i < table_infos.size(); ++i) {
            const TableInfo& table_info = table_infos[i];
            TablePtr t(new Table());

            bool is_succeed = t->Init(table_info);
            if (!is_succeed) {
                *failed_table_name = table_info.table_name();
                return false;
            }
            m_tables[table_info.table_name()] = t;
        }
        return true;
    }

    TablePtr GetTable(const std::string& table_name) {
        std::map<std::string, TablePtr>::iterator iter;

        iter = m_tables.find(table_name);
        if (iter != m_tables.end()) {
            return iter->second;
        }

        return TablePtr(static_cast<Table*>(NULL));
    }

private:
    std::map<std::string, TablePtr> m_tables;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_TABLE_MANAGER_H
