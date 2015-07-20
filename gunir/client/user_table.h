// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_COMMON_BASE_USER_TABLE_H
#define  GUNIR_COMMON_BASE_USER_TABLE_H

#include <map>
#include <string>
#include <vector>

#include "gunir/proto/table.pb.h"

namespace gunir {

class UserTable {
public:
    explicit UserTable(const std::string& file_name);
    ~UserTable();

    bool AddTable(const std::string& table_name,
                  const std::string& path);

    bool QueryTable(const std::vector<std::string>& table_names,
                    std::vector<TableInfo>* table_info);

    bool QueryAllTable(std::vector<TableInfo>* table_info);

    bool QueryTableByName(const std::string& table_name,
                          TableInfo* table_info);

private:
    void LoadFromConfigure();
    void DumpToConfigure();
    bool FillTableInfo(const std::string& table_name,
                       const std::string& path,
                       TableInfo* table_info);

    bool InitTableInfo(const std::string& table_name,
                       const std::string& path,
                       TableInfo* table_info);

    bool GetTableInfoFromTablet(const std::string& tablet_file,
                                std::string* message_name,
                                std::string* table_schema);

private:
    std::string m_file_name;
    std::map<std::string, TableInfo> m_table_info;
    std::map<std::string, std::string> m_config_info;
};

}  // namespace gunir

#endif  // GUNIR_COMMON_BASE_USER_TABLE_H
