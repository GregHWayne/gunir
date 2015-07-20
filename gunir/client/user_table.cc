// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/client/user_table.h"

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/algorithm.h"
#include "toft/storage/file/file.h"
#include "toft/storage/path/path_ext.h"

#include "gunir/io/tablet_reader.h"
#include "gunir/utils/filename_tool.h"

namespace gunir {

UserTable::UserTable(const std::string& file_name)
    : m_file_name(file_name) {
    LoadFromConfigure();
}

UserTable::~UserTable() {
    DumpToConfigure();
}

void UserTable::LoadFromConfigure() {
    if (!IsExist(m_file_name)) {
        LOG(INFO) << "configure file is not exist: "
            << m_file_name;
        return;
    }

    toft::scoped_ptr<toft::File> file(File::Open(m_file_name, "r"));
    if (NULL == file.get()) {
        LOG(ERROR) << "Open configure file error";
        return;
    }

    LOG(INFO) <<"Load configure From FileSystem";

    std::string line = "";
    while (file->ReadLine(&line) > 0) {
        std::vector<std::string> split_line;
        SplitString(line, ":",  &split_line);
        if (split_line.size() == 2) {
            m_config_info[split_line[0]] = StringTrim(split_line[1], "\n");
        }
    }

    file->Close();
}

void UserTable::DumpToConfigure() {
    toft::scoped_ptr<toft::File> file(File::Open(m_file_name, "w"));
    if (NULL == file.get()) {
        LOG(ERROR) << "Dump user table info to file system error ";
        return;
    }

    LOG(INFO) <<"Dump user table info To FileSystem";
    std::map<std::string, std::string>::iterator it;
    for (it = m_config_info.begin(); it != m_config_info.end(); it++) {
        std::string line = it->first + ":" + it->second + "\n";
        if (file->Write(line.data(), line.size()) < 0) {
            LOG(ERROR) << "Dump user table info to filesystem error :"
                << it->second;
        }
    }

    file->Close();
}

bool UserTable::AddTable(const std::string& table_name,
                         const std::string& path) {
    std::map<std::string, std::string>::iterator it
        = m_config_info.find(table_name);
    if (it != m_config_info.end()) {
        LOG(ERROR) << "Table [" << table_name << "] is already exist";
        return false;
    }

    TableInfo table_info;
    if (!InitTableInfo(table_name, path, &table_info)) {
        return false;
    }

    m_config_info[table_name] = path;
    m_table_info[table_name] = table_info;
    return true;
}

bool UserTable::QueryTable(const std::vector<std::string>& table_names,
                           std::vector<TableInfo>* table_info) {
    for (size_t i = 0; i < table_names.size(); ++i) {
        const std::string& table_name = table_names[i];
        // check if the table is exist
        std::map<std::string, std::string>::iterator it
            = m_config_info.find(table_name);
        if (it == m_config_info.end()) {
            continue;
        }
        TableInfo table;
        if (!FillTableInfo(it->first, it->second, &table)) {
            return false;
        }
        table_info->push_back(table);
    }
    return true;
}

bool UserTable::QueryAllTable(std::vector<TableInfo>* table_info) {
    std::map<std::string, std::string>::iterator it;
    for (it = m_config_info.begin(); it != m_config_info.end(); ++it) {
        TableInfo table;
        if (!FillTableInfo(it->first, it->second, &table)) {
            return false;
        }
        table_info->push_back(table);
    }
    return true;
}

bool UserTable::QueryTableByName(const std::string& table_name,
                                 TableInfo* table_info) {
    std::map<std::string, std::string>::iterator it
        = m_config_info.find(table_name);
    if (it == m_config_info.end()) {
        return false;
    }
    if (!FillTableInfo(it->first, it->second, table_info)) {
        return false;
    }
    return true;
}

bool UserTable::FillTableInfo(const std::string& table_name,
                              const std::string& path,
                              TableInfo* table_info) {
    // get table info
    std::map<std::string, TableInfo>::iterator iter
        = m_table_info.find(table_name);
    // if table info is not ready, init table info
    if (iter == m_table_info.end()) {
        if (!InitTableInfo(table_name, path, table_info)) {
            return false;
        }
        m_table_info[table_name] = *table_info;
    } else {
        *table_info = iter->second;
    }

    return true;
}

bool UserTable::InitTableInfo(const std::string& table_name,
                              const std::string& path,
                              TableInfo* table_info) {
    std::vector<std::string> output_files;
    int64_t total_size = 0;

    if (!GetFilesByPattern(path.c_str(), &output_files, &total_size)) {
        LOG(ERROR) << "Get tablet [ " << table_name << " ] files ["
            << path << "] error ";
        return false;
    }

    std::string message_name;
    std::string schema;
    if (!GetTableInfoFromTablet(output_files[0], &message_name, &schema)) {
        LOG(ERROR) << "Tablet file's not right, please check input : " << path;
        return false;
    }

    table_info->set_table_name(table_name);
    table_info->set_message_name(message_name);
    table_info->set_table_schema(schema);
    table_info->set_table_size(total_size);
    for (size_t i = 0; i < output_files.size(); ++i) {
        TabletInfo* tablet = table_info->add_tablets();
        tablet->set_name(output_files[i]);
        GetFileChunkIp(output_files[i], tablet);
        LOG(INFO) << "Add tablet file to table " << table_name
            << " : " << tablet->ShortDebugString();
    }

    LOG(INFO) << "Table " << table_name << " size " << total_size;
    return true;
}

bool UserTable::GetTableInfoFromTablet(const std::string& tablet_file,
                                       std::string* message_name,
                                       std::string* table_schema) {
    MemPool mempool(MemPool::MAX_UNIT_SIZE);
    io::TabletReader tablet_reader(&mempool);
    if (!tablet_reader.Init(tablet_file)) {
        LOG(ERROR) << "Open tablet file " << tablet_file << " error";
        return false;
    }

    io::TabletSchema schema;
    tablet_reader.GetTabletSchema(&schema);
    *message_name = schema.schema_descriptor().record_name();
    *table_schema = schema.schema_descriptor().description();

    return true;
}

}  // namespace gunir
