// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "gunir/master/table_manager.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "gunir/proto/proto_helper.h"
#include "gunir/utils/filename_tool.h"

DECLARE_string(gunir_master_meta_dir);
DECLARE_string(gunir_master_meta_table);

DECLARE_int32(gunir_table_thread_min_num);
DECLARE_int32(gunir_table_thread_max_num);

namespace gunir {
namespace master {

TableManager::TableManager()
    : m_meta_db(NULL) {
    CreateDir(FLAGS_gunir_master_meta_dir);
    std::string db_path = FLAGS_gunir_master_meta_dir +
        "/" + FLAGS_gunir_master_meta_table;
    LoadDatabase(db_path, &m_meta_db);
    CHECK_NOTNULL(m_meta_db);

    m_thread_pool.reset(new toft::ThreadPool(FLAGS_gunir_table_thread_min_num,
                                             FLAGS_gunir_table_thread_max_num));
}

TableManager::~TableManager() {}

bool TableManager::AddTable(const AddTableRequest* request,
                            AddTableResponse* response) {
    if (!request->is_created()) {
        return AddTableInfo(request, response);
    }
    return CreateTable(request, response);
}

bool TableManager::CreateTable(const AddTableRequest* request,
                               AddTableResponse* response) {
    const compiler::CreateTableStmt& stmt = request->create();
    const std::string& table_name = stmt.table_name().char_string();

    {
        toft::MutexLocker lock(&m_mutex);
        std::set<std::string>::iterator set_it =
            m_creating_tables.find(table_name);
        if (set_it != m_creating_tables.end()) {
            response->set_status(kMasterOnWork);
            response->set_reason(table_name + " is creating");
            return false;
        } else {
            m_creating_tables.insert(table_name);
        }
    }
    toft::Closure<void ()>* callback =
        toft::NewClosure(this, &TableManager::DoCreateTable, *request);
    m_thread_pool->AddTask(callback);
    return true;
}

void TableManager::DoCreateTable(AddTableRequest request) {
    LOG(INFO) << "DoCreateTable(): " << request.ShortDebugString();

//     const compiler::CreateTableStmt& stmt = request.create();
//     const std::string& table_name = stmt.table_name().char_string();
//     std::string output_path = FLAGS_gunir_table_root_dir
//         + table_name + "/";

//     CreateTableTool create_table(&stmt);
}

bool TableManager::AddTableInfo(const AddTableRequest* request,
                                AddTableResponse* response) {
    bool has_success = false;
    for (int32_t i = 0; i < request->table_infos_size(); ++i) {
        TableStatusCode status = kTableOk;
        if (AddSingleTable(request->table_infos(i), &status)) {
            has_success = true;
        }
        response->add_results(status);
    }
    return has_success;
}

bool TableManager::AddSingleTable(const TableInfo& table_info, TableStatusCode* code) {
    toft::RwLock::WriterLocker locker(&m_rwlock);
    std::string value;
    leveldb::Status status =
        m_meta_db->Get(leveldb::ReadOptions(),
                       table_info.table_name(), &value);
    if (status.ok()) {
        LOG(ERROR) << "already exist: " << table_info.table_name();
        *code = kTableAlreadyExist;
        return false;
    }

    value = "";
    if (!TableInfoPBToString(table_info, &value)) {
        LOG(ERROR) << "fail to serialize table ("
            << table_info.table_name() << ") to string";
        *code = kTableCorrupt;
        return false;
    }

    status = m_meta_db->Put(leveldb::WriteOptions(), table_info.table_name(), value);
    if (!status.ok()) {
        LOG(ERROR) << "fail to put meta to storage (table: "
            << table_info.table_name() << ")";
        *code = kTableDBError;
        return false;
    }
    return true;
}

bool TableManager::DropTable(const DropTableRequest* request,
                             DropTableResponse* response) {
    bool has_success = false;
    toft::RwLock::WriterLocker locker(&m_rwlock);
    for (int32_t i = 0; i < request->table_names_size(); ++i) {
        leveldb::Status status =
            m_meta_db->Delete(leveldb::WriteOptions(),
                              request->table_names(i));
        if (status.ok()) {
            LOG(ERROR) << "fail to delete table (" << request->table_names(i)
                << ")";
            response->add_results(kTableDBError);
        } else {
            response->add_results(kTableOk);
            has_success = true;
        }
    }
    return has_success;
}

bool TableManager::GetTable(const GetMetaInfoRequest* request,
                            GetMetaInfoResponse* response) {
    bool has_success = false;
    for (int32_t i = 0; i < request->table_names_size(); ++i) {
        TableStatusCode status = kTableOk;
        TableInfo table_info;
        if (GetSingleTable(request->table_names(i),
                           &table_info, &status)) {
            has_success = true;
            response->add_table_infos()->CopyFrom(table_info);
        }
        response->add_results(status);
    }
    return has_success;
}

bool TableManager::GetSingleTable(const std::string& table_name,
                                  TableInfo* table_info, TableStatusCode* code) {
    toft::RwLock::ReaderLocker locker(&m_rwlock);
    std::string value;
    leveldb::Status status = m_meta_db->Get(leveldb::ReadOptions(),
                                            table_name, &value);
    if (!status.ok()) {
        LOG(ERROR) << "table (" << table_name << ") not exist";
        *code = kTableNotExist;
        return false;
    }
    if (!StringToTableInfoPB(value, table_info)) {
        LOG(ERROR) << "fail to unserialize table meta (" << table_name << ")";
        *code = kTableCorrupt;
        return false;
    }
    return true;
}

uint64_t TableManager::GetTableNum() const {
    uint64_t count = 0;
    toft::RwLock::ReaderLocker locker(&m_rwlock);
    leveldb::Iterator* it = m_meta_db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        count++;
    }
    delete it;

    return count;
}

bool TableManager::LoadDatabase(const std::string& db_path, leveldb::DB** db_handler) {
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, db_path, db_handler);
    CHECK(status.ok()) << ", fail to open db: " << db_path
        << ", status: " << status.ToString();
    return true;
}

} // namespace master
} // namespace gunir
