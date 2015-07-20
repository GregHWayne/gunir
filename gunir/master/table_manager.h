// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef GUNIR_MASTER_TABLE_MANAGER_H
#define GUNIR_MASTER_TABLE_MANAGER_H

#include <map>
#include <set>
#include <string>

#include "thirdparty/leveldb/db.h"
#include "toft/base/scoped_ptr.h"
#include "toft/system/threading/rwlock.h"
#include "toft/system/threading/thread_pool.h"

#include "gunir/proto/table.pb.h"
#include "gunir/proto/master_rpc.pb.h"

namespace gunir {
namespace master {

class TableManager {
public:
    TableManager();
    ~TableManager();

    bool AddTable(const AddTableRequest* request,
                  AddTableResponse* response);

    bool DropTable(const DropTableRequest* request,
                   DropTableResponse* response);

    bool GetTable(const GetMetaInfoRequest* request,
                  GetMetaInfoResponse* response);
    bool GetSingleTable(const std::string& table_name,
                        TableInfo* table_info, TableStatusCode* code);

    uint64_t GetTableNum() const;

private:
    bool LoadDatabase(const std::string& db_path, leveldb::DB** db_handler);
    bool AddSingleTable(const TableInfo& table_info, TableStatusCode* status);

    bool AddTableInfo(const AddTableRequest* request,
                      AddTableResponse* response);
    bool CreateTable(const AddTableRequest* request,
                     AddTableResponse* response);
    void DoCreateTable(AddTableRequest request);

private:
    mutable toft::RwLock m_rwlock;
    mutable toft::Mutex m_mutex;
    leveldb::DB* m_meta_db;

    std::set<std::string> m_creating_tables;
    toft::scoped_ptr<toft::ThreadPool> m_thread_pool;

//     std::map<std::string, TableInfo> m_tables;
};

} // namespace master
} // namespace gunir

#endif // GUNIR_MASTER_TABLE_MANAGER_H
