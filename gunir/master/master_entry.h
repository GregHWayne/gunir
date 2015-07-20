// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef GUNIR_MASTER_MASTER_ENTRY_H
#define GUNIR_MASTER_MASTER_ENTRY_H


#include "toft/base/scoped_ptr.h"

#include "gunir/gunir_entry.h"
#include "gunir/rpc_server.h"

namespace gunir {
namespace master {

class MasterImpl;
class RemoteMaster;

class MasterEntry : public GunirEntry {
public:
    MasterEntry();
    ~MasterEntry();

    bool StartServer();
    void ShutdownServer();

private:
    toft::scoped_ptr<MasterImpl> m_master_impl;
    RemoteMaster* m_remote_master;
    toft::scoped_ptr<RpcServer> m_rpc_server;
};

} // namespace master
} // namespace gunir

#endif // GUNIR_MASTER_MASTER_ENTRY_H
