// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/master/master_entry.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "gunir/master/master_impl.h"
#include "gunir/master/remote_master.h"
#include "gunir/utils/ip_address.h"
#include "gunir/utils/env_utils.h"

DECLARE_string(gunir_master_addr);
DECLARE_string(gunir_master_port);

namespace gunir {
namespace master {

MasterEntry::MasterEntry()
    : m_master_impl(NULL),
      m_remote_master(NULL),
      m_rpc_server(NULL) {}

MasterEntry::~MasterEntry() {}

bool MasterEntry::StartServer() {
    IpAddress master_addr(utils::GetLocalHostAddr(), FLAGS_gunir_master_port);
    FLAGS_gunir_master_addr = master_addr.ToString();
    LOG(INFO) << "Start master RPC server at: " << FLAGS_gunir_master_addr;

    m_master_impl.reset(new MasterImpl());
    m_remote_master = new RemoteMaster(m_master_impl.get());

    if (!m_master_impl->Init()) {
        return false;
    }

    m_rpc_server.reset(new RpcServer(master_addr.GetIp(),
                                     master_addr.GetPort()));
    m_rpc_server->RegisterService(m_remote_master);
    if (!m_rpc_server->StartServer()) {
        LOG(ERROR) << "start RPC server error";
        return false;
    }

    LOG(INFO) << "finish starting master server";
    return true;
}

void MasterEntry::ShutdownServer() {
    m_rpc_server->StopServer();
    m_master_impl.reset();
}

} // namespace master
} // namespace gunir
