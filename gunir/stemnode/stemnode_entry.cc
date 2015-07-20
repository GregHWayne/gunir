// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/stemnode/stemnode_entry.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "toft/system/threading/this_thread.h"

#include "gunir/stemnode/stemnode_impl.h"
#include "gunir/stemnode/remote_stemnode.h"
#include "gunir/proto/server_info.pb.h"
#include "gunir/utils/env_utils.h"
#include "gunir/utils/ip_address.h"

DECLARE_string(gunir_stemnode_addr);
DECLARE_string(gunir_stemnode_port);
DECLARE_int64(gunir_heartbeat_period);
DECLARE_int32(gunir_stemnode_worker_thread_num);

namespace gunir {
namespace stemnode {

StemNodeEntry::StemNodeEntry()
    : m_stemnode_impl(NULL),
      m_remote_stemnode(NULL),
      m_rpc_server(NULL) {}

StemNodeEntry::~StemNodeEntry() {}

bool StemNodeEntry::StartServer() {
    IpAddress stemnode_addr(utils::GetLocalHostAddr(), FLAGS_gunir_stemnode_port);
    FLAGS_gunir_stemnode_addr = stemnode_addr.ToString();
    LOG(INFO) << "Start stemnode RPC server at: " << FLAGS_gunir_stemnode_addr;

    ServerInfo node_info;
    node_info.set_addr(stemnode_addr.ToString());
    node_info.set_status(kServerIsRunning);
    node_info.set_type(kStemServer);
    node_info.set_slot_number(FLAGS_gunir_stemnode_worker_thread_num);

    m_stemnode_impl.reset(new StemNodeImpl(node_info));
    m_remote_stemnode = new RemoteStemNode(m_stemnode_impl.get());

    if (!m_stemnode_impl->Init()) {
        return false;
    }

    m_rpc_server.reset(new RpcServer(stemnode_addr.GetIp(),
                                     stemnode_addr.GetPort()));
    m_rpc_server->RegisterService(m_remote_stemnode);
    if (!m_rpc_server->StartServer()) {
        LOG(ERROR) << "start RPC server error";
        return false;
    } else if (!m_stemnode_impl->Register()) {
        LOG(ERROR) << "fail to register to master";
        return false;
    }

//     if (!m_stemnode_impl->Init()) {
//         return false;
//     }

    LOG(INFO) << "finish starting stemnode server";
    return true;
}

void StemNodeEntry::ShutdownServer() {
    m_rpc_server->StopServer();
    m_stemnode_impl.reset();
}

bool StemNodeEntry::Run() {
    m_stemnode_impl->Report();
    toft::ThisThread::Sleep(FLAGS_gunir_heartbeat_period);
    return true;
}

} // namespace stemnode
} // namespace gunir
