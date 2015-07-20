// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/leafnode/leafnode_entry.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "toft/system/threading/this_thread.h"

#include "gunir/leafnode/leafnode_impl.h"
#include "gunir/leafnode/remote_leafnode.h"
#include "gunir/proto/server_info.pb.h"
#include "gunir/utils/env_utils.h"
#include "gunir/utils/ip_address.h"

DECLARE_string(gunir_leafnode_addr);
DECLARE_string(gunir_leafnode_port);
DECLARE_int64(gunir_heartbeat_period);
DECLARE_int32(gunir_leafnode_worker_thread_num);

namespace gunir {
namespace leafnode {

LeafNodeEntry::LeafNodeEntry()
    : m_leafnode_impl(NULL),
      m_remote_leafnode(NULL),
      m_rpc_server(NULL) {}

LeafNodeEntry::~LeafNodeEntry() {}

bool LeafNodeEntry::StartServer() {
    IpAddress leafnode_addr(utils::GetLocalHostAddr(), FLAGS_gunir_leafnode_port);
    FLAGS_gunir_leafnode_addr = leafnode_addr.ToString();
    LOG(INFO) << "Start leafnode RPC server at: " << FLAGS_gunir_leafnode_addr;

    ServerInfo node_info;
    node_info.set_addr(leafnode_addr.ToString());
    node_info.set_status(kServerIsRunning);
    node_info.set_type(kLeafServer);
    node_info.set_slot_number(FLAGS_gunir_leafnode_worker_thread_num);

    m_leafnode_impl.reset(new LeafNodeImpl(node_info));
    m_remote_leafnode = new RemoteLeafNode(m_leafnode_impl.get());

    if (!m_leafnode_impl->Init()) {
        return false;
    }
    m_rpc_server.reset(new RpcServer(leafnode_addr.GetIp(),
                                     leafnode_addr.GetPort()));
    m_rpc_server->RegisterService(m_remote_leafnode);
    if (!m_rpc_server->StartServer()) {
        LOG(ERROR) << "start RPC server error";
        return false;
    } else if (!m_leafnode_impl->Register()) {
        LOG(ERROR) << "fail to register to master";
        return false;
    }

//     if (!m_leafnode_impl->Init()) {
//         return false;
//     }

    LOG(INFO) << "finish starting leafnode server";
    return true;
}

void LeafNodeEntry::ShutdownServer() {
    m_rpc_server->StopServer();
    m_leafnode_impl.reset();
}

bool LeafNodeEntry::Run() {
    m_leafnode_impl->Report();
    toft::ThisThread::Sleep(FLAGS_gunir_heartbeat_period);
    return true;
}

} // namespace leafnode
} // namespace gunir
