// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef GUNIR_LEAFNODE_LEAFNODE_ENTRY_H
#define GUNIR_LEAFNODE_LEAFNODE_ENTRY_H


#include "toft/base/scoped_ptr.h"

#include "gunir/gunir_entry.h"
#include "gunir/rpc_server.h"

namespace gunir {
namespace leafnode {

class LeafNodeImpl;
class RemoteLeafNode;

class LeafNodeEntry : public GunirEntry {
public:
    LeafNodeEntry();
    ~LeafNodeEntry();

    bool StartServer();
    void ShutdownServer();
    bool Run();

private:
    toft::scoped_ptr<LeafNodeImpl> m_leafnode_impl;
    RemoteLeafNode* m_remote_leafnode;
    toft::scoped_ptr<RpcServer> m_rpc_server;
};

} // namespace leafnode
} // namespace gunir

#endif // GUNIR_LEAFNODE_LEAFNODE_ENTRY_H
