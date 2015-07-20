// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef GUNIR_STEMNODE_STEMNODE_ENTRY_H
#define GUNIR_STEMNODE_STEMNODE_ENTRY_H


#include "toft/base/scoped_ptr.h"

#include "gunir/gunir_entry.h"
#include "gunir/rpc_server.h"

namespace gunir {
namespace stemnode {

class StemNodeImpl;
class RemoteStemNode;

class StemNodeEntry : public GunirEntry {
public:
    StemNodeEntry();
    ~StemNodeEntry();

    bool StartServer();
    void ShutdownServer();
    bool Run();

private:
    toft::scoped_ptr<StemNodeImpl> m_stemnode_impl;
    RemoteStemNode* m_remote_stemnode;
    toft::scoped_ptr<RpcServer> m_rpc_server;
};

} // namespace stemnode
} // namespace gunir

#endif // GUNIR_STEMNODE_STEMNODE_ENTRY_H
