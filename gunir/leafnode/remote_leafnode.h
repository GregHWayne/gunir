// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef GUNIR_LEAFNODE_REMOTE_LEAFNODE_H
#define GUNIR_LEAFNODE_REMOTE_LEAFNODE_H

#include "toft/base/scoped_ptr.h"
#include "toft/system/threading/thread_pool.h"

#include "gunir/proto/leafnode_rpc.pb.h"

namespace gunir {
namespace leafnode {

class LeafNodeImpl;

class RemoteLeafNode : public LeafNodeServer {
public:
    RemoteLeafNode(LeafNodeImpl* leafnode_impl);
    ~RemoteLeafNode();

    void PushLeafTask(google::protobuf::RpcController* controller,
                   const PushLeafTaskRequest* request,
                   PushLeafTaskResponse* response,
                   google::protobuf::Closure* done);

private:
    void DoPushLeafTask(google::protobuf::RpcController* controller,
                    const PushLeafTaskRequest* request,
                    PushLeafTaskResponse* response,
                    google::protobuf::Closure* done);

private:
    LeafNodeImpl* m_leafnode_impl;
    toft::scoped_ptr<toft::ThreadPool> m_thread_pool;
};


} // namespace leafnode
} // namespace gunir

#endif // GUNIR_LEAFNODE_REMOTE_LEAFNODE_H
