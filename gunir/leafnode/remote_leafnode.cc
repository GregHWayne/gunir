// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/leafnode/remote_leafnode.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "toft/base/closure.h"

#include "gunir/leafnode/leafnode_impl.h"

DECLARE_int32(gunir_leafnode_thread_min_num);
DECLARE_int32(gunir_leafnode_thread_max_num);

namespace gunir {
namespace leafnode {


RemoteLeafNode::RemoteLeafNode(LeafNodeImpl* leafnode_impl)
    : m_leafnode_impl(leafnode_impl),
      m_thread_pool(new toft::ThreadPool(FLAGS_gunir_leafnode_thread_min_num,
                                   FLAGS_gunir_leafnode_thread_max_num)) {}

RemoteLeafNode::~RemoteLeafNode() {}

void RemoteLeafNode::PushLeafTask(google::protobuf::RpcController* controller,
                            const PushLeafTaskRequest* request,
                            PushLeafTaskResponse* response,
                            google::protobuf::Closure* done) {
    toft::Closure<void ()>* callback =
        toft::NewClosure(this, &RemoteLeafNode::DoPushLeafTask, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

// internal

void RemoteLeafNode::DoPushLeafTask(google::protobuf::RpcController* controller,
                              const PushLeafTaskRequest* request,
                              PushLeafTaskResponse* response,
                              google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (PushLeafTask)";
    m_leafnode_impl->PushLeafTask(request, response);
    LOG(INFO) << "finish RPC (PushLeafTask)";

    done->Run();
}


} // namespace leafnode
} // namespace gunir
