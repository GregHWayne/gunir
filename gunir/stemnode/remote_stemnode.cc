// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/stemnode/remote_stemnode.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "toft/base/closure.h"

#include "gunir/stemnode/stemnode_impl.h"

DECLARE_int32(gunir_stemnode_thread_min_num);
DECLARE_int32(gunir_stemnode_thread_max_num);

namespace gunir {
namespace stemnode {


RemoteStemNode::RemoteStemNode(StemNodeImpl* stemnode_impl)
    : m_stemnode_impl(stemnode_impl),
      m_thread_pool(new toft::ThreadPool(FLAGS_gunir_stemnode_thread_min_num,
                                   FLAGS_gunir_stemnode_thread_max_num)) {}

RemoteStemNode::~RemoteStemNode() {}

void RemoteStemNode::PushInterTask(google::protobuf::RpcController* controller,
                                   const PushInterTaskRequest* request,
                                   PushInterTaskResponse* response,
                                   google::protobuf::Closure* done) {
    toft::Closure<void ()>* callback =
        toft::NewClosure(this, &RemoteStemNode::DoPushInterTask, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteStemNode::ReportTaskResult(google::protobuf::RpcController* controller,
                                      const ReportTaskResultRequest* request,
                                      ReportTaskResultResponse* response,
                                      google::protobuf::Closure* done) {
    toft::Closure<void ()>* callback =
        toft::NewClosure(this, &RemoteStemNode::DoReportTaskResult, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteStemNode::ReportResultSize(google::protobuf::RpcController* controller,
                                      const ReportResultSizeRequest* request,
                                      ReportResultSizeResponse* response,
                                      google::protobuf::Closure* done) {
    toft::Closure<void ()>* callback =
        toft::NewClosure(this, &RemoteStemNode::DoReportResultSize, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

// internal

void RemoteStemNode::DoPushInterTask(google::protobuf::RpcController* controller,
                                     const PushInterTaskRequest* request,
                                     PushInterTaskResponse* response,
                                     google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (PushInterTask)";
    m_stemnode_impl->PushInterTask(request, response);
    LOG(INFO) << "finish RPC (PushInterTask)";

    done->Run();
}

void RemoteStemNode::DoReportTaskResult(google::protobuf::RpcController* controller,
                                        const ReportTaskResultRequest* request,
                                        ReportTaskResultResponse* response,
                                        google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (ReportTaskResult)";
    m_stemnode_impl->ReportTaskResult(request, response);
    LOG(INFO) << "finish RPC (ReportTaskResult)";

    done->Run();
}

void RemoteStemNode::DoReportResultSize(google::protobuf::RpcController* controller,
                                        const ReportResultSizeRequest* request,
                                        ReportResultSizeResponse* response,
                                        google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (ReportResultSize)";
    m_stemnode_impl->ReportResultSize(request, response);
    LOG(INFO) << "finish RPC (ReportResultSize)";

    done->Run();
}


} // namespace stemnode
} // namespace gunir
