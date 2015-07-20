// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef GUNIR_STEMNODE_REMOTE_STEMNODE_H
#define GUNIR_STEMNODE_REMOTE_STEMNODE_H

#include "toft/base/scoped_ptr.h"
#include "toft/system/threading/thread_pool.h"

#include "gunir/proto/stemnode_rpc.pb.h"

namespace gunir {
namespace stemnode {

class StemNodeImpl;

class RemoteStemNode : public StemNodeServer {
public:
    RemoteStemNode(StemNodeImpl* stemnode_impl);
    ~RemoteStemNode();

    void PushInterTask(google::protobuf::RpcController* controller,
                       const PushInterTaskRequest* request,
                       PushInterTaskResponse* response,
                       google::protobuf::Closure* done);

    void ReportTaskResult(google::protobuf::RpcController* controller,
                          const ReportTaskResultRequest* request,
                          ReportTaskResultResponse* response,
                          google::protobuf::Closure* done);

    void ReportResultSize(google::protobuf::RpcController* controller,
                          const ReportResultSizeRequest* request,
                          ReportResultSizeResponse* response,
                          google::protobuf::Closure* done);

private:
    void DoPushInterTask(google::protobuf::RpcController* controller,
                         const PushInterTaskRequest* request,
                         PushInterTaskResponse* response,
                         google::protobuf::Closure* done);

    void DoReportTaskResult(google::protobuf::RpcController* controller,
                            const ReportTaskResultRequest* request,
                            ReportTaskResultResponse* response,
                            google::protobuf::Closure* done);

    void DoReportResultSize(google::protobuf::RpcController* controller,
                            const ReportResultSizeRequest* request,
                            ReportResultSizeResponse* response,
                            google::protobuf::Closure* done);

private:
    StemNodeImpl* m_stemnode_impl;
    toft::scoped_ptr<toft::ThreadPool> m_thread_pool;
};


} // namespace stemnode
} // namespace gunir

#endif // GUNIR_STEMNODE_REMOTE_STEMNODE_H
