// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef GUNIR_MASTER_REMOTE_MASTER_H
#define GUNIR_MASTER_REMOTE_MASTER_H

#include "toft/base/scoped_ptr.h"
#include "toft/system/threading/thread_pool.h"

#include "gunir/proto/master_rpc.pb.h"

namespace gunir {
namespace master {

class MasterImpl;

class RemoteMaster : public MasterServer {
public:
    RemoteMaster(MasterImpl* master_impl);
    ~RemoteMaster();

    void SubmitJob(google::protobuf::RpcController* controller,
                   const SubmitJobRequest* request,
                   SubmitJobResponse* response,
                   google::protobuf::Closure* done);

    void GetJobResult(google::protobuf::RpcController* controller,
                      const GetJobResultRequest* request,
                      GetJobResultResponse* response,
                      google::protobuf::Closure* done);

    void GetMetaInfo(google::protobuf::RpcController* controller,
                      const GetMetaInfoRequest* request,
                      GetMetaInfoResponse* response,
                      google::protobuf::Closure* done);

    void AddTable(google::protobuf::RpcController* controller,
                      const AddTableRequest* request,
                      AddTableResponse* response,
                      google::protobuf::Closure* done);

    void DropTable(google::protobuf::RpcController* controller,
                      const DropTableRequest* request,
                      DropTableResponse* response,
                      google::protobuf::Closure* done);

    void Register(google::protobuf::RpcController* controller,
                  const RegisterRequest* request,
                  RegisterResponse* response,
                  google::protobuf::Closure* done);

    void Report(google::protobuf::RpcController* controller,
                const ReportRequest* request,
                ReportResponse* response,
                google::protobuf::Closure* done);

private:
    void DoSubmitJob(google::protobuf::RpcController* controller,
                    const SubmitJobRequest* request,
                    SubmitJobResponse* response,
                    google::protobuf::Closure* done);

    void DoGetJobResult(google::protobuf::RpcController* controller,
                        const GetJobResultRequest* request,
                        GetJobResultResponse* response,
                        google::protobuf::Closure* done);

    void DoGetMetaInfo(google::protobuf::RpcController* controller,
                       const GetMetaInfoRequest* request,
                       GetMetaInfoResponse* response,
                       google::protobuf::Closure* done);

    void DoAddTable(google::protobuf::RpcController* controller,
                    const AddTableRequest* request,
                    AddTableResponse* response,
                    google::protobuf::Closure* done);

    void DoDropTable(google::protobuf::RpcController* controller,
                     const DropTableRequest* request,
                     DropTableResponse* response,
                     google::protobuf::Closure* done);

    void DoRegister(google::protobuf::RpcController* controller,
                    const RegisterRequest* request,
                    RegisterResponse* response,
                    google::protobuf::Closure* done);

    void DoReport(google::protobuf::RpcController* controller,
                  const ReportRequest* request,
                  ReportResponse* response,
                  google::protobuf::Closure* done);

private:
    MasterImpl* m_master_impl;
    toft::scoped_ptr<toft::ThreadPool> m_thread_pool;
};


} // namespace master
} // namespace gunir

#endif // GUNIR_MASTER_REMOTE_MASTER_H
