// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/master/remote_master.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "toft/base/closure.h"

#include "gunir/master/master_impl.h"

DECLARE_int32(gunir_master_thread_min_num);
DECLARE_int32(gunir_master_thread_max_num);

namespace gunir {
namespace master {


RemoteMaster::RemoteMaster(MasterImpl* master_impl)
    : m_master_impl(master_impl),
      m_thread_pool(new toft::ThreadPool(FLAGS_gunir_master_thread_min_num,
                                   FLAGS_gunir_master_thread_max_num)) {}

RemoteMaster::~RemoteMaster() {}

void RemoteMaster::SubmitJob(google::protobuf::RpcController* controller,
                             const SubmitJobRequest* request,
                             SubmitJobResponse* response,
                             google::protobuf::Closure* done) {
    toft::Closure<void ()>* callback =
        toft::NewClosure(this, &RemoteMaster::DoSubmitJob, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::GetJobResult(google::protobuf::RpcController* controller,
                                const GetJobResultRequest* request,
                                GetJobResultResponse* response,
                                google::protobuf::Closure* done) {
    toft::Closure<void ()>* callback =
        toft::NewClosure(this, &RemoteMaster::DoGetJobResult, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::GetMetaInfo(google::protobuf::RpcController* controller,
                               const GetMetaInfoRequest* request,
                               GetMetaInfoResponse* response,
                               google::protobuf::Closure* done) {
    toft::Closure<void ()>* callback =
        toft::NewClosure(this, &RemoteMaster::DoGetMetaInfo, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::AddTable(google::protobuf::RpcController* controller,
                            const AddTableRequest* request,
                            AddTableResponse* response,
                            google::protobuf::Closure* done) {
    toft::Closure<void ()>* callback =
        toft::NewClosure(this, &RemoteMaster::DoAddTable, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::DropTable(google::protobuf::RpcController* controller,
                             const DropTableRequest* request,
                             DropTableResponse* response,
                             google::protobuf::Closure* done) {
    toft::Closure<void ()>* callback =
        toft::NewClosure(this, &RemoteMaster::DoDropTable, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::Register(google::protobuf::RpcController* controller,
                            const RegisterRequest* request,
                            RegisterResponse* response,
                            google::protobuf::Closure* done) {
    toft::Closure<void ()>* callback =
        toft::NewClosure(this, &RemoteMaster::DoRegister, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::Report(google::protobuf::RpcController* controller,
                          const ReportRequest* request,
                          ReportResponse* response,
                          google::protobuf::Closure* done) {
    toft::Closure<void ()>* callback =
        toft::NewClosure(this, &RemoteMaster::DoReport, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

// internal

void RemoteMaster::DoSubmitJob(google::protobuf::RpcController* controller,
                              const SubmitJobRequest* request,
                              SubmitJobResponse* response,
                              google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (SubmitJob)";
    m_master_impl->SubmitJob(request, response);
    LOG(INFO) << "finish RPC (SubmitJob)";

    done->Run();
}

void RemoteMaster::DoGetJobResult(google::protobuf::RpcController* controller,
                            const GetJobResultRequest* request,
                            GetJobResultResponse* response,
                            google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (GetJobResult)";
    m_master_impl->GetJobResult(request, response);
    LOG(INFO) << "finish RPC (GetJobResult)";

    done->Run();
}

void RemoteMaster::DoGetMetaInfo(google::protobuf::RpcController* controller,
                            const GetMetaInfoRequest* request,
                            GetMetaInfoResponse* response,
                            google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (GetMetaInfo)";
    m_master_impl->GetMetaInfo(request, response);
    LOG(INFO) << "finish RPC (GetMetaInfo)";

    done->Run();
}

void RemoteMaster::DoAddTable(google::protobuf::RpcController* controller,
                              const AddTableRequest* request,
                              AddTableResponse* response,
                              google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (AddTable)";
    m_master_impl->AddTable(request, response);
    LOG(INFO) << "finish RPC (AddTable)";

    done->Run();
}

void RemoteMaster::DoDropTable(google::protobuf::RpcController* controller,
                               const DropTableRequest* request,
                               DropTableResponse* response,
                               google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (DropTable)";
    m_master_impl->DropTable(request, response);
    LOG(INFO) << "finish RPC (DropTable)";

    done->Run();
}

void RemoteMaster::DoRegister(google::protobuf::RpcController* controller,
                              const RegisterRequest* request,
                              RegisterResponse* response,
                              google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (Register)";
    m_master_impl->Register(request, response);
    LOG(INFO) << "finish RPC (Register)";

    done->Run();
}

void RemoteMaster::DoReport(google::protobuf::RpcController* controller,
                            const ReportRequest* request,
                            ReportResponse* response,
                            google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (Report)";
    m_master_impl->Report(request, response);
    LOG(INFO) << "finish RPC (Report)";

    done->Run();
}


} // namespace master
} // namespace gunir
