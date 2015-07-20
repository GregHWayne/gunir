// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#include "gunir/master/master_impl.h"

#include "gunir/master/job_manager.h"
#include "gunir/master/job_scheduler.h"
#include "gunir/master/job_emitter.h"
#include "gunir/master/server_manager.h"
#include "gunir/master/table_manager.h"

namespace gunir {
namespace master {


MasterImpl::MasterImpl()
    : m_server_manager(NULL) {
    m_table_manager.reset(new TableManager());
    m_job_manager.reset(new JobManager(m_table_manager.get(), &m_timer_manager));
    m_server_manager.reset(new ServerManager(m_job_manager.get(), &m_timer_manager));

    m_job_scheduler.reset(new JobScheduler(m_job_manager.get(), m_server_manager.get()));
    m_job_emitter.reset(new JobEmitter(m_job_manager.get(), m_server_manager.get()));

    m_job_scheduler->Start();
    m_job_emitter->Start();
}

MasterImpl::~MasterImpl() {
    m_job_scheduler.reset();
    m_job_emitter.reset();
    m_job_manager.reset();
    m_server_manager.reset();
}

bool MasterImpl::Init() {
    return true;
}

bool MasterImpl::SubmitJob(const SubmitJobRequest* request,
                           SubmitJobResponse* response) {
    LOG(INFO) << "rpc (SubmitJob): " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());
    response->set_status(kMasterOk);
    m_job_manager->AddJob(request, response);
    return true;
}

bool MasterImpl::GetJobResult(const GetJobResultRequest* request,
                              GetJobResultResponse* response) {
    LOG(INFO) << "rpc (GetJobResult): " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());
    response->set_status(kMasterOk);
    m_job_manager->GetJobResult(request, response);
    return true;
}

bool MasterImpl::GetMetaInfo(const GetMetaInfoRequest* request,
                             GetMetaInfoResponse* response) {
    LOG(INFO) << "rpc (GetMetaInfo): " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());
    response->set_status(kMasterOk);
    m_table_manager->GetTable(request, response);
    return true;
}

bool MasterImpl::AddTable(const AddTableRequest* request,
                          AddTableResponse* response) {
    LOG(INFO) << "rpc (AddTable): " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());
    response->set_status(kMasterOk);
    m_table_manager->AddTable(request, response);
    return true;
}

bool MasterImpl::DropTable(const DropTableRequest* request,
                           DropTableResponse* response) {
    LOG(INFO) << "rpc (DropTable): " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());
    response->set_status(kMasterOk);
    m_table_manager->DropTable(request, response);
    return true;
}

bool MasterImpl::Report(const ReportRequest* request,
                        ReportResponse* response) {
    LOG(INFO) << "rpc report: " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());
    response->set_status(kMasterOk);
    m_server_manager->Report(request, response);
    return true;
}

bool MasterImpl::Register(const RegisterRequest* request,
                          RegisterResponse* response) {
    LOG(INFO) << "rpc register: " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());
    response->set_status(kMasterOk);
    m_server_manager->Register(request, response);
    return true;
}


} // namespace master
} // namespace gunir
