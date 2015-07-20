// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef GUNIR_MASTER_MASTER_IMPL_H
#define GUNIR_MASTER_MASTER_IMPL_H

#include "toft/base/scoped_ptr.h"
#include "toft/system/threading/mutex.h"
#include "toft/system/threading/rwlock.h"
#include "toft/system/timer/timer_manager.h"

#include "gunir/proto/master_rpc.pb.h"

namespace gunir {
namespace master {

class JobManager;
class ServerManager;
class TableManager;
class JobScheduler;
class JobEmitter;

class MasterImpl {
public:
    enum MasterStatus {
        kNotInited = kMasterNotInited,
        kIsBusy = kMasterIsBusy,
        kIsSecondary = kMasterIsSecondary,
        kIsReadonly = kMasterIsReadonly
    };

    MasterImpl();
    ~MasterImpl();

    bool Init();

    bool SubmitJob(const SubmitJobRequest* request,
                   SubmitJobResponse* response);

    bool GetJobResult(const GetJobResultRequest* request,
                      GetJobResultResponse* response);

    bool GetMetaInfo(const GetMetaInfoRequest* request,
                     GetMetaInfoResponse* response);

    bool AddTable(const AddTableRequest* request,
                  AddTableResponse* response);

    bool DropTable(const DropTableRequest* request,
                   DropTableResponse* response);

    bool Report(const ReportRequest* request,
                ReportResponse* response);

    bool Register(const RegisterRequest* request,
                  RegisterResponse* response);

private:
    mutable toft::Mutex m_status_mutex;
    MasterStatus m_status;

    mutable toft::RwLock m_rwlock;
    toft::TimerManager m_timer_manager;

    toft::scoped_ptr<ServerManager> m_server_manager;
    toft::scoped_ptr<TableManager> m_table_manager;
    toft::scoped_ptr<JobManager> m_job_manager;
    toft::scoped_ptr<JobScheduler> m_job_scheduler;
    toft::scoped_ptr<JobEmitter> m_job_emitter;
};


} // namespace master
} // namespace gunir

#endif // GUNIR_MASTER_MASTER_IMPL_H
