// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef GUNIR_MASTER_SERVER_MANAGER_H
#define GUNIR_MASTER_SERVER_MANAGER_H

#include <map>

#include "toft/system/threading/mutex.h"
#include "toft/system/timer/timer_manager.h"

#include "gunir/proto/master_rpc.pb.h"
#include "gunir/master/scheduler_plan.pb.h"

namespace gunir {
namespace master {

class ServerState;
class JobManager;

class ServerManager {
public:
    ServerManager(JobManager* job_manager,
                  toft::TimerManager* timer_manager);
    ~ServerManager();

    void Register(const RegisterRequest* request,
                  RegisterResponse* response);

    void Report(const ReportRequest* request,
                ReportResponse* response);

    void GetSchedulerServerInfo(SchedulerInfo* info) const;

private:
    mutable toft::Mutex m_mutex;
    std::map<std::string, ServerState*> m_server_list;

    JobManager* m_job_manager;
    toft::TimerManager* m_timer_manager;
};

} // namespace master
} // namespace gunir

#endif // GUNIR_MASTER_SERVER_MANAGER_H
