// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef GUNIR_MASTER_SERVER_STATE_H
#define GUNIR_MASTER_SERVER_STATE_H

#include "toft/system/threading/rwlock.h"
#include "toft/system/timer/timer_manager.h"

#include "gunir/proto/master_rpc.pb.h"
#include "gunir/proto/server_info.pb.h"
#include "gunir/master/scheduler_plan.pb.h"

namespace gunir {
namespace master {

class JobManager;

class ServerState {
public:
    ServerState(const ServerInfo& server_info,
                JobManager* job_manager,
                toft::TimerManager* timer_manager);
    virtual ~ServerState();

    void Register(const RegisterRequest* request,
                  RegisterResponse* response);

    void Report(const ReportRequest* request,
                ReportResponse* response);

    ServerType GetType() const;
    void FillSchedulerInfo(ServerSchedulerInfo* info) const;
    uint32_t GetCapacity() const;

protected:
    void ProcessReport(const ReportRequest* request,
                       ReportResponse* response);
    void EnableTimeoutTimer();
    void DisableTimeoutTimer();
    void HeartBeatTimeout(uint64_t timer_id);
    bool IsSameServerInfo(const ServerInfo& server_info) const;

    virtual void TimeoutUpdate() = 0;

protected:
    mutable toft::RwLock m_rwlock;

    ServerInfo m_server_info;
    JobManager* m_job_manager;
    toft::TimerManager* m_timer_manager;
    uint64_t m_timeout_timer_id;
    uint64_t m_last_sequence_id;
    StatusCode m_server_status;
};


} // namespace master
} // namespace gunir

#endif // GUNIR_MASTER_SERVER_STATE_H
