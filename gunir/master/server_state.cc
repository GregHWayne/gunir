// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/master/server_state.h"

#include "thirdparty/glog/logging.h"
#include "toft/base/closure.h"

#include "gunir/master/job_manager.h"
#include "gunir/types.h"

DECLARE_int64(gunir_heartbeat_period);
DECLARE_int32(gunir_heartbeat_timeout_period_factor);

namespace gunir {
namespace master {


ServerState::ServerState(const ServerInfo& server_info,
                         JobManager* job_manager,
                         toft::TimerManager* timer_manager)
    : m_server_info(server_info),
      m_job_manager(job_manager),
      m_timer_manager(timer_manager),
      m_timeout_timer_id(kInvalidTimerId),
      m_last_sequence_id(kSequenceIDStart),
      m_server_status(kServerNotInited) {}

ServerState::~ServerState() {}

ServerType ServerState::GetType() const {
    return m_server_info.type();
}

uint32_t ServerState::GetCapacity() const {
    return m_server_info.slot_number();
}

void ServerState::Register(const RegisterRequest* request,
                           RegisterResponse* response) {
    if (kSequenceIDStart != request->sequence_id()) {
        LOG(ERROR) << "Server register with wrong sequence_id="
            << request->sequence_id();
        response->set_status(kInvalidSequenceId);
        return;
    }

    VLOG(10) << "Server send register request ["
        << request->ShortDebugString() << "]";
    DisableTimeoutTimer();
    m_server_info.CopyFrom(request->server_info());
    m_server_status = kServerIsRunning;
    m_last_sequence_id = request->sequence_id();
}

void ServerState::Report(const ReportRequest* request,
                         ReportResponse* response) {
    LOG(INFO) << "Server send report request ["
        << request->ShortDebugString() << "]";

    switch (m_server_status) {
    case kServerNotInited:
        response->set_status(kServerNotRegistered);
        break;
    case kServerIsRunning:
    case kServerTimeout:
        if (!IsSameServerInfo(request->server_info())) {
            response->set_status(kInvalidServerInfo);
            break;
        }

        // disable timeout timer
        DisableTimeoutTimer();

        // start process report
        ProcessReport(request, response);
        return;
    default :
        CHECK(false) << " Server get a unknown status " << m_server_status
            << ", there must be something wrong in your logic ";
    }
}

void ServerState::ProcessReport(const ReportRequest* request,
                                ReportResponse* response) {
    for (int i = 0; i < request->task_state_size(); ++i) {
        const TaskInfo& task_info = request->task_state(i).task_info();

        if (!m_job_manager->ReportTaskState(request->task_state(i))) {
            response->add_finished_job_id(task_info.job_id());
        }
    }
}

bool ServerState::IsSameServerInfo(const ServerInfo& server_info) const {
    return server_info.addr() == m_server_info.addr() &&
        server_info.type() == m_server_info.type();
}

void ServerState::EnableTimeoutTimer() {
    if (m_timeout_timer_id == kInvalidTimerId) {
        toft::Closure<void (uint64_t)>* closure = toft::NewClosure(
            this, &ServerState::HeartBeatTimeout);
        uint64_t wait_time = FLAGS_gunir_heartbeat_period *
            FLAGS_gunir_heartbeat_timeout_period_factor;
        m_timeout_timer_id = m_timer_manager->AddOneshotTimer(
            wait_time, closure);
    } else {
        m_timer_manager->EnableTimer(m_timeout_timer_id);
    }
}

void ServerState::DisableTimeoutTimer() {
    if (m_timeout_timer_id != kInvalidTimerId) {
        m_timer_manager->DisableTimer(m_timeout_timer_id);
    }
}

void ServerState::HeartBeatTimeout(uint64_t timer_id) {
    DCHECK_EQ(timer_id, m_timeout_timer_id);
    m_timeout_timer_id = kInvalidTimerId;

    LOG(WARNING) << "Server [" << m_server_info.ShortDebugString()
        << "] start timeout";
    TimeoutUpdate();
}

void ServerState::FillSchedulerInfo(ServerSchedulerInfo* info) const {
    info->set_id(1);
    info->set_server_addr(m_server_info.addr());
    info->set_left_slot_number(1);
    info->set_reserve_slot_number(0);
}

} // namespace master
} // namespace gunir
