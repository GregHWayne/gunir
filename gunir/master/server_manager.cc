// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#include "gunir/master/server_manager.h"

#include "gunir/master/job_manager.h"
#include "gunir/master/leaf_server_state.h"
#include "gunir/master/server_state.h"
#include "gunir/master/stem_server_state.h"

namespace gunir {
namespace master {


ServerManager::ServerManager(JobManager* job_manager,
                             toft::TimerManager* timer_manager)
    : m_job_manager(job_manager),
      m_timer_manager(timer_manager) {}

ServerManager::~ServerManager() {}

void ServerManager::Register(const RegisterRequest* request,
                             RegisterResponse* response) {
    ServerState* server_state = NULL;
    {
        toft::MutexLocker lock(&m_mutex);
        std::map<std::string, ServerState*>::iterator it =
            m_server_list.find(request->server_info().addr());
        if (it == m_server_list.end()) {
            if (request->server_info().type() == kStemServer) {
                server_state = m_server_list[request->server_info().addr()]
                    = new StemServerState(request->server_info(), m_job_manager,
                                          m_timer_manager);
            } else if (request->server_info().type() == kLeafServer) {
                server_state = m_server_list[request->server_info().addr()]
                    = new LeafServerState(request->server_info(), m_job_manager,
                                          m_timer_manager);
            } else {
                response->set_status(kInvalidServerInfo);
                return;
            }
        } else {
            server_state = it->second;
        }
    }
    CHECK(server_state != NULL);
    server_state->Register(request, response);
}

void ServerManager::Report(const ReportRequest* request,
                           ReportResponse* response) {
    ServerState* server_state = NULL;
    {
        toft::MutexLocker lock(&m_mutex);
        std::map<std::string, ServerState*>::iterator it =
            m_server_list.find(request->server_info().addr());
        if (it == m_server_list.end()) {
            response->set_status(kServerNotExist);
            return;
        } else {
            server_state = it->second;
        }
    }
    CHECK(server_state != NULL);
    server_state->Report(request, response);
}

void ServerManager::GetSchedulerServerInfo(SchedulerInfo* info) const {
    uint32_t inter_slot = 0;
    uint32_t leaf_slot = 0;
    std::map<std::string, ServerState*>::const_iterator it = m_server_list.begin();
    for (; it != m_server_list.end(); ++it) {
        if (it->second->GetType() == kStemServer) {
            it->second->FillSchedulerInfo(info->add_inter_server());
            inter_slot += it->second->GetCapacity();
        } else if (it->second->GetType() == kLeafServer) {
            it->second->FillSchedulerInfo(info->add_leaf_server());
            leaf_slot += it->second->GetCapacity();
        }
    }
    LOG(INFO) << "inter slot: " << inter_slot
        << ", leaf slot: " << leaf_slot;
    info->set_total_inter_slot(inter_slot);
    info->set_total_leaf_slot(leaf_slot);
}

} // namespace master
} // namespace gunir
