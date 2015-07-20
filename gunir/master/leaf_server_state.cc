// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "gunir/master/leaf_server_state.h"

#include "gunir/master/job_manager.h"

namespace gunir {
namespace master {


LeafServerState::LeafServerState(const ServerInfo& server_info,
                                 JobManager* job_manager,
                                 toft::TimerManager* timer_manager)
    : ServerState(server_info, job_manager, timer_manager) {}

LeafServerState::~LeafServerState() {}

void LeafServerState::TimeoutUpdate() {

}

} // namespace master
} // namespace gunir

