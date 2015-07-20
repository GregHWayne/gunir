// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "gunir/master/stem_server_state.h"

namespace gunir {
namespace master {


StemServerState::StemServerState(const ServerInfo& server_info,
                                 JobManager* job_manager,
                                 toft::TimerManager* timer_manager)
    : ServerState(server_info, job_manager, timer_manager) {}

StemServerState::~StemServerState() {}

void StemServerState::TimeoutUpdate() {

}

} // namespace master
} // namespace gunir

