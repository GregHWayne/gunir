// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#ifndef GUNIR_MASTER_STEM_SERVER_STATE_H
#define GUNIR_MASTER_STEM_SERVER_STATE_H

#include "gunir/master/server_state.h"

#include "toft/system/timer/timer_manager.h"

#include "gunir/proto/server_info.pb.h"

namespace gunir {
namespace master {

class JobManager;

class StemServerState : public ServerState {
public:
    StemServerState(const ServerInfo& server_info,
                    JobManager* job_manager,
                    toft::TimerManager* timer_manager);
    virtual ~StemServerState();

protected:
    virtual void TimeoutUpdate();
};

} // namespace master
} // namespace gunir

#endif // GUNIR_MASTER_STEM_SERVER_STATE_H
