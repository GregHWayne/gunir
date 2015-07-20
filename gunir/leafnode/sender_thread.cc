// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <deque>
#include <vector>

#include "gunir/leafnode/sender_thread.h"

#include "thirdparty/glog/logging.h"

#include "gunir/leafnode/task_container.h"
#include "gunir/leafnode/sender_manager.h"


namespace gunir {
namespace leafnode {

SenderThread::SenderThread(SenderManager* sender_manager, int32_t thread_index)
    : ServerThread(thread_index),
      m_sender_manager(sender_manager) {
}

SenderThread::~SenderThread() {
}

void SenderThread::Entry() {
    TaskContainer *container = NULL;
    while (!IsStopRequested()) {
        if (!m_sender_manager->PopNextTask(&container)) {
            continue;
        }
        if (container->SendResult()) {
            UpdateTaskState(container->GetTaskInfo(), kCompletedTask);
        } else {
            UpdateTaskState(container->GetTaskInfo(), kFailedSendTask);
        }
        // delete here, new in leaf_server
        delete container;
    }
}

}  // namespace leafnode
}  // namespace gunir
