// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_LEAFNODE_SENDER_MANAGER_H
#define  GUNIR_LEAFNODE_SENDER_MANAGER_H

#include "toft/base/scoped_ptr.h"
#include "toft/base/uncopyable.h"

#include "gunir/proto/master_rpc.pb.h"

namespace gunir {

class TaskState;

namespace leafnode {

class SenderThread;
class TaskContainer;
class TaskContainerPool;

class SenderManager {
    DECLARE_UNCOPYABLE(SenderManager);

public:
    SenderManager(TaskContainerPool* container_pool, int send_thread_number);
    ~SenderManager();

    void Init();

    bool PopNextTask(TaskContainer** container);

    void FillReportRequest(ReportRequest *request);

    void CancelTask(const ReportResponse& response);

private:
    void JoinThread();

protected:
    TaskContainerPool* m_container_pool;
    int m_sender_thread_number;

    toft::scoped_array<SenderThread*> m_sender_threads;
};

}  // namespace leafnode
}  // namespace gunir

#endif  // GUNIR_LEAFNODE_SENDER_MANAGER_H

