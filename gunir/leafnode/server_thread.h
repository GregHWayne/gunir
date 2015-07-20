// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_LEAF_SERVER_LEAF_SERVER_THREAD_H
#define  GUNIR_LEAF_SERVER_LEAF_SERVER_THREAD_H

#include "toft/base/scoped_ptr.h"
#include "toft/base/uncopyable.h"
#include "toft/system/threading/base_thread.h"
#include "toft/system/threading/mutex.h"
#include "toft/system/threading/rwlock.h"

#include "gunir/proto/master_rpc.pb.h"
#include "gunir/proto/task.pb.h"

namespace gunir {
namespace leafnode {

class ServerThread : public toft::BaseThread {
    DECLARE_UNCOPYABLE(ServerThread);

public:
    explicit ServerThread(int32_t thread_index);

    virtual ~ServerThread();

    virtual void FillReportRequest(ReportRequest *request);

    virtual void UpdateTaskState(const TaskInfo& task_info,
                                 const TaskStatus& task_status);

protected:
    virtual void Entry() {}

    virtual void CancelTaskInQueue() {}

protected:
    mutable toft::RwLock m_rwlock;
    int32_t m_thread_index;
    ReportRequest m_request;
};

}  // namespace leafnode
}  // namespace gunir

#endif  // GUNIR_LEAF_SERVER_LEAF_SERVER_THREAD_H

