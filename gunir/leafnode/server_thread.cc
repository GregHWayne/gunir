// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/leafnode/server_thread.h"

#include "thirdparty/glog/logging.h"

namespace gunir {
namespace leafnode {

ServerThread::ServerThread(int32_t thread_index)
    : m_thread_index(thread_index) {
}

ServerThread::~ServerThread() {
}

void ServerThread::UpdateTaskState(const TaskInfo& task_info,
                                       const TaskStatus& task_status) {
    toft::RwLock::WriterLocker locker(&m_rwlock);
    TaskState task_state;
    task_state.mutable_task_info()->CopyFrom(task_info);
    task_state.mutable_task_info()->set_task_status(task_status);
    m_request.add_task_state()->CopyFrom(task_state);
}

void ServerThread::FillReportRequest(ReportRequest *request) {
    toft::RwLock::WriterLocker locker(&m_rwlock);
    request->mutable_task_state()->MergeFrom(m_request.task_state());
    m_request.Clear();
}

}  // namespace leafnode
}  // namespace gunir
