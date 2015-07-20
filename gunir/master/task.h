// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_MASTER_TASK_H
#define  GUNIR_MASTER_TASK_H

#include <stdint.h>

#include <string>

#include "toft/system/threading/rwlock.h"

#include "gunir/proto/task.pb.h"

namespace gunir {
namespace master {

class Task {
public:
    Task();

    Task(TaskType type, uint64_t job_id, uint32_t task_id);

    ~Task();

    void SetServerID(const uint32_t& id) {
        toft::RwLock::WriterLocker locker(&m_rwlock);
        m_task_info.set_server_id(id);
    }

    void SetServerAddr(const std::string& addr) {
        toft::RwLock::WriterLocker locker(&m_rwlock);
        m_task_info.set_server_addr(addr);
    }

    void SetParentTaskID(const uint32_t& id) {
        toft::RwLock::WriterLocker locker(&m_rwlock);
        m_parent_task_id = id;
    }

    uint32_t GetParentTaskID() const {
        toft::RwLock::ReaderLocker locker(&m_rwlock);
        return m_parent_task_id;
    }

    void SetTaskStatus(const TaskStatus& status) {
        toft::RwLock::WriterLocker locker(&m_rwlock);
        m_task_info.set_task_status(status);
    }


    TaskInfo GetTaskInfo() const {
        toft::RwLock::ReaderLocker locker(&m_rwlock);
        return m_task_info;
    }

    void AddAttemptID() {
        m_task_info.set_attempt_id(m_cur_attempt_id);
        ++m_cur_attempt_id;
    }

protected:
    void DefaultTaskInfo();

    void SetBaseInfo(TaskType type, uint64_t job_id, uint32_t task_id);

protected:
    mutable toft::RwLock m_rwlock;
    TaskInfo m_task_info;
    uint32_t m_cur_attempt_id;
    uint32_t m_parent_task_id;
};

}  // namespace master
}  // namespace gunir

#endif  // GUNIR_MASTER_TASK_H
