// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_LEAFNODE_TASK_CONTAINER_POOL_H
#define  GUNIR_LEAFNODE_TASK_CONTAINER_POOL_H

#include <deque>

#include "toft/system/threading/mutex.h"

#include "gunir/leafnode/task_container.h"

namespace gunir {
namespace leafnode {

class TaskContainerPool {
public:
    TaskContainerPool()
        : m_total_size(0) {}
    ~TaskContainerPool() {
        TaskContainer* container = NULL;
        while (PopNextContainer(&container)) {
            delete container;
        }
    }

    TaskContainer* NewContainer() {
        return new TaskContainer();
    }

    void AddContainer(TaskContainer* container) {
        toft::MutexLocker lock(&m_mutex);
        m_total_size += container->GetContainerSize();
        m_container_queue.push_back(container);
    }

    bool PopNextContainer(TaskContainer** container) {
        toft::MutexLocker lock(&m_mutex);
        if (m_container_queue.empty()) {
            return false;
        }
        *container = m_container_queue.front();
        m_container_queue.pop_front();
        m_total_size -= (*container)->GetContainerSize();
        return true;
    }

    int64_t GetContainerSize() {
        toft::MutexLocker lock(&m_mutex);
        return m_total_size;
    }

private:
    mutable toft::Mutex m_mutex;
    std::deque<TaskContainer*> m_container_queue;
    int64_t m_total_size;
};

}  // namespace leafnode
}  // namespace gunir

#endif  // GUNIR_LEAFNODE_TASK_CONTAINER_POOL_H
