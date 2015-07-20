// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef  GUNIR_LEAFNODE_TASK_WORKER_H
#define  GUNIR_LEAFNODE_TASK_WORKER_H

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/system/memory/mempool.h"

#include "gunir/proto/task.pb.h"
#include "gunir/compiler/executor.h"
#include "gunir/io/tablet_reader.h"

namespace gunir {

namespace io {
class TabletReader;
class TabletScanner;
}  // namespace io

namespace leafnode {

class TaskContainer;
class TaskContainerPool;

class TaskWorker {
public:
    explicit TaskWorker(TaskContainerPool* container_pool);
    ~TaskWorker();

    bool Reset(LeafTaskSpec* spec, MemPool* mempool);

    bool Open();

    bool Run();

    bool Close();

    int64_t GetReadBytes() const;

protected:
    bool OpenInputTabletReader();
    bool OpenResultContainer();

protected:
    LeafTaskSpec* m_spec;
    toft::scoped_ptr<compiler::Executor> m_executor;
    TaskContainerPool* m_container_pool;
    TaskContainer* m_container;
    std::vector<io::TabletReader*> m_tablet_readers;
    std::vector<io::TabletScanner*> m_scanners;
    int64_t m_read_bytes;
};
}  // namespace leafnode
}  // namespace gunir

#endif  // GUNIR_LEAFNODE_TASK_WORKER_H
