// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description: TaskWorker is a executor for inter_task
// InterContainer prepare input for TaskWorker
// After executor task in TaskWorker, the result will be
// sent to high server, or write to xfs if the inter_task is the
// highest task

#ifndef  GUNIR_STEMNODE_TASK_WORKER_H
#define  GUNIR_STEMNODE_TASK_WORKER_H

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "gunir/compiler/executor.h"
#include "gunir/proto/task.pb.h"
#include "gunir/proto/task.pb.h"
#include "gunir/types.h"

namespace gunir {
class Container;

namespace compiler {
class Executor;
} // namespace compiler
namespace io {
class Scanner;
} // namespace io
namespace stemnode {

class TaskWorker {
public:
    TaskWorker();
    ~TaskWorker();

    void Reset(const InterTaskSpec& spec);

    void Open(const std::vector<toft::StringPiece>& results);

    bool Run();

    bool Close();

    void FillReportTaskState(TaskState* task_state);

    void Clear();

private:
    void OpenResultContainer();

private:
    InterTaskSpec m_spec;
    toft::scoped_ptr<compiler::Executor> m_executor;
    std::vector<io::Scanner*> m_scanners;
    toft::scoped_ptr<Container> m_container;
};

}  // namespace stemnode
}  // namespace gunir

#endif  // GUNIR_STEMNODE_TASK_WORKER_H
