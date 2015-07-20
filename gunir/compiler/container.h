// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#ifndef  GUNIR_COMPILER_CONTAINER_H
#define  GUNIR_COMPILER_CONTAINER_H

#include <vector>

#include "gunir/proto/task.pb.h"

namespace gunir {
namespace io {
class Slice;
} // namespace io

class Container {
public:
    virtual ~Container() {}

    virtual bool Insert(const std::vector<io::Slice>& slices) = 0;

    virtual bool Close() = 0;

    virtual void FillReportTaskState(TaskState* task_state) = 0;

    virtual void Clear() = 0;

    virtual bool SendResult() = 0;
};

}  // namespace gunir

#endif  // GUNIR_COMPILER_CONTAINER_H
