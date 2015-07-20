// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_STEMNODE_TASK_CONTAINER_H
#define  GUNIR_STEMNODE_TASK_CONTAINER_H

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"

#include "gunir/compiler/container.h"
#include "gunir/proto/task.pb.h"
// #include "gunir/proto/stemnode_rpc.pb.h"

namespace gunir {
namespace io {
class Slice;
class SliceWriter;
} // namespace io
namespace stemnode {

class TaskContainer : public Container {
public:
    explicit TaskContainer(io::SliceWriter* writer = NULL);
    ~TaskContainer();

    bool Init(const InterTaskSpec& spec);

    bool Insert(const std::vector<io::Slice>& slices);

    bool Close();

    void FillReportTaskState(TaskState* task_state);

    bool SendResult() {
        return true;
    }

    void Clear() {}

private:
    toft::scoped_ptr<io::SliceWriter> m_writer;
    std::vector<std::string> m_output_files;
    InterTaskSpec m_spec;
    int64_t m_slice_number;
    uint32_t m_result_size;
    bool m_result_too_large;
};

}  // namespace stemnode
}  // namespace gunir

#endif  // GUNIR_STEMNODE_TASK_CONTAINER_H
