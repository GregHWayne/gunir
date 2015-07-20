// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_LEAFNODE_TASK_CONTAINER_H
#define  GUNIR_LEAFNODE_TASK_CONTAINER_H

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/system/memory/mempool.h"

#include "gunir/compiler/container.h"
#include "gunir/io/data_holder.h"
#include "gunir/proto/stemnode_rpc.pb.h"

namespace gunir {
namespace io {
class Slice;
} // namespace io

namespace leafnode {

class TaskContainer : public Container {
public:
    explicit TaskContainer();
    virtual ~TaskContainer();

    friend class TaskContainerTest;

    void Init(const TaskInfo& task_info,
              const TaskInfo& parent_task_info);

    bool Insert(const std::vector<io::Slice>& slices);

    bool Close();

    void Clear();

    void FillReportTaskState(TaskState* task_state);

    bool SendResult();

    int64_t GetContainerSize() {
        return m_total_size;
    }

    TaskInfo GetTaskInfo() {
        return m_task_info;
    }

    bool ShouldAddSlice(uint64_t size, uint64_t max_size) {
        return (m_request_size + size + m_total_size) <= max_size;
    }

private:
    bool Insert(const io::Slice& slice);
    void NewRequest(uint32_t content_size);
    void CloseRequest(bool is_last_request);

    bool DumpResult();
    void AddRequest();

    void SendResultCallback(ReportTaskResultRequest* request,
                            ReportTaskResultResponse* response,
                            bool failed, int error_code);
    uint64_t GetContentSize();
    bool SendResultSize();

private:
    TaskInfo m_task_info;
    TaskInfo m_parent_task_info;

    uint64_t m_sequence_id;
    uint32_t m_request_size;
    ReportTaskResultRequest* m_request;

    std::vector<ReportTaskResultRequest*> m_requests_vec;
    int64_t m_total_size;

    char* m_buffer;
    uint32_t m_buffer_size;
    toft::scoped_ptr<DataHolder> m_data_holder;
    static const uint32_t kBufferSize = 1024 * 1024;
};

} // namespace leafnode
} // namespace gunir

#endif  // GUNIR_LEAFNODE_TASK_CONTAINER_H
