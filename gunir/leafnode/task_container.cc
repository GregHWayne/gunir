// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/leafnode/task_container.h"

#include <string>

#include "toft/base/closure.h"
#include "toft/base/string/number.h"

#include "gunir/io/slice.h"
#include "gunir/proto/proto_helper.h"
#include "gunir/stemnode/stemnode_client.h"
#include "gunir/types.h"
#include "gunir/utils/filename_tool.h"

DECLARE_int32(gunir_leafnode_send_package_size);
DECLARE_int64(gunir_leafnode_result_memory_limit);

namespace gunir {
namespace leafnode {

TaskContainer::TaskContainer()
    : m_sequence_id(0),
      m_request_size(0),
      m_request(NULL),
      m_total_size(0),
      m_buffer(NULL) {}

TaskContainer::~TaskContainer() {
    Clear();
    if (m_request != NULL) {
        delete m_request;
    }
}

void TaskContainer::Init(const TaskInfo& task_info,
                         const TaskInfo& parent_task_info) {
    m_task_info = task_info;
    m_parent_task_info = parent_task_info;

    m_sequence_id = kSequenceIDStart;

    Clear();
    NewRequest(static_cast<uint32_t>(FLAGS_gunir_leafnode_send_package_size));
}

bool TaskContainer::Insert(const std::vector<io::Slice>& slices) {
    for (uint i = 0; i < slices.size(); ++i) {
        if (!Insert(slices[i])) {
            return false;
        }
    }
    return true;
}

bool TaskContainer::Insert(const io::Slice& slice) {
    uint32_t slice_size = slice.ByteSize();
    uint32_t value_len = 0;

    if (!ShouldAddSlice(slice_size, FLAGS_gunir_leafnode_result_memory_limit)) {
        LOG(ERROR) << "Inter task result out of memory : "
            << FLAGS_gunir_leafnode_result_memory_limit
            << " < " << m_total_size + slice_size + m_request_size;
        return false;
    }

    if (slice_size > static_cast<uint32_t>(FLAGS_gunir_leafnode_send_package_size)) {
        LOG(ERROR) << "one slice is too big";
        CloseRequest(false);
        NewRequest(slice_size);
    }

    if (m_buffer_size < slice_size) {
        CloseRequest(false);
        NewRequest(static_cast<uint32_t>(FLAGS_gunir_leafnode_send_package_size));
    }

    if (!slice.SerializeToString(m_buffer + m_request_size,
                                 m_buffer_size,
                                 &value_len)) {
        LOG(ERROR) << "Slice serialize to string failed";
        return false;
    }

    m_buffer_size -= value_len;
    m_request_size += value_len;

    return true;
}

void TaskContainer::FillReportTaskState(TaskState* task_state) {
    CHECK(false) << "Not allowed to call FillReportTaskState.";
    return;
}

bool TaskContainer::Close() {
    CloseRequest(true);
    return true;
}

bool TaskContainer::SendResult() {
    LOG(INFO) << m_task_info.ShortDebugString() << ": start send result.";
    bool ret = true;

    if (!SendResultSize()) {
        return false;
    }

    if (m_requests_vec.size() > 0) {
        CHECK(m_requests_vec[m_requests_vec.size() - 1]->finished());
        std::string server_addr = m_requests_vec[0]->parent_task_info().server_addr();
        for (uint32_t i = 0; i < m_requests_vec.size(); ++i) {
            ReportTaskResultResponse *response = new ReportTaskResultResponse;

            toft::Closure<void (ReportTaskResultRequest*, ReportTaskResultResponse*, bool, int)>* done =
                toft::NewClosure(this, &TaskContainer::SendResultCallback);

            stemnode::StemNodeClient node_client(server_addr);
            node_client.ReportTaskResult(m_requests_vec[i], response, done);
        }
//         if (utils::kLocal == utils::GetSysRunningModel()) {
//             inter::LocalSend::Instance().Send(m_requests_vec);
//         } else {
//             ret = inter::RemoteSend::Instance().Send(m_requests_vec);
//         }
    }
    Clear();

    LOG(INFO) << m_task_info.ShortDebugString() << " finish send result.";
    return ret;
}

void TaskContainer::SendResultCallback(ReportTaskResultRequest* request,
                                       ReportTaskResultResponse* response,
                                       bool failed, int error_code) {
    if (failed) {
        LOG(ERROR) << "send result failed: " << request->ShortDebugString();
    }
    bool is_accept = (response->result() == ReportTaskResultResponse::kAccept);
    LOG(INFO) << "send result success, " << (is_accept? "kAccept":"kReject");
//     delete request;
    delete response;
}

bool TaskContainer::SendResultSize() {
    if (m_requests_vec.size() == 0) {
        return false;
    }
    uint64_t content_size = GetContentSize();
    CHECK(content_size > 0);
    ReportResultSizeRequest request;
    ReportResultSizeResponse response;
    request.mutable_parent_task_info()->CopyFrom(m_requests_vec[0]->parent_task_info());
    request.mutable_task_info()->CopyFrom(m_requests_vec[0]->task_info());
    request.set_size(content_size);
    request.set_sequence_id(m_sequence_id++);

    std::string server_addr = m_requests_vec[0]->parent_task_info().server_addr();
    stemnode::StemNodeClient node_client(server_addr);
    if (!node_client.ReportResultSize(&request, &response, NULL)) {
        LOG(ERROR) << "rpc fail to [" << server_addr << "], status: "
            << StatusCodeToString(response.status());
        return false;
    }

    if (response.result() != ReportResultSizeResponse::kAccept) {
        LOG(WARNING) << "send result to stemnode, refused: "
            << m_requests_vec[0]->task_info().ShortDebugString();
    }
    return true;
}

void TaskContainer::NewRequest(uint32_t content_size) {
    VLOG(10) << "NewRequest(): " << content_size;
    CHECK(NULL == m_request);
    m_buffer_size = content_size;
    m_request_size = 0;
    m_request = new ReportTaskResultRequest();
    m_request->mutable_task_info()->CopyFrom(m_task_info);
    m_request->mutable_parent_task_info()->CopyFrom(m_parent_task_info);
    m_request->set_sequence_id(m_sequence_id++);
    m_request->set_finished(false);
    m_request->mutable_content()->resize(content_size);
    m_buffer = const_cast<char*>(m_request->mutable_content()->c_str());
}

void TaskContainer::CloseRequest(bool is_last_request) {
    m_request->set_finished(is_last_request);
    AddRequest();
    DumpResult();
    m_request = NULL;
}

void TaskContainer::Clear() {
    for (size_t i = 0; i < m_requests_vec.size(); ++i) {
        delete m_requests_vec[i];
    }
    m_requests_vec.clear();
    m_total_size = 0;
}

void TaskContainer::AddRequest() {
    CHECK_EQ(m_request->sequence_id(), m_requests_vec.size());
    m_request->mutable_content()->resize(m_request_size);
    m_requests_vec.push_back(m_request);
    m_total_size += m_request_size;
    CHECK_GE(FLAGS_gunir_leafnode_result_memory_limit, m_total_size);
}

uint64_t TaskContainer::GetContentSize() {
    uint64_t sum = 0;
    for (size_t i = 0; i < m_requests_vec.size(); ++i) {
        sum += ((m_requests_vec[i])->mutable_content())->size();
    }
    VLOG(10) << "GetContentSize(): " << m_requests_vec.size()
        << ", sum_size: " << sum;
    return sum;
}

bool TaskContainer::DumpResult() {
//     LOG(INFO) << m_request->task_info().ShortDebugString()
//         << " start dump result.";
//     if (!FLAGS_gunir_enable_task_cache) {
//         return true;
//     }

//     std::string file_name = GetCacheTaskFileName(m_request->task_info())
//         + "_" + NumberToString(m_request->sequence_id());
//     scoped_ptr<File> file;
//     file.reset(File::Open(file_name.c_str(), File::ENUM_FILE_OPEN_MODE_W));

//     if (file.get() == NULL) {
//         LOG(ERROR) << "Create file failed :" << file_name;
//         return false;
//     }

//     std::string* str = m_request->mutable_content();
//     int64_t ret = file->Write(str->c_str(), str->length());

//     if (ret < 0) {
//         LOG(ERROR) << "Write request to file error." << file_name;
//         return false;
//     }
//     LOG(INFO) << m_request->task_info().ShortDebugString()
//         << " dump result size: " << ret;
//     ret = file->Close();
//     if (ret < 0) {
//         LOG(ERROR) << "Close file error." << file_name;
//         return false;
//     }
//     LOG(INFO) << m_request->task_info().ShortDebugString()
//         << " finish dump result.";
    return true;
}

} // namespace leafnode
} // namespace gunir
