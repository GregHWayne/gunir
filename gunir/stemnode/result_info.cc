// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/stemnode/result_info.h"

#include <string>

namespace gunir {
namespace stemnode {

ResultInfo::ResultInfo()
    : m_size(0), m_data_holder(NULL),
      m_is_finished(false), m_next_sequence_id(0) {}

ResultInfo::~ResultInfo() {
    ClearResult();
}

uint32_t ResultInfo::GetSize() {
    return m_size;
}

bool ResultInfo::IsFinished() {
    return m_is_finished;
}

void ResultInfo::ReportResultSize(const ReportResultSizeRequest* request,
                                  ReportResultSizeResponse* response) {
    toft::RwLock::ReaderLocker locker(&m_rwlock);
    if (IsFinished()) {
        LOG(WARNING) << " The task is Completed :"
            << request->task_info().ShortDebugString();
        response->set_result(ReportResultSizeResponse::kReject);
    } else {
        response->set_result(ReportResultSizeResponse::kAccept);
        if (m_data_holder.get() == NULL) {
            m_size = request->size();
            if (m_size > 0) {
                m_data_holder.reset(new DataHolder(new char[m_size], m_size));
            }
        } else {
            CHECK_EQ(request->size(), m_size);
        }
    }
}

bool ResultInfo::AddResult(const ReportTaskResultRequest* request,
                           ReportTaskResultResponse* response,
                           bool* is_add) {
    *is_add = false;
    toft::RwLock::WriterLocker locker(&m_rwlock);
    if (IsFinished()) {
        LOG(WARNING) << " The task is Completed :"
            << request->task_info().ShortDebugString();
        response->set_result(ReportTaskResultResponse::kReject);
        return true;
    }

    response->set_result(ReportTaskResultResponse::kAccept);
    if (m_next_sequence_id != request->sequence_id()) {
        DCHECK_GT(m_next_sequence_id, request->sequence_id());
        LOG(WARNING) << " The sequence is Completed :"
            << request->task_info().ShortDebugString();
        return false;
    }

    const std::string& content = request->content();
    if (m_size != 0) {
        CHECK_NOTNULL(m_data_holder->Write(content.c_str(),
                                           content.length()));
    }
    m_next_sequence_id++;
    *is_add = true;
    m_is_finished = request->finished();
    return m_is_finished;
}

void ResultInfo::ClearResult() {
    toft::RwLock::WriterLocker locker(&m_rwlock);
    if (m_data_holder.get() != NULL) {
        m_data_holder->Reset();
        delete[] m_data_holder->GetStart();
    }
}

toft::StringPiece ResultInfo::GetResult() {
    toft::RwLock::WriterLocker locker(&m_rwlock);
    if (!IsFinished()) {
        return toft::StringPiece(static_cast<const char*>(NULL), 0);
    }
    CHECK(m_data_holder->Reserve(1) == NULL);
    return toft::StringPiece(m_data_holder->GetStart(),
                             m_data_holder->GetLength());
}

}  // namespace stemnode
}  // namespace gunir
