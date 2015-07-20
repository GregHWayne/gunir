// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/stemnode/result_manager.h"

#include "thirdparty/gflags/gflags.h"

#include "gunir/stemnode/result_info.h"

DECLARE_double(gunir_job_precision);

namespace gunir {
namespace stemnode {

ResultManager::ResultManager()
    : m_finished_child(0), m_least_child_number(0) {}

ResultManager::~ResultManager() {
    ClearResultInfo();
}

void ResultManager::Reset(const InterTaskSpec& spec) {
    toft::RwLock::WriterLocker locker(&m_rwlock);
    m_inter_task_spec.CopyFrom(spec);
    m_finished_child = 0;
    InitResultInfo(spec.task_input());
    m_least_child_number = m_infos.size();
    if (spec.task_input().has_task_precision()
        && ((spec.task_input().child_type() == kLeafTask)
            || (FLAGS_gunir_job_precision > 1))) {
        m_least_child_number *= spec.task_input().task_precision();
    }
}

void ResultManager::ReportResultSize(const ReportResultSizeRequest* request,
                                     ReportResultSizeResponse* response) {
    ResultInfo* info = GetResultInfo(request->task_info().task_id());
    if (info == NULL) {
        response->set_result(ReportResultSizeResponse::kReject);
        return;
    }
    info->ReportResultSize(request, response);
}

bool ResultManager::AddAndJudgeIsFull(const ReportTaskResultRequest* request,
                                       ReportTaskResultResponse* response,
                                       bool* is_add) {
    ResultInfo* info = GetResultInfo(request->task_info().task_id());
    if (info == NULL) {
        response->set_result(ReportTaskResultResponse::kReject);
        return false;
    }

    if (info->AddResult(request, response, is_add) && *is_add) {
        toft::RwLock::WriterLocker locker(&m_rwlock);
        ++m_finished_child;
        return IsFinished();
    }
    return false;
}

bool ResultManager::IsFinished() {
    return m_finished_child >= m_least_child_number;
}

void ResultManager::GetAllResult(std::vector<toft::StringPiece>* vec) {
    vec->clear();
    std::map<uint32_t, ResultInfo*>::iterator iter;
    for (iter = m_infos.begin(); iter != m_infos.end(); ++iter) {
        if (iter->second->GetSize() > 0) {
            vec->push_back(iter->second->GetResult());
        } else {
            vec->push_back(toft::StringPiece());
        }
    }
}

ResultInfo* ResultManager::GetResultInfo(uint32_t id) {
    std::map<uint32_t, ResultInfo*>::iterator iter;
    iter = m_infos.find(id);
    if (iter == m_infos.end()) {
        return NULL;
    }
    return iter->second;
}

void ResultManager::InitResultInfo(const InterTaskInput& input) {
    ClearResultInfo();
    for (int32_t i = 0; i < input.child_task_id_size(); ++i) {
        m_infos[input.child_task_id(i)] = new ResultInfo();
    }
}

void ResultManager::ClearResultInfo() {
    std::map<uint32_t, ResultInfo*>::iterator iter;
    for (iter = m_infos.begin(); iter != m_infos.end(); ++iter) {
        delete iter->second;
    }
    m_infos.clear();
    m_finished_child = 0;
    m_least_child_number = 0;
}

}  // namespace stemnode
}  // namespace gunir
