// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/master/job_state.h"

#include "toft/system/time/timestamp.h"

#include "gunir/utils/message_utils.h"

namespace gunir {
namespace master {

JobState::JobState(uint64_t job_id, const JobSpecification& spec)
        : m_job_id(job_id),
        m_job_spec(spec),
        m_status(kJobSubmitSucceed),
        m_failed_reason("") {
}

JobState::~JobState() {}

// void JobState::Init(meta::MetaServerClient* meta_client) {
//     m_meta_client.reset(meta_client);
// }

void JobState::SetJobStatus(const JobStatus& job_status,
                            const std::string& failed_reason) {
    {
        toft::RwLock::WriterLocker locker(&m_rwlock);
        VLOG(30) << "Job : " << m_job_id << " 's status changed : ["
            << m_status << "]";

        m_status = job_status;
        m_failed_reason = failed_reason;
    }
    if (IsJobFinished(m_status)) {
        AddHistoryQuery();
    }
}


void JobState::UpdateTaskState(const TaskState& task_state) {
    const TaskInfo& task_info = task_state.task_info();
    if (IsRootTask(task_info) && IsTaskCompleted(task_info)) {
        VLOG(10) << "Job : " << m_job_id << " finished now ";
        m_result_state.CopyFrom(task_state);
        SetJobStatus(kJobFinished);
    }
}

void JobState::GetJobResult(GetJobResultResponse* response) const {
    response->mutable_result()->set_job_status(GetJobStatus());
    response->mutable_result()->set_reason(GetFailedReason());
    response->mutable_result()->mutable_job_timestamp()
        ->CopyFrom(GetJobProgressTimeStamp());

    if (response->result().job_status() == kJobFinished) {
        for (int32_t i = 0; i < m_result_state.result_tablet_files_size();
             ++i) {
            response->mutable_result()->add_job_result_tablets(
                m_result_state.result_tablet_files(i));
        }
        response->mutable_result()->set_result_slice_number(
            m_result_state.result_slice_number());
        response->mutable_result()->set_result_file_size(
            m_result_state.result_file_size());
    }

    VLOG(30) << "Send get job result response["
        << response->ShortDebugString() << "]";
}

void JobState::AddHistoryQuery() {
    VLOG(30) << "Add histroy query : " << m_job_spec.query();
//     AddHistoryQueryRequest request;
//     AddHistoryQueryResponse response;
//     HistoryJob* history_job = request.add_history_job();
//     history_job->mutable_job_spec()->CopyFrom(m_job_spec);
//     history_job->set_job_id(m_job_id);

//     GetJobResultResponse result_response;
//     GetJobResult(&result_response);
//     history_job->mutable_result()->CopyFrom(result_response.result());
//     if (utils::kLocal == utils::GetSysRunningModel()) {
//         meta::SingletonMetaServer::Instance()
//             .AddHistoryQuery(&request, &response);
//     } else {
//         m_meta_client->AddHistoryQuery(&request, &response);
//     }
}

void JobState::SetJobStartTime(int64_t t) {
    toft::RwLock::WriterLocker locker(&m_rwlock);
    m_timestamp.set_job_start_time(t);
}

void JobState::SetJobAnalyseFinishTime(int64_t t) {
    toft::RwLock::WriterLocker locker(&m_rwlock);
    m_timestamp.set_job_analyse_finish_time(t);
}

void JobState::SetJobScheduleFinishTime(int64_t t) {
    toft::RwLock::WriterLocker locker(&m_rwlock);
    m_timestamp.set_job_schedule_finish_time(t);
}

void JobState::SetJobEmitFinishTime(int64_t t) {
    toft::RwLock::WriterLocker locker(&m_rwlock);
    m_timestamp.set_job_emit_finish_time(t);
}

inline void JobState::SetJobFinishTime(int64_t t) {
    toft::RwLock::WriterLocker locker(&m_rwlock);
    m_timestamp.set_job_finish_time(t);
}

}  // namespace master
}  // namespace gunir
