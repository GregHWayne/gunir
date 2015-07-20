// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_MASTER_JOB_STATE_H
#define  GUNIR_MASTER_JOB_STATE_H

#include <string>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/number.h"
#include "toft/system/threading/rwlock.h"

#include "gunir/proto/job.pb.h"
#include "gunir/proto/task.pb.h"
#include "gunir/proto/master_rpc.pb.h"

namespace gunir {
namespace master {

class JobState {
public:
    JobState(uint64_t job_id, const JobSpecification& job_spec);
    ~JobState();

//     void Init(meta::MetaServerClient* meta_client);
    void SetJobStatus(const JobStatus& job_status,
                      const std::string& failed_reason = "");
    JobStatus GetJobStatus() const;
    std::string GetFailedReason() const;

    void SetJobStartTime(int64_t t);
    void SetJobAnalyseFinishTime(int64_t t);
    void SetJobScheduleFinishTime(int64_t t);
    void SetJobEmitFinishTime(int64_t t);

    int64_t GetJobStartTime() const;
    int64_t GetJobFinishTime() const;
    const JobSpecification& GetJobSpec() const;
    uint64_t GetJobID() const;

    JobProgressTimeStamp GetJobProgressTimeStamp() const;
    void GetJobResult(GetJobResultResponse* response) const;
    void UpdateTaskState(const TaskState& task_state);

private:
    void SetJobFinishTime(int64_t t);
    void AddHistoryQuery();

private:
    mutable toft::RwLock m_rwlock;

    const uint64_t m_job_id;
    const JobSpecification m_job_spec;

    JobStatus m_status;
    std::string m_failed_reason;
    JobProgressTimeStamp m_timestamp;
    TaskState m_result_state;
};

inline JobStatus JobState::GetJobStatus() const {
    toft::RwLock::ReaderLocker locker(&m_rwlock);
    return m_status;
}

inline std::string JobState::GetFailedReason() const {
    toft::RwLock::ReaderLocker locker(&m_rwlock);
    return m_failed_reason;
}

inline int64_t JobState::GetJobStartTime() const {
    toft::RwLock::ReaderLocker locker(&m_rwlock);
    return m_timestamp.job_start_time();
}

inline int64_t JobState::GetJobFinishTime() const {
    toft::RwLock::ReaderLocker locker(&m_rwlock);
    return m_timestamp.job_finish_time();
}

inline JobProgressTimeStamp JobState::GetJobProgressTimeStamp() const {
    toft::RwLock::ReaderLocker locker(&m_rwlock);
    return m_timestamp;
}

inline uint64_t JobState::GetJobID() const {
    return m_job_id;
}

inline const JobSpecification& JobState::GetJobSpec() const {
    return m_job_spec;
}
}  // namespace master
}  // namespace gunir

#endif  // GUNIR_MASTER_JOB_STATE_H
