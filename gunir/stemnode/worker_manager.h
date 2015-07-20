// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef GUNIR_STEMNODE_WORKER_MANAGER_H
#define GUNIR_STEMNODE_WORKER_MANAGER_H

#include <list>
#include <map>
#include <vector>

#include "toft/system/threading/rwlock.h"

#include "gunir/proto/master_rpc.pb.h"
#include "gunir/proto/stemnode_rpc.pb.h"
#include "gunir/proto/task.pb.h"

namespace gunir {
namespace stemnode {

class TaskInfoCmp {
public :
    bool operator()(const TaskInfo& lhs, const TaskInfo& rhs)
        const {
            if (lhs.task_id() != rhs.task_id()) {
                return lhs.task_id() < rhs.task_id();
            } else if (lhs.job_id() != rhs.job_id()) {
                return lhs.job_id() < rhs.job_id();
            } else {
                return lhs.attempt_id()
                    < rhs.attempt_id();
            }
        }
};

class WorkerThread;

class WorkerManager {
    typedef ::google::protobuf::RepeatedPtrField<InterTaskSpec> InterTaskSpecList;
    typedef ::google::protobuf::RepeatedPtrField<uint64_t> FinishJobIdList;

public:
    WorkerManager(uint32_t slot_num);
    ~WorkerManager();

    void AddTask(const InterTaskSpecList& task_spec_list);
    void CancelTask(const FinishJobIdList& id_list);

    void ReportResultSize(const ReportResultSizeRequest* request,
                          ReportResultSizeResponse* response);
    void ReportTaskResult(const ReportTaskResultRequest* request,
                          ReportTaskResultResponse* response);

    void FillReportRequest(ReportRequest* request);

private:
    void InitWorkerThreads(uint32_t slot_num);
    void JoinThreads();

    uint32_t FindTask(const TaskInfo& task_info);


private:
    mutable toft::RwLock m_rwlock;
    uint32_t m_thread_num;

    std::vector<WorkerThread*> m_worker_threads;
    std::list<uint32_t> m_idle_thread_list;
    std::map<TaskInfo, uint32_t, TaskInfoCmp> m_running_task_map;
};

} // namespace stemnode
} // namespace gunir

#endif // GUNIR_STEMNODE_WORKER_MANAGER_H
