// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_MASTER_JOB_EMITTER_H
#define  GUNIR_MASTER_JOB_EMITTER_H

#include "toft/base/scoped_ptr.h"
#include "toft/base/uncopyable.h"
#include "toft/system/atomic/atomic.h"
#include "toft/system/threading/base_thread.h"
#include "toft/system/threading/thread_pool.h"

#include "gunir/master/scheduler_plan.pb.h"
#include "gunir/proto/leafnode_rpc.pb.h"
#include "gunir/proto/stemnode_rpc.pb.h"

namespace gunir {
namespace master {

class JobManager;
class ServerManager;

class JobEmitter : public toft::BaseThread {
    DECLARE_UNCOPYABLE(JobEmitter);

public:
    JobEmitter(JobManager* job_manager,
               ServerManager* server_manager);
    virtual ~JobEmitter();

protected:
    void Entry();
    void EmitJob(const EmitterPlan& plan);

    void EmitInterTask(const InterNode& node);
    void EmitLeafTask(const LeafNode& node);

    void EmitInterTaskCallback(InterNode node, int32_t retry,
                               PushInterTaskRequest* request,
                               PushInterTaskResponse* response,
                               bool failed, int error_code);
    void EmitLeafTaskCallback(LeafNode node, int32_t retry,
                              PushLeafTaskRequest* request,
                              PushLeafTaskResponse* response,
                              bool failed, int error_code);
    bool RpcChannelHealth(int32_t err_code);

private:
    JobManager* m_job_manager;
    ServerManager* m_server_manager;

    toft::Atomic<uint64_t> m_last_sequence_id;
    toft::scoped_ptr<toft::ThreadPool> m_thread_pool;
};

}  // namespace master
}  // namespace gunir

#endif  // GUNIR_MASTER_JOB_EMITTER_H
