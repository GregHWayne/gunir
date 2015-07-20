// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/master/job_emitter.h"

#include "toft/base/closure.h"
#include "toft/system/threading/this_thread.h"
#include "trident/pbrpc.h"

#include "gunir/leafnode/leafnode_client.h"
#include "gunir/master/job_manager.h"
#include "gunir/master/leaf_server_state.h"
#include "gunir/master/server_manager.h"
#include "gunir/proto/proto_helper.h"
#include "gunir/stemnode/stemnode_client.h"
#include "gunir/types.h"

DECLARE_int64(gunir_blocking_queue_time);
DECLARE_int32(gunir_master_emit_retry_times);

DECLARE_int32(gunir_master_emit_thread_min_num);
DECLARE_int32(gunir_master_emit_thread_max_num);


namespace gunir {
namespace master {

JobEmitter::JobEmitter(JobManager* job_manager,
                       ServerManager* server_manager)
    : m_job_manager(job_manager),
      m_server_manager(server_manager),
      m_last_sequence_id(kSequenceIDStart),
      m_thread_pool(new toft::ThreadPool(FLAGS_gunir_master_emit_thread_min_num,
                                         FLAGS_gunir_master_emit_thread_max_num)) {

    stemnode::StemNodeClient::SetThreadPool(m_thread_pool.get());
    leafnode::LeafNodeClient::SetThreadPool(m_thread_pool.get());
    stemnode::StemNodeClient::SetOption();
    leafnode::LeafNodeClient::SetOption();
}

JobEmitter::~JobEmitter() {
    VLOG(10) << "Job emitter thread start exit now ";

    SendStopRequest();
    if (IsJoinable()) {
        Join();
    } else {
        VLOG(20) << "thread is not started";
    }

    VLOG(10) << "Job emitter thread exit succeed ";
}

void JobEmitter::Entry() {
    while (!IsStopRequested()) {
        EmitterPlan plan;
        if (m_job_manager->PopJobForEmitter(
                &plan, FLAGS_gunir_blocking_queue_time)) {
            EmitJob(plan);
        }
    }
}

void JobEmitter::EmitJob(const EmitterPlan& plan) {
    VLOG(10) << "Start emitter Job : " << plan.job_id() << "'s "
        << plan.ShortDebugString();

    const TaskTreeNode& node = plan.root_node();
    if (node.has_inter()) {
        EmitInterTask(node.inter());
    } else {
        DCHECK(node.has_leaf());
        EmitLeafTask(node.leaf());
    }
}

void JobEmitter::EmitInterTask(const InterNode& node) {
    const InterTaskSpec& spec = node.task_spec();
    std::string server_addr = spec.task_info().server_addr();
    TaskType type = spec.task_info().type();
    VLOG(20) << "Start emit inter task : " << spec.ShortDebugString();
    CHECK_EQ(kInterTask, type);

    PushInterTaskRequest* request = new PushInterTaskRequest;
    PushInterTaskResponse* response = new PushInterTaskResponse;
    request->set_sequence_id(m_last_sequence_id++);
    request->add_inter_spec()->CopyFrom(spec);

    toft::Closure<void (PushInterTaskRequest*, PushInterTaskResponse*, bool, int)>* done =
        toft::NewClosure(this, &JobEmitter::EmitInterTaskCallback,
                         node, FLAGS_gunir_master_emit_retry_times);

    stemnode::StemNodeClient node_client(server_addr);
    node_client.PushInterTask(request, response, done);
}

void JobEmitter::EmitLeafTask(const LeafNode& node) {
    const LeafTaskSpec& spec = node.task_spec();
    std::string server_addr = spec.task_info().server_addr();
    TaskType type = spec.task_info().type();
    CHECK_EQ(kLeafTask, type);

    VLOG(20) << "Start emit leaf task : " << spec.ShortDebugString();

    PushLeafTaskRequest* request = new PushLeafTaskRequest;
    PushLeafTaskResponse* response = new PushLeafTaskResponse;
    request->set_sequence_id(m_last_sequence_id++);
    request->add_leaf_spec()->CopyFrom(spec);

    toft::Closure<void (PushLeafTaskRequest*, PushLeafTaskResponse*, bool, int)>* done =
        toft::NewClosure(this, &JobEmitter::EmitLeafTaskCallback,
                         node, FLAGS_gunir_master_emit_retry_times);

    leafnode::LeafNodeClient node_client(server_addr);
    node_client.PushLeafTask(request, response, done);
}

void JobEmitter::EmitInterTaskCallback(InterNode node, int32_t retry,
                                       PushInterTaskRequest* request,
                                       PushInterTaskResponse* response,
                                       bool failed, int error_code) {
    if (failed || response->status() != kServerOk) {
        LOG(WARNING) << "fail to emit inter task, rpc status: "
            << StatusCodeToString(response->status());
        if (retry <= 0 || !RpcChannelHealth(error_code)) {
            LOG(ERROR) << "fail to emit inter task after " << FLAGS_gunir_master_emit_retry_times
                << ", rpc status: " << StatusCodeToString(response->status());
            delete request;
            delete response;
            m_job_manager->EmitTaskFailed(node.task_spec().task_info());
        } else {
            int64_t wait_time = FLAGS_gunir_master_emit_retry_times *
                (FLAGS_gunir_master_emit_retry_times - retry);
            toft::ThisThread::Sleep(wait_time);

            std::string server_addr = node.task_spec().task_info().server_addr();
            toft::Closure<void (PushInterTaskRequest*, PushInterTaskResponse*, bool, int)>* done =
                toft::NewClosure(this, &JobEmitter::EmitInterTaskCallback,
                                 node, retry - 1);

            stemnode::StemNodeClient node_client(server_addr);
            node_client.PushInterTask(request, response, done);
        }
        return;
    }
    VLOG(20) << " Emit inter task succeed : "
        << node.task_spec().ShortDebugString();

    if (!m_job_manager->EmitTaskSucceed(node.task_spec().task_info())) {
        return;
    }

    for (int32_t i = 0; i < node.child_size(); ++i) {
        const TaskTreeNode& task_node = node.child(i);
        if (task_node.has_leaf()) {
            CHECK(!task_node.has_inter());
            EmitLeafTask(task_node.leaf());
        } else {
            CHECK(task_node.has_inter());
            EmitInterTask(task_node.inter());
        }
    }
}

void JobEmitter::EmitLeafTaskCallback(LeafNode node, int32_t retry,
                                      PushLeafTaskRequest* request,
                                      PushLeafTaskResponse* response,
                                      bool failed, int error_code) {
    if (failed || response->status() != kServerOk) {
        LOG(WARNING) << "fail to emit leaf task, rpc status: "
            << StatusCodeToString(response->status());
        if (retry <= 0 || !RpcChannelHealth(error_code)) {
            LOG(ERROR) << "fail to emit leaf task after " << FLAGS_gunir_master_emit_retry_times
                << ", rpc status: " << StatusCodeToString(response->status());
            delete request;
            delete response;
            m_job_manager->EmitTaskFailed(node.task_spec().task_info());
        } else {
            int64_t wait_time = FLAGS_gunir_master_emit_retry_times *
                (FLAGS_gunir_master_emit_retry_times - retry);
            toft::ThisThread::Sleep(wait_time);

            std::string server_addr = node.task_spec().task_info().server_addr();
            toft::Closure<void (PushLeafTaskRequest*, PushLeafTaskResponse*, bool, int)>* done =
                toft::NewClosure(this, &JobEmitter::EmitLeafTaskCallback,
                                 node, retry - 1);

            leafnode::LeafNodeClient node_client(server_addr);
            node_client.PushLeafTask(request, response, done);
        }
        return;
    }
    m_job_manager->EmitTaskSucceed(node.task_spec().task_info());
}

bool JobEmitter::RpcChannelHealth(int32_t err_code) {
    VLOG(20) << "rpc error code: " << trident::RpcErrorCodeToString(err_code);
    return err_code != trident::RPC_ERROR_CONNECTION_CLOSED
        && err_code != trident::RPC_ERROR_SERVER_SHUTDOWN
        && err_code != trident::RPC_ERROR_SERVER_UNREACHABLE
        && err_code != trident::RPC_ERROR_SERVER_UNAVAILABLE
        && err_code != trident::RPC_ERROR_FOUND_SERVICE;
}

}  // namespace master
}  // namespace gunir
