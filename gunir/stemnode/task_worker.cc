// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/stemnode/task_worker.h"

#include "gunir/leafnode/task_container.h"
#include "gunir/stemnode/task_container.h"
#include "gunir/io/slice_scanner.h"
#include "gunir/utils/message_utils.h"

namespace gunir {
namespace stemnode {

TaskWorker::TaskWorker() {}

TaskWorker::~TaskWorker() {
    Clear();
}

void TaskWorker::Reset(const InterTaskSpec& spec) {
    m_spec = spec;

    Clear();
    m_executor.reset(new compiler::Executor());
    compiler::PlanProto proto = m_spec.task_input().exec_plan();

    CHECK(m_executor->Init(
        proto, m_spec.task_input().result_proto(),
        m_spec.task_input().result_message()));
}

void TaskWorker::Clear() {
    if (m_container.get() != NULL) {
        m_container->Clear();
    }
    m_executor.reset();
    for (uint32_t i = 0; i < m_scanners.size(); ++i) {
        delete m_scanners[i];
    }
    m_scanners.clear();
}

void TaskWorker::Open(const std::vector<toft::StringPiece>& results) {
    // set container
    OpenResultContainer();

    // set scanner
    for (size_t i = 0; i < results.size(); ++i) {
        io::SliceScanner* scanner = new io::SliceScanner(results[i]);
        m_scanners.push_back(static_cast<io::Scanner*>(scanner));
    }
}

void TaskWorker::OpenResultContainer() {
    if (IsRootTask(m_spec.task_info())) {
        stemnode::TaskContainer* root_task_container = new stemnode::TaskContainer();
        root_task_container->Init(m_spec);
        m_container.reset(root_task_container);
    } else {
        leafnode::TaskContainer* task_container = new leafnode::TaskContainer();
        task_container->Init(m_spec.task_info(), m_spec.parent_task_info());
        m_container.reset(task_container);
    }
}

bool TaskWorker::Run() {
    m_executor->SetScanner(m_scanners);
    m_executor->SetContainer(m_container.get());
    return m_executor->Run();
}

bool TaskWorker::Close() {
    if (!m_container->Close()) {
        return false;
    }
    return m_container->SendResult();
}

void TaskWorker::FillReportTaskState(TaskState* task_state) {
    m_container->FillReportTaskState(task_state);
}

}  // namespace stemnode
}  // namespace gunir
