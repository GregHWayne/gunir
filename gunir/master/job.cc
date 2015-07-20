// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/master/job.h"

#include "toft/system/time/timestamp.h"

#include "gunir/compiler/compiler_utils.h"
#include "gunir/master/inter_task.h"
#include "gunir/master/leaf_task.h"
#include "gunir/master/task_manager.h"
#include "gunir/utils/message_utils.h"

namespace gunir {
namespace master {

Job::Job(const JobSpecification& spec, uint64_t job_id)
    : m_job_state(new JobState(job_id, spec)) {
}

Job::~Job() {
    VLOG(10) << "Job [ " << GetJobID() << "] exit now ";
}

bool Job::Init(const JobAnalyserResult& analyser_result) {
    const compiler::TaskPlanProto& task_plan = analyser_result.task_plan();
    if (task_plan.sub_task_plan_list_size() <= 0) {
        LOG(ERROR) << " Init job error : " << task_plan.ShortDebugString();
        return false;
    }

    // computer inter_task_number and leaf_task_number;
    uint32_t inter_task_number = 1;
    uint32_t leaf_task_number = 0;
    GetTaskNumberByPlan(task_plan, &inter_task_number, &leaf_task_number);
    m_task_manager.reset(new TaskManager(GetJobID(),
                                         inter_task_number,
                                         leaf_task_number));

    // add task by task plan
    TaskType type = GetTaskTypeByPlan(task_plan);
    DCHECK_EQ(kInterTask, type);
    AddTaskByPlan(task_plan, type, 0);

    VLOG(10) << "Job : " << GetJobID() << " init succeed ";

    return true;
}

void Job::AddTaskByPlan(const compiler::TaskPlanProto& plan,
                        const TaskType& type,
                        const uint32_t& parent_task_id) {
    if (kInterTask == type) {
        CHECK_GT(plan.sub_task_plan_list_size(), 0);
        TaskType child_type = GetTaskTypeByPlan(plan.sub_task_plan_list(0));
        InterTaskInput task_input;
        task_input.mutable_exec_plan()->CopyFrom(plan.exec_plan());
        task_input.set_child_type(child_type);
        task_input.set_result_proto(plan.table_schema_string());
        task_input.set_result_message(plan.table_message_name());
        task_input.set_task_precision(GetJobSpec().job_precision());
        uint32_t task_id = m_task_manager->AddInterTask(task_input,
                                                        parent_task_id);
        for (int32_t i = 0; i < plan.sub_task_plan_list_size(); ++i) {
            AddTaskByPlan(plan.sub_task_plan_list(i),
                          child_type,
                          task_id);
        }
        return;
    }
    DCHECK_EQ(kLeafTask, type);

    LeafTaskInput task_input;
    task_input.mutable_exec_plan()->CopyFrom(plan.exec_plan());

    std::vector<std::vector<std::string> > column_list;
    std::vector<TabletInfo> tablet_list;
    GetTabletFileAndColumnsFromPlan(
        plan.exec_plan(), &tablet_list, &column_list);

    DCHECK_LE(tablet_list.size(), 2U);
    DCHECK_EQ(tablet_list.size(), column_list.size());

    task_input.set_result_proto(plan.table_schema_string());
    task_input.set_result_message(plan.table_message_name());

    for (size_t i = 0; i < tablet_list.size(); ++i) {
        const std::vector<std::string>& column_names = column_list[i];
        ScannerInput* scanner_input = (task_input.mutable_scanner_input())->Add();
        for (size_t j = 0; j < column_names.size(); ++j) {
            scanner_input->add_column_names(column_names[j]);
        }
        (scanner_input->mutable_tablet())->CopyFrom(tablet_list[i]);
    }

    m_task_manager->AddLeafTask(task_input, parent_task_id);
}

void Job::GetJobResult(const GetJobResultRequest* request,
                       GetJobResultResponse* response) const {
    VLOG(30) << "Get get job result request ["
        << request->ShortDebugString() << "]";
    m_job_state->GetJobResult(response);
}

void Job::GenerateSchedulerPlan(SchedulerPlan* plan,
                                const TaskType& type,
                                const uint32_t& task_id) {
    m_task_manager->GenerateSchedulerPlan(plan, type, task_id);
}

void Job::GenerateEmitterPlan(const SchedulerPlan& scheduler_plan,
                              EmitterPlan* emitter_plan) {
    m_task_manager->GenerateEmitterPlan(scheduler_plan, emitter_plan);
}

void Job::EmitTaskSucceed(const TaskInfo& task_info) {
    m_task_manager->EmitTaskSucceed(task_info);

    if (m_task_manager->IsAllTaskEmitSucceed()) {
        VLOG(10) << "All task emit succeed for job : " << GetJobID();
        SetJobStatus(kJobEmitterSucceed);
        SetJobEmitFinishTime(toft::GetTimestamp());
    }
}

uint64_t Job::GetJobID() const {
    return m_job_state->GetJobID();
}

const JobSpecification& Job::GetJobSpec() const {
    return m_job_state->GetJobSpec();
}

void Job::SetJobStartTime(int64_t t) {
    m_job_state->SetJobStartTime(t);
}

void Job::SetJobAnalyseFinishTime(int64_t t) {
    m_job_state->SetJobAnalyseFinishTime(t);
}

void Job::SetJobScheduleFinishTime(int64_t t) {
    m_job_state->SetJobScheduleFinishTime(t);
}

void Job::SetJobEmitFinishTime(int64_t t) {
    m_job_state->SetJobEmitFinishTime(t);
}

void Job::SetJobStatus(const JobStatus& job_status,
                       const std::string& failed_reason) {
    m_job_state->SetJobStatus(job_status, failed_reason);
}

void Job::UpdateTaskState(const TaskState& task_state) {
    m_task_manager->UpdateTaskState(task_state);
    m_job_state->UpdateTaskState(task_state);
}

JobStatus Job::GetJobStatus() const {
    return m_job_state->GetJobStatus();
}

}  // namespace master
}  // namespace gunir
