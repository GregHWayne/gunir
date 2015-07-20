// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/master/job_analyser.h"

#include <string>
#include <vector>

#include "gunir/compiler/job_plan.h"
#include "gunir/compiler/select_query.h"
#include "gunir/master/job.h"
#include "gunir/master/table_manager.h"

DECLARE_string(gunir_table_root_dir);

namespace gunir {
namespace master {

JobAnalyser::JobAnalyser(TableManager* table_manager)
    : m_table_manager(table_manager) {}

JobAnalyser::~JobAnalyser() {
    VLOG(10) << "Job analyser exit now ";
}

bool JobAnalyser::DoAnalyser(const JobSpecification& job_spec,
                             JobAnalyserResult* result) const {
    if (!job_spec.query_stmt().has_select()) {
        LOG(ERROR) << "No select statement in query : ["
            << job_spec.query_stmt().ShortDebugString() << "]";
        return false;
    }
    std::string failed_reason;
    compiler::SelectQuery select_query(job_spec.query_stmt().select());
    if (!TableNameAnalyser(&select_query,
                           job_spec.table_info(),
                           &failed_reason)) {
        result->set_failed_reason(failed_reason);
        LOG(ERROR) << failed_reason;
        return false;
    }

    if (!BigQueryAnalyser(&select_query, result)) {
        result->set_failed_reason(select_query.GetErrString());
        LOG(ERROR) << select_query.GetErrString();
        return false;
    }

    return true;
}

bool JobAnalyser::TableNameAnalyser(
        compiler::SelectQuery* select_query,
        const ::google::protobuf::RepeatedPtrField<TableInfo>& table_info,
        std::string* failed_reason) const {
    std::vector<TableInfo> table_info_vec;
    std::vector<std::string> table_name_vec;
    table_info_vec.clear();
    table_name_vec.clear();
    for (int32_t i = 0; i < table_info.size(); ++i) {
        table_info_vec.push_back(table_info.Get(i));
        table_name_vec.push_back(table_info.Get(i).table_name());
    }

    std::vector<std::string> table_names = select_query->GetQueryTables();
    std::vector<std::string>::const_iterator const_iter;
    for (const_iter = table_names.begin();
         const_iter != table_names.end();
         ++const_iter) {
        bool is_user_defined = false;
        for (uint32_t i = 0; i < table_name_vec.size(); ++i) {
            if (table_name_vec[i] == *const_iter) {
                is_user_defined = true;
                break;
            }
        }
        if (!is_user_defined) {
            TableInfo table_info;
            TableStatusCode status;
            if (m_table_manager->GetSingleTable(*const_iter, &table_info, &status)) {
                table_info_vec.push_back(table_info);
            }
        }
    }

    return select_query->Init(table_info_vec);
}

bool JobAnalyser::BigQueryAnalyser(compiler::SelectQuery* select_query,
                                   JobAnalyserResult* result) const {
    if (!select_query->Analyze()) {
        return false;
    }

    compiler::JobPlan job_plan;
    job_plan.Init(*select_query);
    result->mutable_task_plan()->CopyFrom(job_plan.GetTaskPlan());

    VLOG(10) << "Get task_planer succeed ["
        << result->ShortDebugString() << "]";

    return true;
}

}  // namespace master
}  // namespace gunir
