// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_UTILS_MESSAGE_UTILS_H
#define  GUNIR_UTILS_MESSAGE_UTILS_H

#include <string>
#include <vector>

#include "gunir/proto/job.pb.h"
#include "gunir/proto/master_rpc.pb.h"
#include "gunir/proto/task.pb.h"

namespace gunir {

bool IsRootTask(const TaskInfo& task_info);
bool IsTaskCompleted(const TaskInfo& task_info);
bool IsTaskFailed(const TaskStatus& status);
bool IsTaskFinished(const TaskInfo& task_info);
bool IsTaskUnRecoverable(const TaskStatus& status);

JobIdentity GenerateJobIdentity();
bool IsJobFinished(const JobStatus& status);

double GetJobRunningTime(const JobProgressTimeStamp& time_stamp);
bool ShouldTaskCancel(const ReportResponse& response,
                      const TaskInfo& task_info);
bool IsCharSetEncodingRight(const std::string& encoding);
bool InitCreateTableStmt(compiler::CreateTableStmt* stmt,
                         std::string* err_string);

}  // namespace gunir

#endif  // GUNIR_UTILS_MESSAGE_UTILS_H
