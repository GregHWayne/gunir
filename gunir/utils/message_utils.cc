// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/utils/message_utils.h"

#include "toft/base/string/algorithm.h"
#include "toft/storage/file/file.h"

#include "gunir/utils/csv_helper.h"
#include "gunir/utils/encoding/charset_converter.h"
#include "gunir/utils/proto_message.h"

DECLARE_string(gunir_user_identity);
DECLARE_string(gunir_default_charset_encoding);

namespace gunir {

bool IsRootTask(const TaskInfo& task_info) {
    return task_info.type() == kInterTask && task_info.task_id() == 0;
}

bool IsTaskCompleted(const TaskInfo& task_info) {
    return task_info.task_status() == kCompletedTask;
}

bool IsTaskFailed(const TaskStatus& status) {
    return (kFailedTask == status || kFailedComputeTask == status
        || kFailedSendTask == status || IsTaskUnRecoverable(status));
}

bool IsTaskFinished(const TaskInfo& task_info) {
    return IsTaskCompleted(task_info) || IsTaskFailed(task_info.task_status());
}

bool IsJobFinished(const JobStatus& status) {
    return (kJobFinished == status || kJobAnalyseFailed == status
        || kJobSchedulerFailed == status || kJobRunFailed == status);
}

bool IsTaskUnRecoverable(const TaskStatus& status) {
    return kCanceledTask == status || kResultManagerOOMTask == status
        || kInterResultOOMTask == status || kLeafResultOOMTask == status;
}

JobIdentity GenerateJobIdentity() {
    JobIdentity identity;
    identity.set_identity(FLAGS_gunir_user_identity);
    identity.set_role(FLAGS_gunir_user_identity);
    return identity;
}

double GetJobRunningTime(const JobProgressTimeStamp& time_stamp) {
    return (static_cast<double>(time_stamp.job_finish_time())
            - time_stamp.job_start_time()) / 1000;
}

bool ShouldTaskCancel(const ReportResponse& response,
                      const TaskInfo& task_info) {
//     for (int32_t i = 0; i < response.finished_job_id_size(); ++i) {
//         if (task_info.job_id() == response.finished_job_id(i)) {
//             return true;
//         }
//     }
    return false;
}

bool IsCharSetEncodingRight(const std::string& encoding) {
    CharsetConverter convert;
    return convert.Create(encoding, FLAGS_gunir_default_charset_encoding);
}

bool InitProtoSchema(compiler::CreateTableStmt* stmt,
                     std::string* err_string) {
    ProtoMessage proto_message;
    if (!proto_message.CreateMessageByProtoFile(
            stmt->table_schema().char_string(),
            stmt->message_name().char_string())) {
        *err_string = "Proto file [";
        *err_string += stmt->table_schema().char_string()
            + "] or Message name [" + stmt->message_name().char_string()
            + "] is not correct.\n";
        return false;
    }
    stmt->mutable_table_schema()->set_char_string(
        proto_message.GetFileDescriptorSetString());
    stmt->mutable_table_type()->set_char_string("PB");
    return true;
}

bool InitCsvSchema(compiler::CreateTableStmt* stmt,
                   std::string* err_string) {
    CsvHelper csv_helper;
    if (!csv_helper.CreateMessageByCsvFile(stmt->table_schema().char_string(),
                                           stmt->message_name().char_string())) {
        *err_string = "Csv file [";
        *err_string += stmt->table_schema().char_string()
            + "] is not correct.\n";
        return false;
    }
    stmt->mutable_table_schema()->set_char_string(
        csv_helper.GetFileDescriptorSetString());
    stmt->mutable_table_type()->set_char_string("CSV");
    return true;
}

bool InitCreateTableStmt(compiler::CreateTableStmt* stmt,
                         std::string* err_string) {
    // check encoding
    const std::string& encoding = stmt->charset_encoding().char_string();
    if (!IsCharSetEncodingRight(encoding)) {
        *err_string = "Can not convert charset encoding : ";
        *err_string += encoding + " into : "
            + FLAGS_gunir_default_charset_encoding + "\n";
        return false;
    }

    // generate schema
    std::string schema = stmt->table_schema().char_string();
    StringToLower(&schema);
    if (StringEndsWith(schema, ".proto")) {
        return InitProtoSchema(stmt, err_string);
    } else if (StringEndsWith(schema, ".csv")) {
        return InitCsvSchema(stmt, err_string);
    }
    *err_string = "We Only support .csv and .proto schema file \n";
    return false;
}

}  // namespace gunir
