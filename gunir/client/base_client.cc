// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/client/base_client.h"

#include <signal.h>

#include <iomanip>
#include <vector>

#include "thirdparty/gflags/gflags.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/algorithm.h"
#include "toft/base/string/number.h"
#include "toft/storage/file/file.h"
#include "toft/system/memory/mempool.h"
#include "toft/system/threading/this_thread.h"



#include "gunir/client/sql_command_info.h"
#include "gunir/io/column_metadata.pb.h"
#include "gunir/io/pb_assembler.h"
#include "gunir/io/short_display_reader.h"
#include "gunir/types.h"
#include "gunir/utils/filename_tool.h"
#include "gunir/utils/message_utils.h"


DECLARE_string(gunir_user_defined_table_conf);
DECLARE_double(gunir_job_precision);

extern bool g_quit;

namespace gunir {
namespace client {

BaseClient::BaseClient()
    : m_last_sequence_id(kSequenceIDStart) {
    m_user_table.reset(new UserTable(FLAGS_gunir_user_defined_table_conf));
}

bool BaseClient::Run(const std::string& query) {
    return Run(query, "", &(std::cout));
}

bool BaseClient::Run(const std::string& query, std::ostream* out) {
    return Run(query, "", out);
}

bool BaseClient::Run(const std::string& query,
                     const std::string& job_identity,
                     std::ostream* out) {
    toft::scoped_ptr<compiler::QueryStmt> query_stmt;
    compiler::QueryStmt* tmp_stmt = NULL;
    int flag = parse_line(query.c_str(), &tmp_stmt, out);
    query_stmt.reset(tmp_stmt);

    if (flag != 0) {
        return false;
    }

    bool ret = true;
    if (query_stmt->has_select()) {
        ret =  RunQueryTable(*(query_stmt.get()), query, job_identity, out);
    } else if (query_stmt->has_create()) {
        ret = RunCreateTable(query_stmt->create(), job_identity, out);
    } else if (query_stmt->has_define()) {
        ret = RunDefineTable(query_stmt->define(), job_identity, out);
    } else if (query_stmt->has_drop()) {
        ret = RunDropTable(query_stmt->drop(), job_identity, out);
    } else if (query_stmt->has_help()) {
        ret = RunHelp(query_stmt->help(), out);
    } else if (query_stmt->has_quit()) {
        RunQuit(out);
        ret = true;
    } else if (query_stmt->has_show()) {
        ret = RunShow(query_stmt->show(), job_identity, out);
    } else {
        LOG(ERROR) << " Get empty query stmt here ";
    }

    return ret;
}

void BaseClient::GetQueryTables(const compiler::SelectStmt& stmt,
                                std::vector<std::string>* table_names) const {
    const compiler::RawTableList& from_list = stmt.from_list();
    for (int i = 0; i < from_list.table_list_size(); ++i) {
        const compiler::RawTable& raw_table = from_list.table_list(i);
        if (raw_table.has_table_name())  {
            table_names->push_back(raw_table.table_name().char_string());
        }
    }

    // ADD table from Join table
    if (stmt.has_join()) {
        const compiler::RawTable& raw_table = stmt.join().partner();
        table_names->push_back(raw_table.table_name().char_string());
    }
}

bool BaseClient::RunQueryTable(const compiler::QueryStmt& stmt,
                               const std::string& query,
                               const std::string& user_identity,
                               std::ostream* stream) {
    LOG(INFO) << "RunQueryTable()";
    SubmitJobRequest request;
    request.set_sequence_id(m_last_sequence_id++);
    JobSpecification* job_spec = request.mutable_job_spec();
    job_spec->mutable_query_stmt()->CopyFrom(stmt);
    job_spec->set_query(query);
    JobIdentity job_identity;
    job_identity.set_identity(user_identity);
    job_identity.set_role(user_identity);
    job_spec->mutable_job_identity()->CopyFrom(job_identity);
    job_spec->set_job_precision(FLAGS_gunir_job_precision);

    std::vector<TableInfo> table_info;
    std::vector<std::string> table_names;
    GetQueryTables(stmt.select(), &table_names);
    if (!m_user_table->QueryTable(table_names, &table_info)) {
        LOG(ERROR) << "Query user defined table failed.";
        return false;
    }
    for (uint32_t i = 0; i < table_info.size(); ++i) {
        TableInfo* info = job_spec->add_table_info();
        info->CopyFrom(table_info[i]);
    }

    SubmitJobResponse response;
    if (!SubmitJob(&request, &response)) {
        *stream << "Submit job to master error : "
            << response.ShortDebugString() << std::endl;
        return false;
    }

    if (response.job_status() != kJobSubmitSucceed) {
        LOG(ERROR) << "fail to submit job, server status: "
            << StatusCode_Name(response.status());
        return false;
    }

    LOG(INFO) << "Job submit succeed ";

    if (!GetJobResultWithId(response.job_id(), stream)) {
        return false;
    }
    return true;
}

bool BaseClient::GetJobResultWithId(const uint64_t& job_id,
                                    std::ostream* stream) {
    GetJobResultRequest request;
    request.set_job_id(job_id);
    request.set_sequence_id(m_last_sequence_id++);

    while (!g_quit) {
        GetJobResultResponse response;
        if (!GetJobResult(&request, &response)) {
            *stream << "fail to get result by rpc timeout";
            return false;
        }
        switch (response.result().job_status()) {
        case kJobAnalyseSucceed:
            LOG(INFO) << " Job analyser succeed ";
            break;
        case kJobSchedulerSucceed:
            LOG(INFO) << " Job scheduler succeed ";
            break;
        case kJobEmitterSucceed :
            LOG(INFO) << " Job emitter succeed ";
            break;
        case kJobFinished:
            LOG(INFO) << " Job run succeed ";
            AssembleJobResultTable(job_id, response, stream);
            break;
        case kJobNotExist :
        case kJobAnalyseFailed :
        case kJobSchedulerFailed :
        case kJobRunFailed :
            *stream << response.result().reason() << std::endl;
            return false;
        default :
            break;
        }

        if (kJobFinished == response.result().job_status()) {
            return true;
        }

        toft::ThisThread::Sleep(200);
    }
    if (g_quit) {
        *stream << "User interrupted ... quit" << std::endl;
    }
    return true;
}

bool BaseClient::RunCreateTable(const compiler::CreateTableStmt& stmt,
                                const std::string& job_identity,
                                std::ostream* stream) {
    const std::string& table_name = stmt.table_name().char_string();
    AddTableRequest request;
    AddTableResponse response;

    request.mutable_create()->CopyFrom(stmt);
    request.set_sequence_id(m_last_sequence_id++);
    request.set_is_created(true);

    std::string err_code;
    if (!InitCreateTableStmt(request.mutable_create(), &err_code)) {
        *stream << err_code;
        return false;
    }

    if (!AddTable(&request, &response)) {
        *stream << "fail to rpc to master for meta: " << table_name << std::endl;
        return false;
    }
    return true;
}

bool BaseClient::RunDefineTable(const compiler::DefineTableStmt& stmt,
                                const std::string& job_identity,
                                std::ostream* stream) {
    return m_user_table->AddTable(stmt.table_name().char_string(),
                                  stmt.input_path().char_string());
}

bool BaseClient::RunDropTable(const compiler::DropTableStmt& stmt,
                              const std::string& job_identity,
                              std::ostream* stream) {
    return true;
}

bool BaseClient::DropTable(const std::string& table_name,
                           const std::string& job_identity) {
    return true;
}

void BaseClient::RunQuit(std::ostream* stream) {
    std::cout << GreenString("You have terminated the gunir shell ... bye")
        << std::endl;
    raise(SIGINT);
}

bool BaseClient::RunShow(const compiler::ShowStmt& stmt,
                         const std::string& job_identity,
                         std::ostream* stream) {
    if (stmt.has_table_name()) {
        return QueryTableInfo(stmt.table_name().char_string(),
                              job_identity,
                              stream);
    } else {
        return QueryAllTable(stream, job_identity);
    }
}

// void BaseClient::DumpHistoryQuery(const GetHistoryQueryResponse& response,
//                                   std::ostream* stream) {
// }

void BaseClient::PrintTableInfo(const TableInfo& table_info,
                                const std::string* title,
                                std::ostream* stream) {
    if (title) {
        *stream << GreenString(*title) << std::endl;
    }
    compiler::TableSchema schema;
    CHECK(schema.InitSchemaFromFileDescriptorProto(table_info.table_schema(),
                                                   table_info.message_name()));

    std::vector<compiler::ColumnInfo> static_info = schema.GetAllColumnInfo();
    *stream << GreenString("Type\t\tField Name") << std::endl;
    for (uint32_t i = 0; i < static_info.size(); ++i) {
        *stream << compiler::BigQueryType::EnumToString(static_info[i].m_type);
        *stream << "\t\t" << static_info[i].m_column_path[0];
        for (uint32_t j = 1; j < static_info[i].m_column_path.size(); ++j) {
            *stream << "." << static_info[i].m_column_path[j];
        }
        *stream << std::endl;
    }
}

bool BaseClient::RunHelp(const compiler::HelpStmt& stmt,
                         std::ostream* stream) {
    int index = 0;
    if (stmt.has_cmd_name()) {
        const char *name = NULL;
        const char *cmd_name = stmt.cmd_name().char_string().c_str();
        int len = strlen(cmd_name);
        while (NULL != (name = g_command_lists[index].command)) {
            if (0 == strncmp(name, cmd_name, len)) {
                *stream << GreenString(g_command_lists[index].command)
                    << ":" << g_command_lists[index].descr << std::endl
                    << "Usage:" << std::endl
                    << g_command_lists[index].usage << std::endl;
                break;
            }
            index++;
        }
        if (name == NULL) {
            LOG(ERROR) << "not found command: "
                << stmt.cmd_name().char_string();
            return false;
        }
    } else {
        while (NULL != g_command_lists[index].command) {
            *stream << GreenString(g_command_lists[index].command)
                << "\t\t" << g_command_lists[index].descr
                << std::endl;
            index++;
        }
    }
    return true;
}

bool BaseClient::QueryTableInfo(const std::vector<std::string>& table_names,
                                std::vector<TableInfo>* table_infos) {
    GetMetaInfoRequest request;
    GetMetaInfoResponse response;

    request.set_sequence_id(m_last_sequence_id++);
    for (uint32_t i = 0; i < table_names.size(); ++i) {
        request.add_table_names(table_names[i]);
    }

    if (!GetMetaInfo(&request, &response)) {
        LOG(ERROR) << "fail to fetch table meta";
        return false;
    }

    for (int32_t i = 0; i < response.results_size(); ++i) {
        if (response.results(i) == kTableOk) {
            table_infos->push_back(response.table_infos(i));
        }
    }
    return table_infos->size() > 0;
}

bool BaseClient::QueryTableInfo(const std::string& table_name,
                                const std::string& job_identity,
                                std::ostream* stream) {
    bool has_user_table = false;
    TableInfo table_info;
    if (m_user_table->QueryTableByName(table_name, &table_info)) {
        const std::string title = "User-defined table:";
        PrintTableInfo(table_info, &title, stream);
        has_user_table = true;
    }


    std::vector<std::string> tables;
    tables.push_back(table_name);
    std::vector<TableInfo> infos;
    bool has_system_table = QueryTableInfo(tables, &infos);
    if (has_system_table) {
        const std::string title = "System-created table:";
        PrintTableInfo(infos[0], &title, stream);
    }

    if (has_user_table || has_system_table) {
        return true;
    } else {
        *stream << "Table [" << table_name << "] not exist yet" << std::endl;
        return false;
    }
}

bool BaseClient::QueryAllTable(std::ostream* stream,
                               const std::string& job_identity) {
    LOG(INFO) << "QueryAllTable()";
    std::vector<TableInfo> table_info;
    if (m_user_table->QueryAllTable(&table_info)) {
        if (table_info.size() > 0U) {
            *stream << GreenString("No\t\tUser-defined Table Name (Size)") << std::endl;
            for (uint32_t i = 0; i < table_info.size(); ++i) {
                *stream << i << "\t\t" << table_info[i].table_name()
                    << " (" << CovertByteToString(table_info[i].table_size())
                    << ")"<< std::endl;
            }
        }
    }

    return true;
}

static uint64_t kMaxPrintSize = 512;
void BaseClient::AssembleJobResult(uint64_t job_id,
                                   const GetJobResultResponse& response,
                                   std::ostream* stream) {
    std::string file_name = GetUserJobResultFileName(job_id, "text");
    VLOG(10) << "assemble job result at file: " << file_name;
    toft::scoped_ptr<toft::File> file(toft::File::Open(file_name, "w"));
    CHECK_NOTNULL(file.get());

    toft::MemPool mempool(toft::MemPool::MAX_UNIT_SIZE);
    uint64_t print_size = 0;
    bool is_print_last_line = false;
    const std::string comment = GreenString("-----------------");
    for (int32_t i = 0; i < response.result().job_result_tablets_size(); ++i) {
        io::TabletReader reader(&mempool);
        CHECK(reader.Init(response.result().job_result_tablets(i)));

        io::PbRecordAssembler assembler;
        CHECK(assembler.Init(&reader));

        toft::scoped_ptr<google::protobuf::Message> message;
        message.reset(assembler.GetProtoMessage()->New());
        std::string record = "";
        while (assembler.AssembleRecord(message.get())) {
            record = message->Utf8DebugString();
            uint32_t length = record.length();
            if (print_size < kMaxPrintSize) {
                *stream << comment + GreenString(" Record ") + comment
                    << std::endl << record;
                print_size += length;
            } else if (!is_print_last_line) {
                *stream << comment + GreenString(" More record in file : ")
                    << RedString(file_name) + " " + comment << std::endl;
                is_print_last_line = true;
            }
            if (length > 0) {
                CHECK_NE(-1, file->Write(record.c_str(), length))
                    << "Write file error : " << file_name;
            }
        }
        reader.Close();
    }

    if (!is_print_last_line) {
        *stream << comment + GreenString(" All record are saved in file : ")
            << RedString(file_name) + " " + comment << std::endl;
    }

    *stream << comment << GreenString(" Time consume : ")
        << GetJobRunningTime(response.result().job_timestamp())
        << GreenString("(s) ") << ", Query result size : "
        << CovertByteToString(response.result().result_file_size())
        << comment << std::endl;

    CHECK_NE(-1, file->Close()) << "Close output file error ";
}

static uint64_t kMaxTablePrintSize = 8192 - 1024;
static uint64_t kMaxTablePrintNum = 10;
void BaseClient::AssembleJobResultTable(uint64_t job_id,
                                        const GetJobResultResponse& response,
                                        std::ostream* stream) {
    uint64_t print_size = 0;
    uint64_t record_no = 0;
    uint64_t rest_record_no = response.result().result_slice_number();
    if (rest_record_no > kMaxTablePrintNum) {
        rest_record_no -= kMaxTablePrintNum;
    } else {
        rest_record_no = 0;
    }

    std::string output_body;
    const std::string semi = ";";
    bool is_print_last_line = false;
    std::vector<std::string> max_field_name;

    for (int32_t i = 0; i < response.result().job_result_tablets_size(); ++i) {
        if (is_print_last_line) {
            break;
        }
        io::ShortDisplayReader reader;
        CHECK(reader.Init("", response.result().job_result_tablets(i)));
        while (reader.Next()) {
            if (is_print_last_line) {
                break;
            }
            io::Slice* slice = reader.GetSlice();
            std::vector<uint32_t> field_count;
            std::vector<std::string> field_name;

            reader.GetFieldCount(&field_count);
            reader.GetFieldName(&field_name);

            if (field_name.size() > max_field_name.size()) {
                max_field_name.clear();
                std::vector<std::string>::iterator it = field_name.begin();
                for (; it != field_name.end(); ++it) {
                    max_field_name.push_back(*it);
                }
            }
            std::string output_line = toft::NumberToString(record_no++) + semi;
            std::string content_string;

            for (uint32_t j = 0; j < slice->GetCount(); ++j) {
                const io::Block* block = slice->GetBlock(j);
                std::string value;
                if (!block->IsNull()) {
                    std::vector<std::string> field_value;
                    std::string delim = "\n";
                    toft::SplitString(block->DebugString(), delim.c_str(), &field_value);
                    value = field_value[3];
                    std::vector<std::string> val_items;
                    toft::SplitString(field_value[3], ": ", &val_items);
                    if (val_items.size() > 1) {
                        value = val_items[1];
                    } else {
                        value = val_items[0];
                    }
                    LOG(INFO) << "value = " << value;
                }

                if (j != 0) {
                    content_string += semi;
                }

                if (print_size < kMaxTablePrintSize && record_no <= kMaxTablePrintNum) {
                    content_string += value
                        + semi + NumberToString(field_count[j]);

                    print_size += field_name[j].length() + value.length();
                } else if (!is_print_last_line) {
                    is_print_last_line = true;
                }

                // dump to file
            }
            if (!is_print_last_line) {
                output_line += content_string + "\n";
                output_body += output_line;
            }
        }
    }
    const std::string bar_line = GreenString("|");
    std::string output_title = bar_line + "Row";
    std::vector<int32_t> title_widths;
    title_widths.push_back(std::string("Row").length());
    int32_t total_width = title_widths[0];
    for (std::vector<std::string>::iterator it = max_field_name.begin();
         it != max_field_name.end(); ++it) {
        output_title += bar_line + *it;
        title_widths.push_back(it->length());
        total_width += it->length();
    }
    if (title_widths.size() > 0) {
        output_title += bar_line;
    }

    // output to screen
    *stream << std::endl;
    *stream << GreenTitleLine(title_widths) << std::endl;
    *stream << output_title << std::endl;

    std::vector<std::string> lines;
    toft::SplitString(output_body, "\n", &lines);
    for (uint32_t i = 0; i < lines.size(); ++i) {
        *stream << GreenTitleLine(title_widths) << std::endl;
        std::vector<std::string> items;
        toft::SplitStringKeepEmpty(lines[i], ";", &items);
        for (uint32_t j = 0, w = 0; j < items.size(); ++j) {
            *stream << bar_line << std::setw(title_widths[w++]) << items[j];
            if (j != 0) {
                j++;
                if (items[j] != "1" && items[j] != "0") {
                    *stream << "*";
                }
            }
        }
        *stream << bar_line << std::endl;
    }

    *stream << GreenTitleLine(title_widths);
    *stream << std::endl;
    *stream << GreenString("More records: ") << rest_record_no << std::endl;
    *stream << GreenString("Time consume : ")
        << GetJobRunningTime(response.result().job_timestamp())
        << GreenString("(s), Query result size : ")
        << CovertByteToString(response.result().result_file_size())
        << std::endl << std::endl;
}

std::string BaseClient::GreenTitleLine(const std::vector<int32_t>& widths) {
    std::string out(PROMPT_GREEN_BEGIN);
    for (uint32_t i = 0; i < widths.size(); ++i) {
        for (uint32_t j = 0; j < widths[i] + 1; ++j) {
            if (j == 0) {
                out += "+";
            } else {
                out += "-";
            }
        }
    }
    return out + "+" + PROMPT_COLOR_END;
}

} // namespace client
} // namespace gunir
