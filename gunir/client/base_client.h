// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef GUNIR_CLIENT_BASE_CLIENT_H
#define GUNIR_CLIENT_BASE_CLIENT_H

#include <ostream>
#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"

#include "gunir/client/user_table.h"
#include "gunir/compiler/parser/big_query_parser.h"
#include "gunir/compiler/parser/big_query_types.h"
#include "gunir/compiler/parser/select_stmt.pb.h"
#include "gunir/compiler/parser/table_schema.h"
#include "gunir/proto/master_rpc.pb.h"

#define PROMPT_GREEN_BEGIN  "\033[32m"
#define PROMPT_RED_BEGIN  "\033[31m"
#define PROMPT_COLOR_END    "\033[0m"

namespace gunir {
namespace client {

class BaseClient {
public:
    BaseClient();

    virtual ~BaseClient() {}

    bool Run(const std::string& query);

    bool Run(const std::string& query, std::ostream* out);

protected:
    bool Run(const std::string& query, const std::string& job_identity,
             std::ostream* out);

    virtual bool RunQueryTable(const compiler::QueryStmt& stmt,
                               const std::string& query,
                               const std::string& job_identity,
                               std::ostream* stream);

    virtual void RunQuit(std::ostream* stream);

    virtual bool RunCreateTable(const compiler::CreateTableStmt& stmt,
                                const std::string& job_identity,
                                std::ostream* stream);

    virtual bool RunDefineTable(const compiler::DefineTableStmt& stmt,
                                const std::string& job_identity,
                                std::ostream* stream);

    virtual bool RunDropTable(const compiler::DropTableStmt& stmt,
                              const std::string& job_identity,
                              std::ostream* stream);

    bool RunHelp(const compiler::HelpStmt& stmt,
                 std::ostream* stream);

    virtual bool RunShow(const compiler::ShowStmt& stmt,
                         const std::string& job_identity,
                         std::ostream* stream);

    virtual bool DropTable(const std::string& table_name,
                           const std::string& job_identity);

    virtual void AssembleJobResult(uint64_t job_id,
                                   const GetJobResultResponse& response,
                                   std::ostream* stream);
    virtual void AssembleJobResultTable(uint64_t job_id,
                                        const GetJobResultResponse& response,
                                        std::ostream* stream);
//     virtual void DumpHistoryQuery(const GetHistoryQueryResponse& request,
//                                   std::ostream* stream);

    virtual std::string GreenString(const std::string& word) {
        return PROMPT_GREEN_BEGIN + word + PROMPT_COLOR_END;
    }

    virtual std::string RedString(const std::string& word) {
        return PROMPT_RED_BEGIN + word + PROMPT_COLOR_END;
    }

    virtual std::string GreenTitleLine(const std::vector<int32_t>& widths);

protected:
    virtual bool SubmitJob(const SubmitJobRequest* request,
                           SubmitJobResponse* response) = 0;
    virtual bool GetJobResult(const GetJobResultRequest* request,
                              GetJobResultResponse* response) = 0;
    virtual bool GetMetaInfo(const GetMetaInfoRequest* request,
                             GetMetaInfoResponse* response) = 0;
    virtual bool AddTable(const AddTableRequest* request,
                          AddTableResponse* response) = 0;
    virtual void PrintTableInfo(const TableInfo& table_info,
                                const std::string* title,
                                std::ostream* stream);
    virtual bool GetJobResultWithId(const uint64_t& job_id,
                                    std::ostream* stream);

private:
    void GetQueryTables(const compiler::SelectStmt& stmt,
                        std::vector<std::string>* table_names) const;
    bool QueryTableInfo(const std::vector<std::string>& table_names,
                        std::vector<TableInfo>* table_infos);
    bool QueryTableInfo(const std::string& table_name,
                        const std::string& job_identity,
                        std::ostream* stream);
    bool QueryAllTable(std::ostream* stream,
                       const std::string& job_identity);

protected:
    uint64_t m_last_sequence_id;
    toft::scoped_ptr<UserTable> m_user_table;
};

} // namespace client
} // namespace gunir

#endif // GUNIR_CLIENT_BASE_CLIENT_H
