// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_MASTER_JOB_ANALYSER_H
#define  GUNIR_MASTER_JOB_ANALYSER_H

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"

#include "gunir/proto/job.pb.h"
#include "gunir/compiler/parser/select_stmt.pb.h"

namespace gunir {

namespace compiler {
class SelectQuery;
}  // namespace compiler

namespace master {

class TableManager;

class JobAnalyser {
public:
    JobAnalyser(TableManager* table_manager);
    virtual ~JobAnalyser();

    virtual bool DoAnalyser(const JobSpecification& job_spec,
                            JobAnalyserResult* result) const;

private:
    bool TableNameAnalyser(
        compiler::SelectQuery* select_query,
        const ::google::protobuf::RepeatedPtrField<TableInfo>& table_info,
        std::string* failed_reason) const;

    bool BigQueryAnalyser(compiler::SelectQuery* select_query,
                          JobAnalyserResult* result) const;

private:
    TableManager* m_table_manager;
};

}  // namespace master
}  // namespace gunir
#endif  // GUNIR_MASTER_JOB_ANALYSER_H
