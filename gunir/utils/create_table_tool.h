// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_UTILS_CREATE_TABLE_TOOL_H
#define  GUNIR_UTILS_CREATE_TABLE_TOOL_H

#include <string>

#include "gunir/compiler/parser/select_stmt.pb.h"

namespace gunir {

class CreateTablet;

class CreateTableTool {
public:
    explicit CreateTableTool(const compiler::CreateTableStmt* create_stmt);
    ~CreateTableTool();
    bool LocalRun(const std::string& output_prefix);
    bool RemoteRun(const std::string& output_prefix);

private:
    bool PrepareInputInfo(const std::string& output_prefix, std::string* arg);
    bool ParseSingleFile(CreateTablet* create_tablet,
                         const std::string& filename);

private:
    const compiler::CreateTableStmt* m_create_stmt;
};

} // namespace gunir

#endif  // GUNIR_UTILS_CREATE_TABLE_TOOL_H
