// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_UTILS_PARSE_LOG_TOOL_H
#define  GUNIR_UTILS_PARSE_LOG_TOOL_H

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"

namespace gunir {
class CreateTablet;
class ParseLogTool {
public:
    ParseLogTool();
    ~ParseLogTool();

    bool Open(const std::string& input,
              const std::string& table_name,
              const std::string& output_dir);

    bool Parse();

    bool Close();

private:
    bool ParseOneFile(const std::string& file);

private:
    toft::scoped_ptr<CreateTablet> m_create_tablet;
    std::vector<std::string> m_input_files;
};

}  // namespace gunir

#endif  // GUNIR_UTILS_PARSE_LOG_TOOL_H
