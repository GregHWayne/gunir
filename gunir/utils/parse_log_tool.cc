// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/utils/parse_log_tool.h"

#include "gunir/io/tablet_schema.pb.h"
#include "gunir/utils/create_tablet.h"
#include "gunir/utils/filename_tool.h"
#include "gunir/utils/parse_log.h"
#include "gunir/utils/proto_message.h"

namespace gunir {
ParseLogTool::ParseLogTool() {}
ParseLogTool::~ParseLogTool() {}

bool ParseLogTool::Open(const std::string& input,
                        const std::string& table_name,
                        const std::string& output_dir) {
    if (!CreateDir(output_dir)) {
        return false;
    }

    std::string schema_string;
    LogMessage message;
    ProtoMessage::GenerateDescriptorString(message.GetDescriptor()->file(),
                                           &schema_string);
    // set schema;
    io::SchemaDescriptor schema;
    schema.set_type("PB");
    schema.set_description(schema_string);
    schema.set_record_name("LogMessage");
    schema.set_charset_encoding("UTF-8");

    std::string sd;
    schema.SerializeToString(&sd);

    m_create_tablet.reset(new CreateTablet());
    if (!m_create_tablet->Setup(sd,
                                table_name,
                                output_dir + table_name)) {
        return false;
    }

    int64_t total_size = 0;
    if (!GetFilesByPattern(input.c_str(), &m_input_files, &total_size)) {
        LOG(ERROR) << "Get input [ " << input << " ] files error ";
        return false;
    }
    return true;
}

bool ParseLogTool::Parse() {
    uint32_t number = 0;
    for (size_t i = 0; i < m_input_files.size(); ++i) {
        if (!ParseOneFile(m_input_files[i])) {
            ++number;
        }
    }
    return number == 0;
}

bool ParseLogTool::ParseOneFile(const std::string& file) {
    ParseLog parse;
    if (!parse.Open(file)) {
        LOG(ERROR) << "parser open fail: " << file;
        return false;
    }

    while (!parse.IsFinished()) {
        LogMessage message;
        if (!parse.GetNextMessage(&message)) {
            LOG(ERROR) << "fail to GetNextMessage()";
            return false;
        }
        std::string value;
        message.SerializeToString(&value);
        m_create_tablet->WriteMessage(value);
    }
    return true;
}

bool ParseLogTool::Close() {
    return m_create_tablet->Flush();
}

}  // namespace gunir
