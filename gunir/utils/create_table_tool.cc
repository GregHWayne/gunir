// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#include "gunir/utils/create_table_tool.h"

#include <fstream>
#include <string>
#include <vector>

#include "toft/base/string/number.h"
#include "toft/crypto/uuid/uuid.h"
#include "toft/storage/file/file.h"

#include "gunir/io/tablet_schema.pb.h"
#include "gunir/utils/create_table_helper.pb.h"
#include "gunir/utils/create_tablet.h"
#include "gunir/utils/filename_tool.h"

DECLARE_string(identity);
DECLARE_string(role);
DECLARE_string(log_dir);

DEFINE_bool(gunir_csv_create_table_ignore_first_line, true, "");

namespace gunir {

CreateTableTool::CreateTableTool(const compiler::CreateTableStmt* create_stmt)
    : m_create_stmt(create_stmt) {}

CreateTableTool::~CreateTableTool() {}

bool CreateTableTool::PrepareInputInfo(const std::string& output_prefix,
                                       std::string* arg) {
    // clear output dir
    if (!RemoveFile(output_prefix, true)) {
        return false;
    }

    if (!CreateDir(output_prefix)) {
        return false;
    }

    // set schema;
    io::SchemaDescriptor schema;
    schema.set_type(m_create_stmt->table_type().char_string());
    schema.set_description(m_create_stmt->table_schema().char_string());
    schema.set_record_name(m_create_stmt->message_name().char_string());
    schema.set_charset_encoding(
        m_create_stmt->charset_encoding().char_string());

    std::string sd;
    schema.SerializeToString(&sd);

    const std::string& table_name = m_create_stmt->table_name().char_string();
    InputInfo input_info;
    input_info.set_table_name(table_name);
    input_info.set_tablet_file_prefix(output_prefix + table_name);
    input_info.set_schema_descriptor(sd);

    std::string file_name = table_name + "_" + toft::CreateCanonicalUUIDString();

    std::ofstream output(file_name.c_str());
    input_info.SerializeToOstream(&output);
    output.close();

    return true;
}

bool CreateTableTool::LocalRun(const std::string& output_prefix) {
    if (!RemoveFile(output_prefix, true)) {
        return false;
    }

    if (!CreateDir(output_prefix)) {
        return false;
    }

    io::SchemaDescriptor schema;
    schema.set_type(m_create_stmt->table_type().char_string());
    schema.set_description(m_create_stmt->table_schema().char_string());
    schema.set_record_name(m_create_stmt->message_name().char_string());
    schema.set_charset_encoding(
        m_create_stmt->charset_encoding().char_string());

    std::string sd;
    schema.SerializeToString(&sd);
    const std::string& table_name = m_create_stmt->table_name().char_string();

    CreateTablet create_tablet;
    if (!create_tablet.Setup(sd, table_name, output_prefix + table_name)) {
        LOG(ERROR) << "fail to setup tablet creator for " << table_name;
        return false;
    }

    std::string input_path = m_create_stmt->input_path().char_string();
    std::vector<std::string> input_files;
    int64_t total_size = 0;
    if (!GetFilesByPattern(input_path, &input_files, &total_size)) {
        LOG(ERROR) << "Get input [ " << input_path << " ] files error ";
        return false;
    }

    for (uint32_t i = 0; i < input_files.size(); ++i) {
        if (!ParseSingleFile(&create_tablet, input_files[i])) {
            LOG(ERROR) << "fail to consume file: " << input_files[i];
        }
    }

    if (!create_tablet.Flush()) {
        LOG(ERROR) << "fail to flush tablet";
    }
    return true;
}

bool CreateTableTool::RemoteRun(const std::string& output_prefix) {
    return false;
}

bool CreateTableTool::ParseSingleFile(CreateTablet* create_tablet,
                                      const std::string& filename) {
    toft::scoped_ptr<toft::File> file(toft::File::Open(filename, "r"));
    if (file.get() == NULL) {
        LOG(ERROR) << "fail to open file: " << filename;
        return false;
    }
    std::string line;
    if (FLAGS_gunir_csv_create_table_ignore_first_line) {
        file->ReadLine(&line);
    }
    while (file->ReadLine(&line)) {
        if (!create_tablet->WriteMessage(line)) {
            LOG(ERROR) << "fail to dump line for " << filename;
        }
    }
    return true;
}

} // namespace gunir
