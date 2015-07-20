// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "toft/base/binary_version.h"

#include "gunir/utils/create_table_tool.h"
#include "gunir/utils/message_utils.h"

DEFINE_string(gunir_table_name, "", "define table's name");

DEFINE_string(gunir_input_files, "", "define inputs' pattern");
DEFINE_string(gunir_input_schema, "", "define inputs' schema");
DEFINE_string(gunir_input_message, "gunir", "define inputs' nessage name");
DEFINE_string(gunir_input_encoding, "UTF-8", "define input's charset encoding");

DEFINE_string(gunir_output_dir, "", "define output dir for result");

int main(int argc, char** argv) {
    toft::SetupBinaryVersion();
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::google::InitGoogleLogging(argv[0]);

    gunir::compiler::CreateTableStmt stmt;
    stmt.mutable_table_name()->set_char_string(FLAGS_gunir_table_name);
    stmt.mutable_input_path()->set_char_string(FLAGS_gunir_input_files);
    stmt.mutable_table_schema()->set_char_string(FLAGS_gunir_input_schema);
    stmt.mutable_message_name()->set_char_string(FLAGS_gunir_input_message);
    stmt.mutable_charset_encoding()->set_char_string(
        FLAGS_gunir_input_encoding);

    std::string err_string;
    if (!gunir::InitCreateTableStmt(&stmt, &err_string)) {
        LOG(ERROR) << err_string;
        return -1;
    }

    std::string output_path = FLAGS_gunir_output_dir;
    if (output_path == "" || output_path == "./") {
        LOG(ERROR) << "Output path can't be empty or current dir";
        return -1;
    }

    gunir::CreateTableTool create_table(&stmt);
    bool ret = create_table.LocalRun(output_path);

    return (ret ? 0 : -1);
}
