// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "gunir/utils/gflag_parser.h"
#include "gunir/utils/parse_log_tool.h"

DEFINE_string(gunir_table_name, "", "define table's name");
DEFINE_string(gunir_input_files, "", "define inputs' pattern");
DEFINE_string(gunir_output_dir, "", "define output dir for result");

using namespace gunir;

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::google::InitGoogleLogging(argv[0]);

    if (FLAGS_gunir_table_name == ""
        || FLAGS_gunir_input_files == ""
        || FLAGS_gunir_output_dir == "") {
        LOG(ERROR) << "Usage : please set (--gunir_table_name && --gunir_input_files && --gunir_output_dir) 's value "; // NOLINT
        return 1;
    }

    ParseLogTool parse;
    if (!parse.Open(FLAGS_gunir_input_files,
                    FLAGS_gunir_table_name,
                    FLAGS_gunir_output_dir)) {
        LOG(ERROR) << "Open failed " << FLAGS_gunir_input_files
            << "," << FLAGS_gunir_output_dir
            << "," << FLAGS_gunir_table_name;
        return 1;
    }

    if (!parse.Parse()) {
        LOG(ERROR) << "Parse [" << FLAGS_gunir_input_files << "] failed ";
        return 1;
    }

    if (!parse.Close()) {
        LOG(ERROR) << "Close [" << FLAGS_gunir_input_files << "] failed ";
        return 1;
    }
    return 0;
}


