// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "gunir/utils/env_utils.h"

#include <stdio.h>
#include <stdlib.h>

#include "toft/base/scoped_ptr.h"
#include "toft/storage/path/path_ext.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

DECLARE_string(log_dir);

namespace gunir {
namespace utils {

std::string GetBinaryLocationDir() {
    char exec_full_path[1024] = {'\0'};
    readlink ("/proc/self/exe", exec_full_path, 1024);
    VLOG(5) << "current binary location: " << exec_full_path;

    std::string full_dir;
    toft::SplitStringPath(exec_full_path, &full_dir, NULL);
    return full_dir;
}

std::string GetCurrentLocationDir() {
    char current_path[1024] = {'\0'};
    std::string current_dir;

    if (getcwd(current_path, 1024)) {
        current_dir = current_path;
    }
    return current_dir;
}

std::string GetValueFromeEnv(const std::string& env_name) {
    if (env_name.empty()) {
        return "";
    }

    const char* env = getenv(env_name.c_str());
    if (!env) {
        VLOG(5) << "fail to fetch from env: " << env_name;
        return "";
    }
    return env;
}

bool ExecuteShellCmd(const std::string cmd, std::string* ret_str) {
    char output_buffer[80];
    FILE *fp = popen(cmd.c_str(), "r");
    if (!fp) {
        LOG(ERROR) << "fail to execute cmd: " << cmd;
        return false;
    }
    fgets(output_buffer, sizeof(output_buffer), fp);
    pclose(fp);
    if (ret_str) {
        *ret_str = std::string(output_buffer);
    }
    return true;
}

std::string GetLocalHostAddr() {
    std::string cmd =
        "/sbin/ifconfig | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}'";
    std::string addr;
    if (!ExecuteShellCmd(cmd, &addr)) {
        LOG(ERROR) << "fail to fetch local host addr";
    } else if (addr.length() > 1) {
        addr.erase(addr.length() - 1, 1);
    }
    return addr;
}

std::string GetLocalHostName() {
    const uint32_t kMaxHostNameSize = 255;
    char str[kMaxHostNameSize + 1];
    if (0 != gethostname(str, kMaxHostNameSize + 1)) {
        LOG(FATAL) << "gethostname fail";
        exit(1);
    }
    std::string hostname(str);
    return hostname;
}

void SetupLog(const std::string& name) {
    // log info/warning/error/fatal to gunir.log
    // log warning/error/fatal to gunir.wf

    std::string program_name = "gunir";
    if (!name.empty()) {
        program_name = name;
    }

    if (FLAGS_log_dir.empty()) {
        FLAGS_log_dir = GetBinaryLocationDir();
    }
    std::string log_filename = FLAGS_log_dir + "/" + program_name + ".INFO.";
    std::string wf_filename = FLAGS_log_dir + "/" + program_name + ".WARNING.";
    google::SetLogDestination(google::INFO, log_filename.c_str());
    google::SetLogDestination(google::WARNING, wf_filename.c_str());
    google::SetLogDestination(google::ERROR, "");
    google::SetLogDestination(google::FATAL, "");

    google::SetLogSymlink(google::INFO, program_name.c_str());
    google::SetLogSymlink(google::WARNING, program_name.c_str());
    google::SetLogSymlink(google::ERROR, "");
    google::SetLogSymlink(google::FATAL, "");
}

} // namespace utils
} // namespace gunir
