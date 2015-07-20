// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef GUNIR_UTILS_ENV_UTILS_H
#define GUNIR_UTILS_ENV_UTILS_H

#include <map>
#include <string>

#include "leveldb/env.h"

namespace gunir {
namespace utils {

std::string GetBinaryLocationDir();

std::string GetCurrentLocationDir();

std::string GetValueFromeEnv(const std::string& env_name);

std::string GetLocalHostAddr();

std::string GetLocalHostName();

bool ExecuteShellCmd(const std::string cmd,
                     std::string* ret_str = NULL);

void SetupLog(const std::string& program_name);

} // namespace gunir
} // namespace tera

#endif // GUNIR_UTILS_ENV_UTILS_H
