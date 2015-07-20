// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef GUNIR_PROTO_PROTO_HELPER_H
#define GUNIR_PROTO_PROTO_HELPER_H

#include <stdint.h>

#include <string>

#include "gunir/proto/job.pb.h"

namespace gunir {

std::string StatusCodeToString(int32_t status);

bool TableInfoPBToString(const TableInfo& message, std::string* output);

bool StringToTableInfoPB(const std::string& str, TableInfo* message);

} // namespace gunir

#endif // GUNIR_PROTO_PROTO_HELPER_H
