// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "gunir/proto/proto_helper.h"

#include <stdio.h>

#include "gunir/proto/status_code.pb.h"

namespace gunir {

std::string StatusCodeToString(int32_t code) {
    switch (code) {
    case Unknown:
        return "Unknown";
    case kMasterNotInited:
        return "kMasterNotInited";
    case kMasterIsBusy:
        return "kMasterIsBusy";
    case kMasterIsSecondary:
        return "kMasterIsSecondary";
    case kMasterIsReadonly:
        return "kMasterIsReadonly";
    case kMasterOnRestore:
        return "kMasterOnRestore";
    case kMasterOnWait:
        return "kMasterOnWait";
    case kMasterIsRunning:
        return "kMasterIsRunning";

    // server node
    case kServerNotInited:
        return "kServerNotInited";
    case kServerIsBusy:
        return "kServerIsBusy";
    case kServerIsIniting:
        return "kServerIsIniting";
    case kServerTimeout:
        return "kServerTimeout";
    case kServerIsRunning:
        return "kServerIsRunning";

    // ACL & system
    case kIllegalAccess:
        return "kIllegalAccess";
    case kNotPermission:
        return "kNotPermission";
    case kIOError:
        return "kIOError";

    //// master rpc ////

    case kMasterOk:
        return "kMasterOk";
    case kMasterOnWork:
        return "kMasterOnWork";

    // register
    case kInvalidSequenceId:
        return "kInvalidSequenceId";
    case kInvalidServerInfo:
        return "kInvalidServerInfo";

    // report
    case kServerNotRegistered:
        return "kServerNotRegistered";
    case kServerNotExist:
        return "kServerNotExist";

    // cmdctrl
    case kInvalidArgument:
        return "kInvalidArgument";


    // response
    case kServerOk:
        return "kServerOk";

    // meta table
    case kMetaError:
        return "kMetaError";

    // RPC
    case kRPCError:
        return "kRPCError";
    case kServerError:
        return "kServerError";
    case kConnectError:
        return "kConnectError";
    case kRPCTimeout:
        return "kRPCTimeout";

    default:
        ;
    }
    char num[16];
    snprintf(num, 16, "%d", code);
    num[15] = '\0';
    return num;
}

bool TableInfoPBToString(const TableInfo& message, std::string* output) {
    if (!message.IsInitialized()) {
        LOG(ERROR) << "missing required fields: "
                << message.InitializationErrorString();
        return false;
    }
    if (!message.AppendToString(output)) {
        LOG(ERROR) << "fail to convert to string";
        return false;
    }

    return true;
}

bool StringToTableInfoPB(const std::string& str, TableInfo* message) {
    if (!message->ParseFromArray(str.c_str(), str.size())) {
        LOG(WARNING) << "missing required fields: "
            << message->InitializationErrorString();;
        return false;
    }
    return true;
}

} // namespace gunir
