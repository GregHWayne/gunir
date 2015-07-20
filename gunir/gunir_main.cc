// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <signal.h>

#include <iostream>

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "toft/base/binary_version.h"
#include "toft/base/scoped_ptr.h"

#include "gunir/client/client_entry.h"
#include "gunir/gunir_entry.h"
#include "gunir/leafnode/leafnode_entry.h"
#include "gunir/stemnode/stemnode_entry.h"
#include "gunir/master/master_entry.h"
#include "gunir/utils/env_utils.h"

DECLARE_string(gunir_role);

bool g_quit = false;

static void SignalIntHandler(int sig) {
    LOG(INFO) << "receive interrupt signal from user, will stop";
    g_quit = true;
}

gunir::GunirEntry* SwitchGunirEntry() {
    const std::string& server_name = FLAGS_gunir_role;

    if (server_name == "master") {
        return new gunir::master::MasterEntry();
    } else if (server_name == "stemnode") {
        return new gunir::stemnode::StemNodeEntry();
    } else if (server_name == "leafnode") {
        return new gunir::leafnode::LeafNodeEntry();
    } else if (server_name == "client") {
        return new gunir::client::ClientEntry();
    }
    LOG(ERROR) << "FLAGS_gunir_role should be one of ("
        << "master | leafnode"
        << "), not : " << FLAGS_gunir_role;
    return NULL;
}

int main(int argc, char** argv) {
    toft::SetupBinaryVersion();
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::google::InitGoogleLogging(argv[0]);

    gunir::utils::SetupLog(FLAGS_gunir_role);

    if (argc > 1) {
        std::string ext_cmd = argv[1];
        if (ext_cmd == "version" || ext_cmd == "-version"
            || ext_cmd == "--version") {
//             PrintSystemVersion();
            return 0;
        }
    }

    signal(SIGINT, SignalIntHandler);
    signal(SIGTERM, SignalIntHandler);

    toft::scoped_ptr<gunir::GunirEntry> entry(SwitchGunirEntry());
    if (entry.get() == NULL) {
        return -1;
    }

    if (!entry->Start()) {
        return -1;
    }

    while (!g_quit) {
        if (!entry->Run()) {
            LOG(ERROR) << "Server run error ,and then exit now ";
            break;
        }
        signal(SIGINT, SignalIntHandler);
        signal(SIGTERM, SignalIntHandler);
    }

    if (!entry->Shutdown()) {
        return -1;
    }

    return 0;
}
