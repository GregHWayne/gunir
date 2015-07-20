// Copyright (C) 2014, Baidu Inc.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <signal.h>

#include "common/base/scoped_ptr.h"
#include "tera.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "bigquery/client/base_client.h"
#include "bigquery/client/shell.h"
#include "bigquery/client/tera/tera_client.h"

bool g_quit = false;

static void SignalIntHandler(int sig) {
    g_quit = true;
}

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::google::InitGoogleLogging(argv[0]);

    signal(SIGINT, SignalIntHandler);
    signal(SIGTERM, SignalIntHandler);

    scoped_ptr< ::tera::Client> tera_cli_impl(::tera::Client::NewClient("./tera.flag"));
    bigquery::client::TeraClient client(tera_cli_impl.get());
    bigquery::QueryShell shell;
    shell.Init(&client);

    while (!g_quit) {
        if (shell.Run()) {
            LOG(ERROR) << "Server run error, and then exit now ";
            break;
        }
    }

    return 0;
}
