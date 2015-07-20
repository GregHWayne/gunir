// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/client/client_entry.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "gunir/client/remote_client.h"

namespace gunir {
namespace client {

ClientEntry::ClientEntry()
    : m_client(new RemoteClient()) {}

ClientEntry::~ClientEntry() {}

bool ClientEntry::StartServer() {
    gunir::QueryShell::Instance()->Init(m_client.get());
    return true;
}

void ClientEntry::ShutdownServer() {
    m_client.reset();
}

bool ClientEntry::Run() {
    gunir::QueryShell::Instance()->Run();
    return true;
}

} // namespace client
} // namespace gunir
