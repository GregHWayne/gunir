// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef GUNIR_CLIENT_CLIENT_ENTRY_H
#define GUNIR_CLIENT_CLIENT_ENTRY_H


#include "toft/base/scoped_ptr.h"

#include "gunir/gunir_entry.h"
#include "gunir/client/shell.h"

namespace gunir {
namespace client {

class RemoteClient;
class QueryShell;

class ClientEntry : public GunirEntry {
public:
    ClientEntry();
    ~ClientEntry();

    bool StartServer();
    void ShutdownServer();
    bool Run();

private:
    toft::scoped_ptr<RemoteClient> m_client;
};

} // namespace client
} // namespace gunir

#endif // GUNIR_CLIENT_CLIENT_ENTRY_H
