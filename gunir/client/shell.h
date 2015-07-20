// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_CLIENT_SHELL_H
#define  GUNIR_CLIENT_SHELL_H

#include <string>

#include "toft/base/singleton.h"

namespace gunir {
namespace client {
class BaseClient;
}  // namespace client

char** CompletionCmd(const char *text, int start, int end);

class QueryShell : public toft::SingletonBase<QueryShell> {
    friend class SingletonBase<QueryShell>;
    QueryShell();

public:
    virtual ~QueryShell();

    void Init(client::BaseClient* client);

    int Run();

    int Run(const char* read_line);

private:
    size_t GetHyphenIndex(const std::string& cur_line);

    void InitializeReadLine();

private:
    client::BaseClient* m_client;
    std::string m_head_name;
    std::string m_read_line;
};

} // namespace gunir

#endif  // GUNIR_CLIENT_SHELL_H
