// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <cstdio>
#include <iostream>

#include "gunir/compiler/compiler_test_helper.h"

#include "gunir/readline/history.h"
#include "gunir/readline/readline.h"

#define NONE "\033[m"
#define GREEN "\033[0;32;32m"
#define RED "\033[0;32;31m"
#define LIGHT_GREEN "\033[1;32m"
#define LIGHT_RED "\033[1;31m"

static  key_color_info key_color_table[] = {
    {"gunir", LIGHT_GREEN, 6},
    {"create", LIGHT_GREEN, 6},
    {"drop", LIGHT_GREEN, 4},
    {"select", LIGHT_GREEN, 6},
    {"as", LIGHT_GREEN, 2},
    {"from", LIGHT_GREEN, 4},
    {"where", LIGHT_GREEN, 5},
    {"within", LIGHT_GREEN, 6},
    {"record", LIGHT_GREEN, 6},
    {"groupby", LIGHT_GREEN, 7},
    {"having", LIGHT_GREEN, 6},
    {"orderby", LIGHT_GREEN, 7},
    {"limit", LIGHT_GREEN, 5},
    {"show", LIGHT_GREEN, 4},
    {"help", LIGHT_GREEN, 4},
    {"quit", LIGHT_GREEN, 4},
    {"exit", LIGHT_GREEN, 4},
    {"CONCAT", LIGHT_GREEN, 6},
    {"SUBSTR", LIGHT_GREEN, 6},
    {"LENGTH", LIGHT_GREEN, 6},
    {"SUM", LIGHT_GREEN, 3},
    {"MAX", LIGHT_GREEN, 3},
    {"MIN", LIGHT_GREEN, 3},
    {"AVG", LIGHT_GREEN, 3},
    {"COUNT", LIGHT_GREEN, 5},
    {NULL, NULL, 0}
};

void init() {
    rl_set_key_color_table(key_color_table);
}

DEFINE_bool(stdin_client, false, "client is interactive");
DECLARE_bool(gunir_local_run);

namespace gunir {
namespace compiler {

static const char* kDefaultDocumentProto =
    "./testdata/client/Document.proto";
static const char* kDefaultDocumentData =
    "./testdata/client/DocumentTabletData.dat";

void Start() {
    const char* logo = "gunir>";
    toft::scoped_ptr<char> querys;

    do {
        querys.reset(readline(logo));
        if (querys == NULL) {
            break;
        }

        const char* query_start = querys.get();
        while (*query_start == ' ' || *query_start == '\t') {
            query_start++;
        }

        if (*query_start == '\0') {
            continue;
        }

        add_history(querys.get());

        Executor executor;
        std::vector<io::Scanner*> scanners;

        if (!ProcessQuery(querys.get(),
                          kDefaultDocumentProto,
                          kDefaultDocumentData,
                          &executor,
                          &scanners)) {
            continue;
        }

        io::Slice* slice;
        while (executor.NextSlice(&slice));

        for (size_t i = 0; i < scanners.size(); ++i) {
            delete scanners[i];
        }
    } while (true);
}

} // namespace compiler
} // namespace gunir

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);

    if (!FLAGS_stdin_client) {
        return 0;
    }

    FLAGS_log_dir = "./logs/";
    FLAGS_gunir_local_run = true;

    init();
    ::gunir::compiler::Start();
    return 0;
}
