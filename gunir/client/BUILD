cc_library(
    name = 'base_client',
    srcs = 'base_client.cc',
    deps = [
        ':user_table',
        '//gunir/compiler/parser:parser',
        '//gunir/compiler/parser:table_schema',
        '//gunir/io:pb_assembler',
        '//gunir/io:short_display_reader',
        '//gunir/proto:master_rpc_proto',
        '//gunir/proto:proto_helper',
        '//gunir/utils:filename_tool',
        '//gunir/utils:proto_message',
        '//gunir:gunir_flags',
        '//toft/system/threading:threading',
        '//toft/storage/file:file'
    ]
)

cc_library(
    name = 'remote_client',
    srcs = 'remote_client.cc',
    deps = [
        ':base_client',
        '//gunir/master:master_client',
        '//toft/system/threading:threading'
    ]
)

cc_library(
    name = 'shell',
    srcs = 'shell.cc',
    deps = [
        ':base_client',
        ':remote_client',
        '//thirdparty/readline:readline'
    ]
)

cc_library(
    name = 'client_entry',
    srcs = 'client_entry.cc',
    deps = [
        ':shell',
        ':remote_client'
    ]
)

cc_library(
    name = 'user_table',
    srcs = 'user_table.cc',
    deps = [
        '//gunir/proto:table_proto',
        '//gunir/io:tablet_reader',
        '//gunir/utils:filename_tool',
        '//toft/base/string:string',
        '//toft/storage/file:file',
    ]
)
