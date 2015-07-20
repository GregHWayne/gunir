cc_library(
    name = 'leafnode_impl',
    srcs = [
        'leafnode_impl.cc',
        'remote_leafnode.cc'
    ],
    deps = [
        ':worker_manager',
        ':sender_manager',
        '//gunir/master:master_client',
        '//gunir/proto:leafnode_rpc_proto',
        '//gunir/proto:master_rpc_proto',
        '//gunir/proto:proto_helper',
        '//gunir/utils:env_utils',
        '//gunir:rpc_server',
        '//gunir:gunir_flags',
        '//toft/system/threading:threading',
        '//toft/system/timer:timer'
    ]
)

cc_library(
    name = 'leafnode_client',
    srcs = 'leafnode_client.cc',
    deps = [
        '//gunir/proto:leafnode_rpc_proto',
        '//gunir:rpc_client'
    ]
)

cc_library(
    name = 'leafnode_entry',
    srcs = 'leafnode_entry.cc',
    deps = [
        ':leafnode_impl',
        '//gunir:gunir_entry'
    ]
)

cc_library(
    name = 'task_container',
    srcs = 'task_container.cc',
    deps = [
        '//gunir/io:data_holder',
        '//gunir/io:io',
        '//gunir/proto:stemnode_rpc_proto',
        '//gunir/stemnode:stemnode_client',
        '//gunir/utils:filename_tool',
        '//toft/base/string:string'
    ]
)

cc_library(
    name = 'server_thread',
    srcs = 'server_thread.cc',
    deps = [
        '//gunir/proto:task_proto', 
        '//gunir/proto:master_rpc_proto',
        '//toft/system/threading:threading'
    ]
)
cc_library(
    name = 'worker_manager',
    srcs = [
        'worker_manager.cc',
        'worker_thread.cc',
        'task_worker.cc',
    ],
    deps = [
        ':server_thread',
        ':task_container',
        '//gunir/utils:message_utils',
        '//gunir:gunir_flags',
        '//gunir/compiler:executor',
        '//gunir/io:tablet_scanner',
        '//gunir/io:tablet_schema_proto',
        '//toft/system/threading:threading'
    ]
)

cc_library(
    name = 'sender_manager',
    srcs = [
        'sender_thread.cc',
        'sender_manager.cc'
    ],
    deps = [
        ':server_thread',
        '//gunir/utils:message_utils'
    ]
)
