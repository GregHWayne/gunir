cc_library(
    name = 'stemnode_impl',
    srcs = [
        'stemnode_impl.cc',
        'remote_stemnode.cc'
    ],
    deps = [
        ':worker_manager',
        '//gunir/master:master_client',
        '//gunir/proto:server_info_proto',
        '//gunir/proto:stemnode_rpc_proto',
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
    name = 'stemnode_client',
    srcs = 'stemnode_client.cc',
    deps = [
        '//gunir/proto:stemnode_rpc_proto',
        '//gunir:rpc_client'
    ]
)

cc_library(
    name = 'stemnode_entry',
    srcs = 'stemnode_entry.cc',
    deps = [
        ':stemnode_impl',
        '//gunir:gunir_entry'
    ]
)

cc_library(
    name = 'result_manager',
    srcs = [
        'result_info.cc',
        'result_manager.cc'
    ],
    deps = [
        '//gunir:gunir_flags',
        '//gunir/proto:stemnode_rpc_proto',
        '//gunir/io:data_holder',
        '//toft/base/string:string',
        '//toft/system/threading:threading',
    ] 
)

cc_library(
    name = 'task_container',
    srcs = 'task_container.cc',
    deps = [
        '//gunir/io:io',
        '//gunir/utils:filename_tool',
        '//gunir/proto:task_proto'
    ]
)

cc_library(
    name = 'worker_manager',
    srcs =[
        'task_worker.cc',
        'worker_thread.cc',
        'worker_manager.cc'
    ],
    deps = [
        ':task_container',
        ':result_manager',
        '//gunir/proto:master_rpc_proto',
        '//gunir/proto:stemnode_rpc_proto',
        '//gunir/io:io',
        '//gunir/io:slice_scanner',
        '//gunir/utils:message_utils',
        '//toft/system/threading:threading'
    ]
)
