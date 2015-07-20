proto_library(
    name = 'scheduler_plan_proto',
    srcs = 'scheduler_plan.proto',
    deps = [
        '//gunir/proto:task_proto',
        '//gunir/proto:server_info_proto'
    ]
)

cc_library(
    name = 'master_impl',
    srcs = [
        'master_impl.cc',
        'remote_master.cc'
    ],
    deps = [
        ':server_manager',
        ':table_manager',
        ':job',
        ':job_scheduler',
        ':job_emitter',
        '//gunir/proto:master_rpc_proto',
        '//gunir/utils:env_utils',
        '//gunir:rpc_server',
        '//toft/system/threading:threading',
        '//toft/system/timer:timer'
    ]
)

cc_library(
    name = 'master_client',
    srcs = 'master_client.cc',
    deps = [
        '//gunir/proto:master_rpc_proto',
        '//gunir:gunir_flags',
        '//gunir:rpc_client'
    ]
)

cc_library(
    name = 'master_entry',
    srcs = 'master_entry.cc',
    deps = [
        ':master_impl',
        '//gunir:gunir_entry'
    ]
)

cc_library(
    name = 'server_manager',
    srcs = [
        'server_state.cc',
        'leaf_server_state.cc',
        'stem_server_state.cc',
        'server_manager.cc',
    ],
    deps = [
        '//gunir/proto:server_info_proto',
        '//gunir/proto:master_rpc_proto',
        '//toft/system/timer:timer'
    ]
)

cc_library(
    name = 'task',
    srcs = [
        'inter_task.cc',
        'leaf_task.cc',
        'task.cc',
        'task_manager.cc',
    ],
    deps = [
        ':scheduler_plan_proto',
        '//gunir/proto:task_proto',
        '//gunir/utils:bit_map',
        '//gunir:gunir_flags',
        '//toft/system/threading:threading',
    ],
)

cc_library(
    name = 'job',
    srcs = [
        'job.cc',
        'job_manager.cc',
        'job_state.cc',
    ],
    deps = [
        ':job_analyser',
        ':task',
        '//gunir/compiler:compiler_utils',
        '//gunir/proto:master_rpc_proto',
        '//gunir/utils:message_utils',
        '//gunir:gunir_flags',
        '//toft/system/atomic:atomic',
        '//toft/system/threading:threading',
        '//toft/system/timer:timer',
    ],
    extra_cppflags = '-std=c++11',
)

cc_library(
    name = 'job_analyser',
    srcs = [
        'job_analyser.cc',
    ],
    deps = [
        ':table_manager',
        '//gunir/proto:job_proto',
        '//gunir/compiler:analyzer',
        '//gunir/compiler:job_plan',
    ],
)

cc_library(
    name = 'job_scheduler',
    srcs = [
        'job_scheduler.cc',
        'default_scheduler.cc'
    ],
    deps = [
        ':scheduler_plan_proto',
        ':job',
        ':server_manager',
        '//toft/system/threading:threading',
        '//thirdparty/glog:glog',
    ]
)

cc_library(
    name = 'job_emitter',
    srcs = 'job_emitter.cc',
    deps = [
        ':job',
        ':server_manager',
        '//gunir:gunir_flags',
        '//gunir/proto:leafnode_rpc_proto',
        '//gunir/proto:stemnode_rpc_proto',
        '//gunir/leafnode:leafnode_client',
        '//gunir/stemnode:stemnode_client',
        '//toft/system/threading:threading',
        '//toft/system/atomic:atomic'
    ]
)

cc_library(
    name = 'table_manager',
    srcs = 'table_manager.cc',
    deps = [
        '//gunir/proto:table_proto',
        '//thirdparty/leveldb:leveldb'
    ]
)
