cc_library(
    name = 'gunir_flags',
    srcs = 'gunir_flags.cc',
)

cc_library(
    name = 'rpc_client',
    srcs = 'rpc_client.cc',
    deps = [
        '//gunir/utils:ip_address',
        '//toft/system/threading:threading',
        '//trident:trident',
        '//thirdparty/gflags:gflags',
        '//thirdparty/glog:glog'
    ]
)

cc_library(
    name = 'rpc_server',
    srcs = 'rpc_server.cc',
    deps = [
        '//gunir/utils:ip_address',
        '//toft/system/timer:timer',
        '//trident:trident',
        '//thirdparty/gflags:gflags',
        '//thirdparty/glog:glog'
    ]
)

cc_library(
    name = 'gunir_entry',
    srcs = 'gunir_entry.cc',
    deps = [
        '//toft/system/threading:threading' 
    ]
)

cc_binary(
    name = 'gunir_main',
    srcs = 'gunir_main.cc',
    deps = [
        '//gunir/master:master_entry',
        '//gunir/stemnode:stemnode_entry',
        '//gunir/leafnode:leafnode_entry',
        '//gunir/client:client_entry',
        '//gunir/utils:env_utils',
        '//gunir:gunir_flags',
        '//thirdparty/gflags:gflags',
        '//thirdparty/glog:glog',
        '//toft/base:binary_version'
    ]
)
