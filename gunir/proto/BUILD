proto_library(    
    name = 'error_code_proto',    
    srcs = 'error_code.proto',
)

proto_library(    
    name = 'tablet_proto',    
    srcs = 'tablet.proto'
)

proto_library(    
    name = 'table_proto',    
    srcs = 'table.proto',    
    deps = [        
        ":tablet_proto",    
    ],
)

proto_library(    
    name = 'task_proto',    
    srcs = 'task.proto',    
    deps = [        
        '//gunir/compiler/parser:plan_proto',    
    ]
)

proto_library(    
    name = 'job_proto',    
    srcs = 'job.proto',    
    deps = [        
        '//gunir/compiler/parser:select_stmt_proto',        
        '//gunir/proto:table_proto',        
        '//gunir/compiler/parser:plan_proto',    
    ]
)


proto_library(
    name = 'status_code_proto',
    srcs = 'status_code.proto'
)

proto_library(
    name = 'server_info_proto',
    srcs = 'server_info.proto',
    deps = [
        ':status_code_proto'
    ]
)

proto_library(
    name = 'leafnode_info_proto',
    srcs = 'leafnode_info.proto',
    deps = ':status_code_proto'
)

proto_library(
    name = 'master_rpc_proto',
    srcs = 'master_rpc.proto',
    deps = [
        ':status_code_proto',
        ':server_info_proto',
        ':job_proto',
        '//trident:rpc_option_proto',
        '//gunir/compiler/parser:select_stmt_proto'
    ]
)

proto_library(
    name = 'leafnode_rpc_proto',
    srcs = 'leafnode_rpc.proto',
    deps = [
        ':status_code_proto',
        ':task_proto',
        '//trident:rpc_option_proto'
    ]
)

proto_library(
    name = 'stemnode_rpc_proto',
    srcs = 'stemnode_rpc.proto',
    deps = [
        ':status_code_proto',
        ':task_proto',
        '//trident:rpc_option_proto'
    ]
)

cc_library(
    name = 'proto_helper',
    srcs = 'proto_helper.cc',
    deps = [
        ':status_code_proto',
        ':job_proto',
    ]
)
