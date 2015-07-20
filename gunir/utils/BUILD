proto_library(
    name = 'create_table_helper_proto',
    srcs = 'create_table_helper.proto'
)

cc_library(
    name = 'proto_message',
    srcs = [
        'proto_message.cc',
    ],
    deps = [
        ':filename_tool',
        '//gunir/io:column_metadata_proto',
        '//toft/storage/path:path',
        '//thirdparty/glog:glog',
        '//thirdparty/protobuf:protobuf',
    ],
)

cc_library(
    name = 'csv_helper',
    srcs = [
        'csv_helper.cc',
    ],
    deps = [
        '//gunir:gunir_flags',
        '//toft/base/string:string',
        '//toft/storage/file:file',
        '//thirdparty/protobuf:protobuf',
    ],
)

cc_test(
    name = 'proto_message_test',
    srcs = ['proto_message_test.cc'],
    deps = [':proto_message'],
    testdata = 'testdata',
)

cc_library(
    name = 'bit_map',
    srcs = 'bit_map.cc'
)

cc_library(
    name = 'ip_address',
    srcs = 'ip_address.cc',
    deps = [
        '//toft/base/string:string'
    ]
)

cc_library(
    name = 'message_utils',
    srcs = 'message_utils.cc',
    deps = [
        ':csv_helper',
        ':proto_message',
        '//gunir/utils/encoding:encoding',
        '//toft/base/string:string',
        '//toft/storage/file:file',
        '//gunir/compiler/parser:plan_proto',
        '//gunir/proto:job_proto',
        '//gunir/proto:task_proto',
        '//gunir/proto:master_rpc_proto',
    ]
)

cc_library(
    name = 'create_tablet',
    srcs = [
        'create_tablet.cc',
    ],
    deps = [
        '//gunir/io:table_builder',
    ]
)

cc_library(
    name = 'create_table_tool',
    srcs = [
        'create_table_tool.cc',
    ],
    deps = [
        ':create_table_helper_proto',
        ':filename_tool',
        ':create_tablet',
        '//gunir/io:tablet_schema_proto',
        '//gunir:gunir_flags',
        '//gunir/compiler/parser:select_stmt_proto',
        '//toft/base/string:string',
        '//toft/crypto/uuid:uuid',
        '//toft/storage/file:file',
    ],
)

cc_library(
    name = 'filename_tool',
    srcs = 'filename_tool.cc',
    deps = [
        '//gunir:gunir_flags',
        '//gunir/proto:tablet_proto',
        '//gunir/proto:table_proto',
        '//toft/base/string:string',
        '//toft/storage/file:file',
        '//toft/storage/path:path',
        '//toft/system/threading:threading',
        '//toft/crypto/uuid:uuid',
        '//thirdparty/gflags:gflags',
    ]
)

proto_library(
    name = 'parse_log_proto',
    srcs = 'parse_log.proto'
)

cc_library(
    name = 'parse_log',
    srcs = [
        'parse_log.cc',
    ],
    deps = [
        ':parse_log_proto',
        '//toft/base/string:string',
        '//toft/storage/file:file',
        '//thirdparty/glog:glog',
    ]
)

cc_library(
    name = 'parse_log_tool',
    srcs = [
        'parse_log_tool.cc',
    ],
    deps = [
        ':parse_log',
        ':filename_tool',
        ':create_tablet',
    ]
)

cc_library(
    name = 'env_utils',
    srcs = 'env_utils.cc',
    deps = [
        '//toft/storage/path:path'
    ]
)
