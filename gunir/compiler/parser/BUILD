proto_library (
    name = 'select_stmt_proto',
    srcs = 'select_stmt.proto'
)

proto_library (
    name = 'plan_proto',
    srcs = 'plan.proto',
    deps = [
        ':select_stmt_proto',
        '//gunir/proto:tablet_proto',
    ]
)

#lex_yacc_library (
#    name = 'parser',
#    srcs = [
#        'big_query.ll',
#        'big_query.yy'
#    ],
#    prefix = 'gunir_',
#    deps = [
#        ':big_query_types',
#        ':select_stmt_proto'
#    ],
#    recursive = 1
#)

cc_library(
    name = 'parser',
    srcs = [
        'big_query.ll.cc',
        'big_query.yy.cc'
    ],
    deps = [
        ':big_query_types',
        ':select_stmt_proto'
    ]
)

cc_test (
    name = 'big_query_test',
    srcs = [
        'big_query_test.cc',
    ],
    deps = ':parser'
)

cc_library(
    name = 'big_query_types',
    srcs = 'big_query_types.cc',
    deps = [
        '//thirdparty/protobuf:protobuf',
    ]
)

cc_library(
    name = 'table_schema',
    srcs = 'table_schema.cc',
    deps = [
        ':big_query_types',
        '//gunir/utils:proto_message',
        '//toft/storage/path:path',
    ],
)


