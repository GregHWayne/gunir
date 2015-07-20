cc_library (
    name = 'analyzer',
    srcs = [
        'aggregate_functions.cc',
        'expression.cc',
        'function_expression.cc',
        'function_resolver.cc',
        'operator_expression.cc',
        'operator_functions.cc',
        'cast_functions.cc',
        'plain_functions.cc',
        'schema_builder.cc',
        'select_query.cc',
        'query_convertor.cc',
        'table_entry.cc',
        'target.cc',
        'type_resolver.cc',
    ],
    deps = [
        '//gunir/compiler/parser:big_query_types',
        '//gunir/compiler/parser:plan_proto',
        '//gunir/compiler/parser:select_stmt_proto',
        '//gunir/compiler/parser:table_schema',
        '//gunir/proto:table_proto',
        '//thirdparty/glog:glog',
        '//thirdparty/protobuf:protobuf',
    ],
    extra_cppflags = '-std=c++11',
    link_all_symbols = 1
)

cc_library (
    name = 'scan_reader',
    srcs = [
        'scan_reader.cc',
    ],
    deps = [
        '//gunir/proto:task_proto',
        '//toft/base/string:string',
        '//gunir/io:io',
        '//gunir/io:slice',
        '//thirdparty/glog:glog',
    ]
)

cc_library (
    name = 'executor',
    srcs = [
        'executor.cc',
    ],
    deps = [
        ':job_plan',
        '//gunir/io:data_holder',
        '//gunir/proto:task_proto',
        '//toft/base/string:string',
        '//gunir/io:io',
        '//gunir/io:slice',
        '//thirdparty/glog:glog',
    ]
)

cc_library (
    name = 'job_plan',
    srcs = [
        'aggregate_plan.cc',
        'agg_expr_info.cc',
        'dis_agg_expr_info.cc',
        'filter_plan.cc',
        'limit_plan.cc',
        'merge_plan.cc',
        'parallel_planner.cc',
        'plan.cc',
        'projection_plan.cc',
        'scan_plan.cc',
        'simple_planner.cc',
        'sort_plan.cc',
        'join_plan.cc',
        'job_plan.cc',
        'union_plan.cc',
        'uniq_plan.cc',
        'within_plan.cc'
    ],
    deps = [
        '//gunir/compiler/parser:select_stmt_proto',
        '//gunir/compiler/parser:plan_proto',
        ':scan_reader',
        ':analyzer',
        ':simple_plan_scheduler',
        '//gunir/io:io',
        '//thirdparty/glog:glog',
        '//thirdparty/protobuf:protobuf'
    ]
)

cc_library (
    name = 'simple_plan_scheduler',
    srcs = [
        'simple_plan_scheduler.cc',
    ],
    deps = [
        '//thirdparty/glog:glog',
        '//thirdparty/gflags:gflags',
        '//gunir:gunir_flags',
    ],
)

cc_test (
    name = 'table_schema_test',
    srcs = [
        'table_schema_test.cc',
    ],
    deps = [
        '//gunir/compiler/parser:table_schema',
        '//thirdparty/glog:glog',
        '//thirdparty/protobuf:protobuf',
    ],
    testdata = [
         'testdata/table_schema_test',
         'testdata/Document.proto',
    ]
)

cc_test (
    name = 'analyze_query_test',
    srcs = [
        'analyze_query_test.cc',
    ],
    deps = [
        ':analyzer',
        '//gunir/compiler/parser:parser',
        '//thirdparty/glog:glog',
        '//thirdparty/protobuf:protobuf'
    ],
    testdata = [
         'testdata/analyze_query_test',
    ]
)

cc_test(
    name = 'projection_plan_test',
    srcs = 'projection_plan_test.cc',
    deps = [
        ':executor',
        ':compiler_test_helper'
    ],
    testdata = [
        'testdata/projection_test',
        'testdata/datatype_test'
    ]
)

cc_test(
    name = 'merge_plan_test',
    srcs = 'merge_plan_test.cc',
    deps = [
        ':compiler_test_helper'
    ],
    testdata = [
        'testdata/sort_test',
    ]
)

cc_test(
    name = 'sort_plan_test',
    srcs = 'sort_plan_test.cc',
    deps = [
        ':compiler_test_helper'
    ],
    testdata = [
        'testdata/sort_test',
    ]
)

cc_test (
    name = 'job_plan_test',
    srcs = [
        'job_plan_test.cc',
    ],
    deps = [
        ':job_plan',
        ':analyzer',
        '//gunir/compiler/parser:parser',
        '//thirdparty/glog:glog',
        '//thirdparty/protobuf:protobuf'
    ],
    testdata = [
         'testdata/Document.proto',
    ]
)

#cc_test(
#    name = 'executor_test',
#    srcs = [
#        'executor_test.cc',
#    ],
#    deps = [
#        ':executor',
#        '//gunir/io:tablet_scanner',
#        '//gunir/common:server_test_helper',
#        '//gunir/io:io',
#        '//gunir/utils:test_helper',
#        '//thirdparty/glog:glog',
#        '//thirdparty/gmock:gmock',
#        '//thirdparty/protobuf:protobuf',
#    ],
#    testdata = [
#        'testdata/executor_test',
#        'testdata/Document.proto',
#    ]
#)

cc_test (
    name = 'parallel_planner_test',
    srcs = [
        'parallel_planner_test.cc',
    ],
    deps = [
        ':compiler_test_helper',
        ':compiler_utils',
        ':executor',
        '//gunir/compiler/parser:parser',
        '//gunir/io:data_holder',
        '//gunir:gunir_flags',
        '//gunir/io:io',
        '//thirdparty/glog:glog',
        '//thirdparty/protobuf:protobuf',
        '//gunir/utils:proto_helper'
    ],
    testdata = [
        'testdata/DocumentJoinLeft.proto',
        'testdata/DocumentJoinRight.proto',
        'testdata/parallel_planner_test',
    ]
)

cc_test (
    name = 'join_plan_test',
    srcs = [
        'join_plan_test.cc',
    ],
    deps = [
        ':compiler_test_helper',
        ':compiler_utils',
        ':executor',
        '//gunir/compiler/parser:parser',
        '//gunir/io:data_holder',
        '//gunir:gunir_flags',
        '//gunir/io:io',
        '//thirdparty/glog:glog',
        '//thirdparty/protobuf:protobuf',
        '//gunir/utils:proto_helper'
    ],
    testdata = [
        'testdata/DocumentJoinLeft.proto',
        'testdata/DocumentJoinRight.proto',
    ]
)

cc_test (
    name = 'query_convertor_test',
    srcs = [
        'query_convertor_test.cc',
    ],
    deps = [
        ':analyzer',
        ':compiler_test_helper',
        ':compiler_utils',
        '//gunir/compiler/parser:parser',
        '//gunir:gunir_flags',
        '//thirdparty/glog:glog',
        '//thirdparty/protobuf:protobuf',
    ],
)


cc_test (
    name = 'uniq_plan_test',
    srcs = [
        'uniq_plan_test.cc',
    ],
    deps = [
        ':compiler_test_helper',
        ':compiler_utils',
        ':executor',
        '//gunir/compiler/parser:parser',
        '//gunir/io:data_holder',
        '//gunir:gunir_flags',
        '//gunir/io:io',
        '//thirdparty/glog:glog',
        '//thirdparty/protobuf:protobuf',
        '//gunir/utils:proto_helper'
    ],
    testdata = [
        'testdata/UniqDocument.proto',
    ]
)

cc_test (
    name = 'simple_plan_scheduler_test',
    srcs = [
        'simple_plan_scheduler_test.cc',
    ],
    deps = [
        ':job_plan',
        ':analyzer',
        '//gunir/compiler/parser:parser',
        '//gunir/compiler/parser:plan_proto',
        '//gunir/io:io',
        '//thirdparty/gflags:gflags',
        ':simple_plan_scheduler',
        '//thirdparty/glog:glog',
    ],
)

cc_test (
    name = 'simple_planner_test',
    srcs = [
        'simple_planner_test.cc',
    ],
    deps = [
        ':job_plan',
        ':analyzer',
        '//gunir/compiler/parser:parser',
        '//gunir/io:io',
        '//thirdparty/glog:glog',
        '//thirdparty/protobuf:protobuf',
        '//thirdparty/gflags:gflags'
    ],
    testdata = [
         'testdata/Document.proto',
         'testdata/DocumentJoinLeft.proto',
         'testdata/DocumentJoinRight.proto',
    ]
)

cc_test(
    name = 'within_plan_test',
    srcs = 'within_plan_test.cc',
    deps = [
        ':compiler_test_helper'
    ],
    testdata = [
        'testdata/within_test',
    ]
)

cc_test (
    name = 'limit_plan_test',
    srcs = [
        'limit_plan_test.cc',
    ],
    deps = [
        ':job_plan',
        ':analyzer',
        '//gunir/compiler/parser:parser',
        '//gunir/io:io',
        '//thirdparty/glog:glog',
        '//thirdparty/protobuf:protobuf'
    ],
    testdata = [
         'testdata/Document.proto',
    ]
)

cc_test (
    name = 'compiler_test',
    srcs = 'compiler_test.cc',
    deps = [
        ':executor',
        ':compiler_test_helper',
        '//gunir/readline:readline'
    ],
    testdata = [
         'testdata/client',
    ]
)

cc_binary (
    name = 'sort_test_helper',
    srcs = 'sort_test_helper.cc',
    deps = [
        ':compiler_test_helper',
        ':compiler_test_helper_proto',
    ],
)

cc_binary (
    name = 'projection_test_helper',
    srcs = 'projection_test_helper.cc',
    deps = [
        ':compiler_test_helper'
    ],
)

proto_library (
    name = 'within_test_helper_proto',
    srcs = 'within_test_helper.proto'
)

cc_binary (
    name = 'within_test_helper',
    srcs = 'within_test_helper.cc',
    deps = [
        ':within_test_helper_proto',
        ':compiler_test_helper'
    ],
)

cc_test (
    name = 'aggregate_plan_test',
    srcs = 'aggregate_plan_test.cc',
    deps = [
        ':compiler_test_helper'
    ],
    testdata = [
         'testdata/aggregate_test',
    ]
)

cc_binary (
    name = 'aggregate_test_helper',
    srcs = 'aggregate_test_helper.cc',
    deps = [
        ':compiler_test_helper_proto',
        ':compiler_test_helper'
    ],
)

proto_library (
    name = 'compiler_test_helper_proto',
    srcs = 'compiler_test_helper.proto'
)

cc_library (
    name = 'compiler_test_helper',
    srcs = 'compiler_test_helper.cc',
    deps = [
        ':executor',
        '//gunir/compiler/parser:parser',
        ':analyzer',
        ':executor',
        '//gunir/compiler/testdata:document_proto',
        '//gunir/io:table_builder',
        '//toft/storage/file:file',
        '//toft/storage/recordio:recordio',
        '//toft/system/memory:memory',
        '//thirdparty/gflags:gflags',
        '//thirdparty/glog:glog',
        '//thirdparty/gtest:gtest',
        '//thirdparty/protobuf:protobuf'
    ],
)

cc_library(
    name = 'compiler_utils',
    srcs = 'compiler_utils.cc',
    deps = [
        '//gunir/compiler/parser:plan_proto',
        '//gunir/proto:task_proto',
    ]
)
