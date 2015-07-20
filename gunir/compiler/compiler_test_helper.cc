// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
// Description: help to do tests in compiler
//
#include "gunir/compiler/compiler_test_helper.h"

DEFINE_int32(record_number, 3, "");
DEFINE_bool(print_readable_result, false, "");

namespace gunir {
namespace compiler {

// ************** Generate Random Document **************
static const uint32_t kMaxRepeatedTime = 5;

uint32_t RandNumber() {
    return rand() % 10000; // NOLINT
}

Links RandLinks() {
    Links links;
    uint32_t backward_number = RandNumber() % kMaxRepeatedTime;
    uint32_t forward_number = RandNumber() % kMaxRepeatedTime;
    for (uint32_t i = 0; i < backward_number; ++i) {
        links.add_backward(RandNumber());
    }

    for (uint32_t i = 0; i < forward_number; ++i) {
        links.add_forward(RandNumber());
    }
    return links;
}

Language RandLanguage() {
    Language language;
    static std::string code[4] = {"en-us", "us", "en-gb", "zh"};
    language.set_code(code[RandNumber() % 4]);

    static std::string country[4] = {"usa", "england", "china", "tw"};
    if (RandNumber() % 2 == 0) {
        language.set_country(country[RandNumber() % 4]);
    }
    return language;
}

Name RandName() {
    Name name;

    uint32_t language_number = RandNumber() % kMaxRepeatedTime;
    for (uint32_t i = 0; i < language_number; ++i) {
        name.add_language()->CopyFrom(RandLanguage());
    }

    static std::string url[4]
        = {"http://A", "http://B", "http://C", "http://D"};
    if (RandNumber() % 2 == 0) {
        name.set_url(url[RandNumber() % 4]);
    }

    return name;
}

PBMessage* RandDocument() {
    Document document;
    document.set_docid(RandNumber());

    if (RandNumber() % 2 == 0) {
        document.mutable_links()->CopyFrom(RandLinks());
    }

    uint32_t name_number = RandNumber() % kMaxRepeatedTime;
    for (uint32_t i = 0; i < name_number; ++i) {
        document.add_name()->CopyFrom(RandName());
    }

    return new Document(document);
}

// ************** Create Test Data **************
struct CreateTabletSpec {
    int record_number;
    std::string output_file_name;
    std::string proto_file_name;
    std::string message_name;
    std::string table_name;
};

void CreateTablet(const CreateTabletSpec& input_spec,
                  const std::string& input_file_name,
                  std::string* output_file_name) {
    io::TableOptions opt;

    opt.add_input_files();
    opt.mutable_input_files(0)->set_uri(input_file_name);
    opt.mutable_schema_descriptor()->set_type("PB");

    ProtoMessage proto_message;
    proto_message.CreateMessageByProtoFile(input_spec.proto_file_name,
                                           input_spec.message_name);
    opt.mutable_schema_descriptor()->set_description(
        proto_message.GetFileDescriptorSetString());

    opt.mutable_schema_descriptor()->set_record_name(input_spec.message_name);
    opt.set_output_file(input_spec.output_file_name);
    opt.set_output_table(input_spec.table_name);

    io::TableBuilder builder;
    std::vector<std::string> output_files;
    builder.CreateTable(opt, &output_files);

    if (output_files.size() == 0) {
        std::cerr << "Output file size:" << output_files.size() << std::endl;
        *output_file_name = "NO_OUTPUT_FILE";
    } else {
        *output_file_name = output_files[0];
    }
}

bool CreateTestDataHelper(const CreateTabletSpec& test_data_input_spec,
                        const CreateTabletSpec& test_data_output_spec,
                        std::string* test_input_tablet,
                        std::string* test_output_tablet,
                        SrcGenerator src_generator,
                        ResultGenerator result_generator) {
    srand(time(NULL));

    std::string test_input_record_io_file =
        test_data_input_spec.output_file_name + ".msg";
    toft::scoped_ptr<File> file(File::Open(test_input_record_io_file.c_str(), "w"));
    if (NULL == file.get()) {
        LOG(ERROR) << "open file error :" << test_input_record_io_file;
        return false;
    }

    std::string test_output_record_io_file =
        test_data_output_spec.output_file_name + ".msg";
    toft::scoped_ptr<File> agg_file(File::Open(
            test_output_record_io_file.c_str(), "w"));
    if (NULL == agg_file.get()) {
        LOG(ERROR) << "open file error :" << test_output_record_io_file;
        return false;
    }

    RecordWriter test_input_data_writer(file.get());
    RecordWriter test_output_data_writer(agg_file.get());
    for (int32_t i = 0; i < test_data_input_spec.record_number; ++i) {
        ::google::protobuf::Message* src = (*src_generator)();
        if (!test_input_data_writer.WriteMessage(*src)) {
            LOG(ERROR) << "write message error ";
            return false;
        }

        ::google::protobuf::Message* result = (*result_generator)(src);

        if (FLAGS_print_readable_result) {
            std::cout << "========== input message ==========\n";
            std::cout << src->DebugString() << std::endl;

            std::cout << "========== output message =========\n";
            if (result != NULL) {
                std::cout << "HAS OUTPUT\n:" << result->DebugString();
            } else {
                std::cout << "NO OUTPUT";
            }
            std::cout << "\n" << std::endl;
        }


        if (result == NULL) {
            continue;
        }

        if (!test_output_data_writer.WriteMessage(*result)) {
            LOG(ERROR) << "write message error";
            return false;
        }

        delete src;
        delete result;
    }

//     if (!test_input_data_writer.Flush() || !test_output_data_writer.Flush()) {
//         LOG(ERROR) << "flush to file error : " <<
//             test_data_input_spec.output_file_name;
//         LOG(ERROR) << "flush to file error : " <<
//             test_data_output_spec.output_file_name;
//         return false;
//     }

    file->Close();
    agg_file->Close();

    CreateTablet(
        test_data_input_spec, test_input_record_io_file, test_input_tablet);
    CreateTablet(
        test_data_output_spec, test_output_record_io_file, test_output_tablet);

    return true;
}

void CreateTestData(const std::string& test_file_prefix,
                    const std::string& input_data_proto_file,
                    const std::string& input_data_proto_message,
                    const std::string& output_data_proto_file,
                    const std::string& output_data_proto_message,
                    SrcGenerator src_generator,
                    ResultGenerator result_generator) {
    CreateTabletSpec test_data_input_spec;
    CreateTabletSpec test_data_output_spec;
    int record_number = FLAGS_record_number;

    test_data_input_spec.record_number = record_number;
    test_data_input_spec.output_file_name = test_file_prefix + "_src";
    test_data_input_spec.proto_file_name = input_data_proto_file;
    test_data_input_spec.message_name = input_data_proto_message;
    test_data_input_spec.table_name = "table1";

    test_data_output_spec.record_number = record_number;
    test_data_output_spec.output_file_name = test_file_prefix + "_result";
    test_data_output_spec.proto_file_name = output_data_proto_file;
    test_data_output_spec.message_name = output_data_proto_message;
    test_data_output_spec.table_name = "table2";

    std::string test_input_document, test_output_document;
    CreateTestDataHelper(test_data_input_spec,
                         test_data_output_spec,
                         &test_input_document,
                         &test_output_document,
                         src_generator,
                         result_generator);

    std::cout << "test_input_data:" << test_input_document <<
        ", test_output_data:" << test_output_document << std::endl;
}

// *************** Prepare to Start Executor ***************
const char* kTabletSpliter = ";";
std::vector<std::string> GetInputTablets(const std::string& file_string) {
    std::vector<std::string> files;
    const char* p = file_string.c_str();
    const char* q;

    q = strstr(p, kTabletSpliter);
    while (q != NULL) {
        files.push_back(std::string(p, q - p));

        p = q + 1;
        q = strstr(p, kTabletSpliter);
    }

    if (*p != '\0') {
        files.push_back(std::string(p));
    }
    return files;
}

std::vector<TableInfo> GetTableInfos(std::vector<std::string> tables,
                                     const std::string& proto_file,
                                     const std::string& pb_data_file) {
    CHECK_EQ(1, tables.size()) << "In client test, only 1 table is permitted";
    std::vector<std::string> files = GetInputTablets(pb_data_file);

    std::vector<TableInfo> table_infos;
    for (size_t i = 0; i < tables.size(); ++i) {
        TableSchema schema;
        std::string table_proto = proto_file;

        schema.InitSchemaFromProtoFile(table_proto, tables[i]);
        std::string schema_string = schema.GetTableSchemaString();

        TableInfo info;

        info.set_table_name(tables[i]);
        info.set_message_name(tables[i]);
        info.set_table_schema(schema_string);

        for (size_t j = 0; j < files.size(); ++j) {
            info.add_tablets()->set_name(files[j]);
        }

        table_infos.push_back(info);
    }
    return table_infos;
}

io::Scanner* GetScanner(
    const SelectQuery& select_query, const TableInfo& table_info) {
    std::vector<AffectedColumnInfo> affect_columns =
        select_query.GetAffectedColumnInfo();
    std::vector<std::string> columns;

    for (size_t i = 0; i < affect_columns.size(); ++i) {
        columns.push_back(affect_columns[i].m_column_info.m_column_path_string);
    }

    LocalTabletScanner* scanner = new LocalTabletScanner();
    if (!scanner->Init(table_info.table_name(),
                       table_info.tablets(0).name(),
                       columns)) {
        delete scanner;
        return NULL;
    }

    return scanner;
}

std::string TrimQuery(const std::string& input_query) {
    const char* query_start = input_query.c_str();
    while (*query_start == ' ' || *query_start == '\t')
        query_start++;
    return std::string(query_start);
}

bool ParseQuery(const std::string& query_string,
                const std::string& proto_file,
                const std::string& pb_data_file,
                SelectQuery** select_query) {
    QueryStmt* query_stmt_ptr;
    if (parse_line(query_string.c_str(), &query_stmt_ptr) != 0) {
        LOG(ERROR) << "syntax error";
        return false;
    }

    toft::scoped_ptr<QueryStmt> query_stmt(query_stmt_ptr);
    *select_query = new SelectQuery(query_stmt->select());

    std::vector<std::string> tables = (*select_query)->GetQueryTables();
    std::vector<TableInfo> table_infos =
        GetTableInfos(tables, proto_file, pb_data_file);

    if (!(*select_query)->Init(table_infos)) {
        return false;
    }
    if (!(*select_query)->Analyze()) {
        return false;
    }
    return true;
}

bool ProcessQuery(const std::string& query_string,
                  const std::string& proto_file,
                  const std::string& pb_data_file,
                  Executor* executor,
                  std::vector<io::Scanner*>* scanners) {
    SelectQuery* query = NULL;
    if (!ParseQuery(query_string, proto_file, pb_data_file, &query)) {
        return false;
    }
    toft::scoped_ptr<SelectQuery> local_query(query);
    const SelectQuery& select_query = *query;

    std::cout << "ResultSchema: "
        <<select_query.GetReadableResultSchema() << std::endl;
    SimplePlanner planer(select_query);

    TaskPlanProto proto;
    if (!planer.GenerateExecutePlan(&proto)) {
        LOG(ERROR) << "Cannot generate execute plan !!!!";
        return false;
    }
    Plan* plan = Plan::InitPlanFromProto(proto.exec_plan());

    std::vector<std::string> tables = select_query.GetQueryTables();
    std::vector<TableInfo> table_infos =
        GetTableInfos(tables, proto_file, pb_data_file);
    io::Scanner* scanner = GetScanner(select_query, table_infos[0]);
    if (scanner == NULL) {
        LOG(ERROR) << "init scanner failed, tablet file not exists?";
        return false;
    }
    scanners->push_back(scanner);
    plan->SetScanner(*scanners);

    executor->Init(plan,
                   select_query.GetResultSchema(),
                   SchemaBuilder::kQueryResultMessageName);
    return true;
}

// **************** Used to Check Slice **********************
#define EXPECT_EQ_NOT_RETURN(v1, v2) \
        do { \
            EXPECT_EQ(v1, v2); \
            if (v1 != v2) { \
                return false; \
            } \
        } while (0)

bool IsSliceEqual(io::Slice* slice1, io::Slice* slice2) {
    EXPECT_EQ_NOT_RETURN(slice1->GetCount(), slice2->GetCount());

    for (size_t i = 0; i < slice1->GetCount(); ++i) {
        if (slice1->HasBlock(i) != slice2->HasBlock(i)) {
            LOG(ERROR) << "Has Block Not Equal";
            return false;
        }

        if (!slice1->HasBlock(i)) {
            continue;
        }

        const io::Block *b1, *b2;
        b1 = slice1->GetBlock(i);
        b2 = slice2->GetBlock(i);

        if (b1->IsNull() != b2->IsNull() ||
            b1->GetRepLevel() != b2->GetRepLevel() ||
            b1->GetDefLevel() != b2->GetDefLevel()) {
            LOG(ERROR) << "level not equal";
            return false;
        }

        if (b1->IsNull()) {
            continue;
        }

        EXPECT_EQ(b1->GetValue(), b2->GetValue());
    }
    return true;
}

std::string SliceDebugInfo(const io::Slice* s1, const io::Slice* s2) {
    std::string info;

    info = "\nright_slice:";
    info += s1->DebugString();
    info += "\n\n, executor_slice:";
    info += s2->DebugString();

    return info;
}

} // namespace compiler
} // namespace gunir
