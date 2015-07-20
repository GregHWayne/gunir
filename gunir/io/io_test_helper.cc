// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
#include "gunir/io/io_test_helper.h"

#include <stdlib.h>

#include "toft/base/string/number.h"
#include "toft/storage/file/file.h"
#include "toft/storage/recordio/recordio.h"

#include "gunir/io/table_builder.h"
#include "gunir/io/tablet_schema.pb.h"
#include "gunir/io/tablet_writer.h"
#include "gunir/utils/proto_message.h"

namespace gunir {
namespace io {

IOTestHelper::IOTestHelper()
    :m_file_count(0) {
}
IOTestHelper::~IOTestHelper() {
}

void IOTestHelper::BuildRecordIOMessageTestFile(uint32_t num) {
    m_recordio_msg_file_name = "document.msg.recordio_" + NumberToString(m_file_count);
    m_file_count++;

    scoped_ptr<File> file;
    file.reset(File::Open(m_recordio_msg_file_name.c_str(), "w"));
    CHECK_NOTNULL(file.get());

    scoped_ptr<RecordWriter> writer;
    writer.reset(new RecordWriter(file.get()));
    CHECK_NOTNULL(writer.get());

    for (uint32_t i = 0; i < num; ++i) {
        Document r1;
        r1.set_docid(10);
        r1.mutable_links()->add_forward(20);
        r1.mutable_links()->add_forward(40);
        r1.mutable_links()->add_forward(60);
        r1.add_name();
        r1.mutable_name(0)->add_language();
        r1.mutable_name(0)->mutable_language(0)->set_code("en-us");
        r1.mutable_name(0)->mutable_language(0)->set_country("us");
        r1.mutable_name(0)->add_language();
        r1.mutable_name(0)->mutable_language(1)->set_code("en");
        r1.mutable_name(0)->set_url("http://A");
        r1.add_name();
        r1.mutable_name(1)->set_url("http://B");
        r1.add_name();
        r1.mutable_name(2)->add_language();
        r1.mutable_name(2)->mutable_language(0)->set_code("en-gb");
        r1.mutable_name(2)->mutable_language(0)->set_country("gb");
        writer->WriteMessage(r1);

        Document r2;
        r2.set_docid(20);
        r2.mutable_links()->add_backward(10);
        r2.mutable_links()->add_backward(30);
        r2.mutable_links()->add_forward(80);
        r2.add_name();
        r2.mutable_name(0)->set_url("http://C");
        writer->WriteMessage(r2);
    }
    file->Flush();
    file->Close();
}

void IOTestHelper::GetRecordIOMessageTestFileName(std::string *file_name) const {
    *file_name = m_recordio_msg_file_name;
}

void IOTestHelper::BuildTabletTestFile(
    const std::string& table_name,
    std::vector<std::string> *output_files) {

    BuildRecordIOMessageTestFile();

    TableOptions opt;
    opt.add_input_files();
    opt.mutable_input_files(0)->set_uri(m_recordio_msg_file_name);

    opt.mutable_schema_descriptor()->set_type("PB");

    ProtoMessage proto_message;
    proto_message.CreateMessageByProtoFile("testdata/document.proto",
                                           "Document");
    opt.mutable_schema_descriptor()->set_description(
        proto_message.GetFileDescriptorSetString());

    opt.mutable_schema_descriptor()->set_record_name("Document");

    opt.set_output_file("tablet_file_from_msg.tablet");
    opt.set_output_table(table_name);

    TableBuilder builder;
    builder.CreateTable(opt, output_files);
}

static const uint32_t kMaxRepeatedTime = 4;

Document IOTestHelper::RandDocument() {
    Document document;
    document.set_docid(RandNumber());

    if (RandNumber() % 2 == 0) {
        document.mutable_links()->CopyFrom(RandLinks());
    }

    uint32_t name_number = RandNumber() % kMaxRepeatedTime;
    for (uint32_t i = 0; i < name_number; ++i) {
        document.add_name()->CopyFrom(RandName());
    }

    return document;
}

Name IOTestHelper::RandName() {
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

Language IOTestHelper::RandLanguage() {
    Language language;
    static std::string code[4] = {"en-us", "us", "en-gb", "zh"};
    language.set_code(code[RandNumber() % 4]);

    static std::string country[4] = {"us", "en", "zh", "gb"};
    if (RandNumber() % 2 == 0) {
        language.set_country(country[RandNumber() % 4]);
    }
    return language;
}

Links IOTestHelper::RandLinks() {
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

uint32_t IOTestHelper::RandNumber() {
    return rand(); // NOLINT
}

} // namespace io
} // namespace gunir
