// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//

#include "gunir/io/history_info_io.h"

#include <string>
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/testdata/document.pb.h"

namespace gunir {
namespace io {

class HistoryInfoIOTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        m_reader.reset(new HistoryInfoReader());
        m_writer.reset(new HistoryInfoWriter());

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

        r2.set_docid(20);
        r2.mutable_links()->add_backward(10);
        r2.mutable_links()->add_backward(30);
        r2.mutable_links()->add_forward(80);
        r2.add_name();
        r2.mutable_name(0)->set_url("http://C");

        r3.set_docid(30);
        r3.mutable_links()->add_backward(100);
        r3.add_name();
        r3.mutable_name(0)->set_url("http://D");
    }
    virtual void TearDown() {
        m_reader.reset();
        m_writer.reset();
    }

protected:
    toft::scoped_ptr<HistoryInfoReader> m_reader;
    toft::scoped_ptr<HistoryInfoWriter> m_writer;
    Document r1;
    Document r2;
    Document r3;
};

TEST_F(HistoryInfoIOTest, TestMessage) {
    toft::scoped_ptr<toft::File> file;
    file.reset(File::Open("./test.dat", "w"));
    CHECK_NOTNULL(file.get());

    EXPECT_TRUE(m_writer->Reset(file.get()));

    std::string r1_str = r1.SerializeAsString();
    std::string r2_str = r2.SerializeAsString();
    std::string r3_str = r3.SerializeAsString();

    for (uint32_t i = 0; i < 20; ++i) {
        EXPECT_TRUE(m_writer->WriteMessage(r1));
        EXPECT_TRUE(m_writer->WriteMessage(r2));
    }
    EXPECT_TRUE(m_writer->WriteMessage(r3));

    EXPECT_EQ(0, file->Close());

    file.reset(File::Open("./test.dat", File::ENUM_FILE_OPEN_MODE_R));
    CHECK_NOTNULL(file.get());

    EXPECT_TRUE(m_reader->Reset(file.get()));

    EXPECT_EQ(1, m_reader->Next());
    Document message;
    EXPECT_TRUE(m_reader->ReadMessage(&message));
    std::string content;
    EXPECT_TRUE(message.SerializeToString(&content));
    EXPECT_EQ(r3_str, content);

    for (uint32_t i = 0; i < 40; ++i) {
        EXPECT_EQ(1, m_reader->Next());

        Document message;
        EXPECT_TRUE(m_reader->ReadMessage(&message));

        std::string content;
        EXPECT_TRUE(message.SerializeToString(&content));
        if (i % 2 == 0) {
            EXPECT_EQ(r2_str, content);
        } else {
            EXPECT_EQ(r1_str, content);
        }
    }

    EXPECT_EQ(0, m_reader->Next());
    EXPECT_EQ(0, file->Close());
}

TEST_F(HistoryInfoIOTest, TestStringPiece) {
    toft::scoped_ptr<File> file;
    file.reset(File::Open("./test2.dat", File::ENUM_FILE_OPEN_MODE_W));
    CHECK_NOTNULL(file.get());

    EXPECT_TRUE(m_writer->Reset(file.get()));

    std::string r1_str = r1.SerializeAsString();
    std::string r2_str = r2.SerializeAsString();
    std::string r3_str = r3.SerializeAsString();

    for (uint32_t i = 0; i < 20; ++i) {
        EXPECT_TRUE(m_writer->WriteRecord(r1_str));
        EXPECT_TRUE(m_writer->WriteRecord(r2_str));
    }
    EXPECT_TRUE(m_writer->WriteRecord(r3_str));

    EXPECT_EQ(0, file->Close());

    file.reset(File::Open("./test2.dat", File::ENUM_FILE_OPEN_MODE_R));
    CHECK_NOTNULL(file.get());

    EXPECT_TRUE(m_reader->Reset(file.get()));

    EXPECT_EQ(1, m_reader->Next());
    StringPiece sp;
    m_reader->ReadRecord(&sp);
    std::string content = sp.as_string();
    EXPECT_EQ(r3_str, content);

    for (uint32_t i = 0; i < 40; ++i) {
        EXPECT_EQ(1, m_reader->Next());

        StringPiece sp;
        m_reader->ReadRecord(&sp);

        std::string content = sp.as_string();
        if (i % 2 == 0) {
            EXPECT_EQ(r2_str, content);
        } else {
            EXPECT_EQ(r1_str, content);
        }
    }

    EXPECT_EQ(0, m_reader->Next());
    EXPECT_EQ(0, file->Close());
}


} // namespace io
} // namespace gunir
