// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <string>

#include "toft/base/string/string_piece.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/csv_dissector.h"
#include "gunir/io/table_options.pb.h"
#include "gunir/utils/csv_helper.h"

const std::string kRecordFileName = "./recordfile_one.dat";
const std::string kRecordAnotherFileName = "./recordfile_two.dat";
const std::string kUnexistFileName = "non_exist_file";

namespace gunir {
namespace io {

class CsvDissectorTest : public testing::Test {
protected:
    virtual void SetUp() {
        m_column_number = 7;
        m_line_length = 0;
        m_importer = new CsvRecordImporter();
        m_dissector = new CsvRecordDissector();
    }

    virtual void TearDown() {
        delete m_importer;
        delete m_dissector;
    }

    void InitSchema() {
        m_schema.set_type("CSV");
        std::string columns(m_column_number * 2 - 1, 'a');
        for (size_t i = 1; i < columns.size(); i += 2) {
            columns[i] = ',';
            columns[i + 1] = columns[i - 1] + 1;
        }
        CsvHelper helper;
        helper.CreateMessageByCsvString(columns, "test");
        m_schema.set_description(helper.GetFileDescriptorSetString());
        m_schema.set_record_name("test");
        m_schema.set_charset_encoding("GBK");
    }

    void CreateFile(uint32_t record_number,
                    const std::string& file_name,
                    uint32_t record_size) {
        scoped_ptr<File> file(File::Open(file_name.c_str(), "w"));
        CHECK_NOTNULL(file.get());
        std::string record(record_size, 'a');
        record = GenerateLine(record);
        for (uint32_t i = 0; i < record_number ; ++i) {
            EXPECT_EQ(m_line_length,
                      file->Write(record.c_str(), m_line_length));
        }

        EXPECT_TRUE(file->Close());
    }

    std::string GenerateLine(const std::string& column) {
        std::string line = column;
        for (size_t i = 0; i < m_column_number - 1; ++i) {
            line += "," + column;
        }
        line += "\n";
        m_line_length = line.length();
        return line;
    }

protected:
    ::google::protobuf::RepeatedPtrField<URI> m_files;
    SchemaDescriptor m_schema;
    CsvRecordImporter* m_importer;
    CsvRecordDissector* m_dissector;
    uint32_t m_column_number;
    uint32_t m_line_length;
};

// CsvRecordReader

TEST_F(CsvDissectorTest, OpenExist) {
    CreateFile(1, kRecordFileName, 10);
    m_files.Clear();
    URI *uri_one = m_files.Add();
    uri_one->set_uri(kRecordFileName);
    EXPECT_TRUE(m_importer->Open(m_files))
                << "fail to open exist file";
    EXPECT_TRUE(m_importer->Close());
}

TEST_F(CsvDissectorTest, OpenNotExistFile) {
    m_files.Clear();
    URI *uri_two = m_files.Add();
    uri_two->set_uri(kUnexistFileName);
    EXPECT_FALSE(m_importer->Open(m_files))
                << "should not open un-exist file";
    EXPECT_TRUE(m_importer->Close());
}

TEST_F(CsvDissectorTest, ReadNotEmptyFile) {
    CreateFile(1, kRecordFileName, 10);
    m_files.Clear();
    URI *uri_one = m_files.Add();
    uri_one->set_uri(kRecordFileName);
    EXPECT_TRUE(m_importer->Open(m_files));
    StringPiece value;
    EXPECT_TRUE(m_importer->ReadRecord(&value));
    EXPECT_EQ(m_line_length, value.size())
            << "not found record from file";
    EXPECT_FALSE(m_importer->NextRecord());
    EXPECT_FALSE(m_importer->ReadRecord(&value));
    EXPECT_TRUE(m_importer->Close());
}

// TEST_F(CsvDissectorTest, ReadTooBigRecord) {
//     CreateFile(1, kRecordFileName, 1024 * 1024 * 5);
//     m_files.Clear();
//     URI *uri_one = m_files.Add();
//     uri_one->set_uri(kRecordFileName);
//     EXPECT_FALSE(m_importer->Open(m_files));
// }

TEST_F(CsvDissectorTest, ReadMultipleFiles) {
    CreateFile(2, kRecordFileName, 10);
    CreateFile(3, kRecordAnotherFileName, 10);
    m_files.Clear();
    URI *uri_one = m_files.Add();
    uri_one->set_uri(kRecordFileName);
    uri_one = m_files.Add();
    uri_one->set_uri(kRecordAnotherFileName);
    EXPECT_TRUE(m_importer->Open(m_files))
                << "fail to open exist file";
    StringPiece value;
    EXPECT_TRUE(m_importer->ReadRecord(&value));
    EXPECT_EQ(m_line_length, value.size())
        << "not found record from file";
    for (int i = 0; i < 4; ++i) {
        EXPECT_TRUE(m_importer->NextRecord());
        EXPECT_TRUE(m_importer->ReadRecord(&value));
        EXPECT_EQ(m_line_length, value.size())
            << "not found record from file";
    }

    EXPECT_FALSE(m_importer->NextRecord());
    EXPECT_FALSE(m_importer->ReadRecord(&value));
    EXPECT_TRUE(m_importer->Close());
}

TEST_F(CsvDissectorTest, InitSucceed) {
    InitSchema();
    EXPECT_TRUE(m_dissector->Init(m_schema));

    std::vector<ColumnStaticInfo> columns;
    EXPECT_TRUE(m_dissector->GetSchemaColumnStat(&columns));
    EXPECT_EQ(columns.size(), m_column_number);
    for (size_t i = 0; i < columns.size(); ++i) {
        EXPECT_EQ(columns[i].column_index(), i);
        EXPECT_EQ(columns[i].column_name().size(), 6U);
//         EXPECT_TRUE(columns[i].column_name()[5] == ('a' + i));
    }
}

TEST_F(CsvDissectorTest, InitFailed) {
    EXPECT_FALSE(m_dissector->Init(m_schema));
}

TEST_F(CsvDissectorTest, Dissector) {
    CreateFile(10, kRecordFileName, 10);
    m_files.Clear();
    URI *uri_one = m_files.Add();
    uri_one->set_uri(kRecordFileName);
    EXPECT_TRUE(m_importer->Open(m_files));

    InitSchema();
    EXPECT_TRUE(m_dissector->Init(m_schema));
    std::vector<ColumnStaticInfo> columns;
    EXPECT_TRUE(m_dissector->GetSchemaColumnStat(&columns));
    char buffer[1024 * 1024];
    m_dissector->SetBuffer(buffer, 1024 * 1024);

    StringPiece value;
    std::vector<const Block*> blocks;
    std::vector<uint32_t> indexs;
    EXPECT_TRUE(m_importer->ReadRecord(&value));
    EXPECT_EQ(m_line_length, value.size())
            << "not found record from file";
    EXPECT_TRUE(m_dissector->DissectRecord(value, &blocks, &indexs));
    EXPECT_EQ(blocks.size(), m_column_number);
    EXPECT_EQ(indexs.size(), m_column_number);
    for (size_t i = 0; i < indexs.size(); ++i) {
        EXPECT_EQ(indexs[i], i);
    }

    for (int i = 0; i < 9; ++i) {
        EXPECT_TRUE(m_importer->NextRecord());
        EXPECT_TRUE(m_importer->ReadRecord(&value));
        EXPECT_EQ(m_line_length, value.size())
            << "not found record from file";
        EXPECT_TRUE(m_dissector->DissectRecord(value, &blocks, &indexs));
        EXPECT_EQ(blocks.size(), m_column_number);
        EXPECT_EQ(indexs.size(), m_column_number);
        for (size_t i = 0; i < indexs.size(); ++i) {
            EXPECT_EQ(indexs[i], i);
        }
    }

    EXPECT_FALSE(m_importer->NextRecord());
    EXPECT_FALSE(m_importer->ReadRecord(&value));
    EXPECT_TRUE(m_importer->Close());
}

} // namespace io
} // namespace gunir
