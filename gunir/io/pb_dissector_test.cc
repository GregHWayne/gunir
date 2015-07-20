// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Created: 03/22/12
// Description:

#include <string>

#include "toft/base/string/string_piece.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/pb_dissector.h"
#include "gunir/io/table_options.pb.h"

const std::string kRecordFileName = "./testdata/recordfile_one.dat";
const std::string kRecordAnotherFileName = "./testdata/recordfile_two.dat";
const std::string kUnexistFileName = "non_exist_file";

namespace gunir {
namespace io {

class PbDissectorTest : public testing::Test {
protected:
    virtual void SetUp() {
        m_importer = new PbRecordImporter();
        m_dissector = new PbRecordDissector();
    }

    virtual void TearDown() {
        delete m_importer;
        delete m_dissector;
    }

protected:
    ::google::protobuf::RepeatedPtrField<URI> m_files;
    PbRecordImporter* m_importer;
    PbRecordDissector* m_dissector;
};

// PbRecordReader

TEST_F(PbDissectorTest, OpenExist) {
    m_files.Clear();
    URI *uri_one = m_files.Add();
    uri_one->set_uri(kRecordFileName);
    EXPECT_TRUE(m_importer->Open(m_files))
                << "fail to open exist file";
    EXPECT_TRUE(m_importer->Close());
}

TEST_F(PbDissectorTest, OpenNotExistFile) {
    m_files.Clear();
    URI *uri_two = m_files.Add();
    uri_two->set_uri(kUnexistFileName);
    EXPECT_FALSE(m_importer->Open(m_files))
                << "should not open un-exist file";
    EXPECT_TRUE(m_importer->Close());
}

TEST_F(PbDissectorTest, ReadNotEmptyFile) {
    m_files.Clear();
    URI *uri_one = m_files.Add();
    uri_one->set_uri(kRecordFileName);
    EXPECT_TRUE(m_importer->Open(m_files));
    StringPiece value;
    EXPECT_TRUE(m_importer->ReadRecord(&value));
    EXPECT_LT(0U, value.size())
            << "not found record from file";
    EXPECT_TRUE(m_importer->Close());
}


TEST_F(PbDissectorTest, OpenMultipleFiles) {
    // TODO(anthony):
    // case of having un-exist file name
    // case of shipping from one file to next
}

TEST_F(PbDissectorTest, ReadMultipleFiles) {
    // TODO(anthony):
    // case of having un-exist file name
    // case of shipping from one file to next
}


TEST_F(PbDissectorTest, ShipCheckRecord) {
    // TODO(anthony):
    // check whether correct ship record
}

// PbRecordDissector

TEST_F(PbDissectorTest, DissectorRecord) {
    // TODO(anthony):
    //
}

TEST_F(PbDissectorTest, ParseSchema) {
    // TODO(anthony)
}


} // namespace io
} // namespace gunir
