// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Created: 03/22/12
// Description:

#include <string>
#include <vector>

#include "thirdparty/gmock/gmock.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/field_productor.h"
#include "gunir/io/mock_dissector.h"
#include "gunir/io/table_options.pb.h"
#include "gunir/io/tablet_schema.pb.h"

namespace gunir {
namespace io {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::Pointee;
using ::testing::SaveArg;
using ::testing::SaveArgPointee;
using ::testing::NiceMock;

class FieldProductorTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        m_schema_descriptor.set_type("mock_schema");
        m_record_importer = new MockRecordImporter();
        m_record_dissector = new MockRecordDissector();
        m_field_productor.reset(new FieldProductor(m_files,
                                                   m_schema_descriptor,
                                                   m_record_importer,
                                                   m_record_dissector));
    }

protected:
    MockRecordImporter* m_record_importer;
    MockRecordDissector* m_record_dissector;
    scoped_ptr<FieldProductor> m_field_productor;
    SchemaDescriptor m_schema_descriptor;
    ::google::protobuf::RepeatedPtrField<URI> m_files;
};

TEST_F(FieldProductorTest, GetSchemaColumnStat) {
    std::vector<ColumnStaticInfo> column_stats;
    EXPECT_CALL(*m_record_dissector, GetSchemaColumnStat(_))
        .WillOnce(Return(true));
    EXPECT_TRUE(m_field_productor->GetSchemaColumnStat(&column_stats));
}

TEST_F(FieldProductorTest, GetFieldsSuccess) {
    std::vector<const Block*> output_blocks;
    std::vector<uint32_t> indexes;
    // const StringPiece record_data;
    EXPECT_CALL(*m_record_importer, ReadRecord(_))
        // .WillOnce(DoAll(SaveArgPointee<0>(&record_data), Return(true)));
        .WillOnce(Return(true));
    EXPECT_CALL(*m_record_dissector, DissectRecord(_, _, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_record_importer, HasRecord())
        .WillOnce(Return(false));

    // success case
    EXPECT_TRUE(m_field_productor->NextRecordFields(&output_blocks, &indexes));
}

TEST_F(FieldProductorTest, GetFieldsFailByImporter) {
    std::vector<const Block*> output_blocks;
    std::vector<uint32_t> indexes;
    EXPECT_CALL(*m_record_importer, ReadRecord(_))
        .WillOnce(Return(false));

    // case of read record fail
    EXPECT_FALSE(m_field_productor->NextRecordFields(&output_blocks, &indexes));
}

TEST_F(FieldProductorTest, GetFieldsFailByDissector) {
    std::vector<const Block*> output_blocks;
    std::vector<uint32_t> indexes;
    EXPECT_CALL(*m_record_importer, ReadRecord(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_record_dissector, DissectRecord(_, _, _))
        .WillOnce(Return(false));

    // case of dissect record fail
    EXPECT_FALSE(m_field_productor->NextRecordFields(&output_blocks, &indexes));
}


TEST_F(FieldProductorTest, CheckNextRecord) {
    EXPECT_CALL(*m_record_importer, HasRecord())
        .WillOnce(Return(false))
        .WillOnce(Return(true));

    EXPECT_FALSE(m_field_productor->HasRecord());
    EXPECT_TRUE(m_field_productor->HasRecord());
}

} // namespace io
} // namespace gunir
