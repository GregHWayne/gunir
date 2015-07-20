// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Created 03/21/12

#include "gunir/io/table_builder.h"

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "thirdparty/gmock/gmock.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/mock_field_productor.h"
#include "gunir/io/mock_tablet_writer.h"

namespace gunir {
namespace io {

using ::testing::_;
using ::testing::Return;
using ::testing::NiceMock;

class TableBuilderTest : public ::testing::Test {
public:
    TableBuilderTest() {
        m_options.add_input_files()->set_uri("mock_file_path");
        m_options.mutable_schema_descriptor()->set_type("mock_schema_type");

        m_tablet_writer = new MockTabletWriter;
        m_field_productor = new MockFieldProductor;
        m_table_builder.reset(new TableBuilder(m_tablet_writer,
                                               m_field_productor));
    }

protected:
    void SetUp() {
        ON_CALL(*m_field_productor, Reset(_, _))
            .WillByDefault(Return());
        ON_CALL(*m_field_productor, SetBuffer(_, _))
            .WillByDefault(Return());
        ON_CALL(*m_field_productor, GetSchemaColumnStat(_))
            .WillByDefault(Return(true));
        ON_CALL(*m_field_productor, HasRecord())
            .WillByDefault(Return(false));
        ON_CALL(*m_field_productor, NextRecordFields(_, _))
            .WillByDefault(Return(true));
        ON_CALL(*m_tablet_writer, SetBuffer(_, _))
            .WillByDefault(Return());
        ON_CALL(*m_tablet_writer, BuildTabletSchema(_, _, _))
            .WillByDefault(Return(true));
        ON_CALL(*m_tablet_writer, Open(_))
            .WillByDefault(Return(true));
        ON_CALL(*m_tablet_writer, Close())
            .WillByDefault(Return(true));
        ON_CALL(*m_tablet_writer, Write(_, _))
            .WillByDefault(Return(true));
    }

protected:
    MockTabletWriter* m_tablet_writer;
    MockFieldProductor* m_field_productor;
    TableOptions m_options;
    scoped_ptr<TableBuilder> m_table_builder;
};

TEST_F(TableBuilderTest, GetSchemaFail) {
    EXPECT_CALL(*m_field_productor, Reset(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, SetBuffer(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, GetSchemaColumnStat(_))
        .WillOnce(Return(false));

    EXPECT_FALSE(m_table_builder->CreateTable(m_options));
}

TEST_F(TableBuilderTest, InitTabletSchemaFail) {
    EXPECT_CALL(*m_field_productor, Reset(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, SetBuffer(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, GetSchemaColumnStat(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, BuildTabletSchema(_, _, _))
        .WillOnce(Return(false));

    EXPECT_FALSE(m_table_builder->CreateTable(m_options));
}

TEST_F(TableBuilderTest, OpenTabletWriterFail) {
    EXPECT_CALL(*m_field_productor, Reset(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, SetBuffer(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, GetSchemaColumnStat(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, BuildTabletSchema(_, _, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, SetBuffer(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_tablet_writer, Open(_))
        .WillOnce(Return(false));
    EXPECT_CALL(*m_tablet_writer, Close())
        .WillOnce(Return(true));

    EXPECT_FALSE(m_table_builder->CreateTable(m_options));
}

TEST_F(TableBuilderTest, CloseWriterFail) {
    EXPECT_CALL(*m_field_productor, Reset(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, SetBuffer(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, GetSchemaColumnStat(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, BuildTabletSchema(_, _, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, SetBuffer(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_tablet_writer, Open(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_field_productor, HasRecord())
        .WillOnce(Return(false));
    EXPECT_CALL(*m_tablet_writer, Close())
        .WillOnce(Return(false));

    EXPECT_FALSE(m_table_builder->CreateTable(m_options));
}

TEST_F(TableBuilderTest, CreateEmptyTableSuccess) {
    EXPECT_CALL(*m_field_productor, Reset(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, SetBuffer(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, GetSchemaColumnStat(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, BuildTabletSchema(_, _, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, SetBuffer(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_tablet_writer, Open(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_field_productor, HasRecord())
        .WillOnce(Return(false));
    EXPECT_CALL(*m_tablet_writer, Close())
        .WillOnce(Return(true));

    EXPECT_TRUE(m_table_builder->CreateTable(m_options));
}

TEST_F(TableBuilderTest, GetRecordFieldsFail) {
    EXPECT_CALL(*m_field_productor, Reset(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, SetBuffer(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, GetSchemaColumnStat(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, BuildTabletSchema(_, _, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, SetBuffer(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_tablet_writer, Open(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_field_productor, HasRecord())
        .WillOnce(Return(true));
    EXPECT_CALL(*m_field_productor, NextRecordFields(_, _))
        .WillOnce(Return(false));
    EXPECT_CALL(*m_tablet_writer, Close())
        .WillOnce(Return(true));

    EXPECT_FALSE(m_table_builder->CreateTable(m_options));
}

TEST_F(TableBuilderTest, WriteFieldFail) {
    EXPECT_CALL(*m_field_productor, Reset(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, SetBuffer(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, GetSchemaColumnStat(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, BuildTabletSchema(_, _, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, SetBuffer(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_tablet_writer, Open(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_field_productor, HasRecord())
        .WillOnce(Return(true));
    EXPECT_CALL(*m_field_productor, NextRecordFields(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, Write(_, _))
        .WillOnce(Return(false));
    EXPECT_CALL(*m_tablet_writer, Close())
        .WillOnce(Return(true));

    EXPECT_FALSE(m_table_builder->CreateTable(m_options));
}

TEST_F(TableBuilderTest, CreateTableSuccess) {
    EXPECT_CALL(*m_field_productor, Reset(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, SetBuffer(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_field_productor, GetSchemaColumnStat(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, BuildTabletSchema(_, _, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, SetBuffer(_, _))
        .WillOnce(Return());
    EXPECT_CALL(*m_tablet_writer, Open(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_field_productor, HasRecord())
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    EXPECT_CALL(*m_field_productor, NextRecordFields(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, Write(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, Close())
        .WillOnce(Return(true));

    EXPECT_TRUE(m_table_builder->CreateTable(m_options));
}

}  // namespace io
}  // namespace gunir
