// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Created: 03/23/12
// Description:

#ifndef GUNIR_IO_MOCK_DISSECTOR_H
#define GUNIR_IO_MOCK_DISSECTOR_H

#include <vector>

#include "thirdparty/gmock/gmock.h"

#include "gunir/io/dissector.h"

namespace gunir {
namespace io {

// Mock for RecordImporter.
class MockRecordImporter : public RecordImporter {
public:
    MockRecordImporter() {}
    MOCK_METHOD1(Open, bool(const ::google::protobuf::RepeatedPtrField<URI>&
                            files));
    MOCK_METHOD0(Close, bool());
    MOCK_METHOD1(ReadRecord, bool(StringPiece *value));
    MOCK_METHOD0(NextRecord, bool());
    MOCK_METHOD0(HasRecord, bool());
};

class MockRecordDissector : public RecordDissector {
public:
    MockRecordDissector() {}
    MOCK_METHOD1(Init, bool(const SchemaDescriptor& schema_descriptor));
    MOCK_METHOD2(SetBuffer, void(char* buffer, uint32_t length));
    MOCK_METHOD3(DissectRecord, bool(const StringPiece& data,
                                std::vector<const Block*>* output_fields,
                                std::vector<uint32_t>* indexes));
    MOCK_METHOD1(GetSchemaColumnStat, bool(std::vector<ColumnStaticInfo>* column_stats));
};

} // namespace io
} // namespace gunir

#endif  // GUNIR_IO_MOCK_DISSECTOR_H

