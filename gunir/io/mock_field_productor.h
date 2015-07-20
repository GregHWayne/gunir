// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)

#ifndef GUNIR_IO_MOCK_FIELD_PRODUCTOR_H
#define GUNIR_IO_MOCK_FIELD_PRODUCTOR_H

#include <vector>

#include "thirdparty/gmock/gmock.h"

#include "gunir/io/field_productor.h"

namespace gunir {
namespace io {

class RecordDissector;
class RecordImporter;

class MockFieldProductor : public FieldProductor {
public:
    MockFieldProductor() {}
    MOCK_METHOD2(Reset, void(const ::google::protobuf::RepeatedPtrField<URI>&
                             file_paths,
                             const SchemaDescriptor& schema_descriptor));
    MOCK_METHOD2(SetBuffer, void(char* buffer, uint32_t length));
    MOCK_METHOD1(GetSchemaColumnStat, bool(std::vector<ColumnStaticInfo>*
                                           column_stats));
    MOCK_METHOD0(HasRecord, bool());
    MOCK_METHOD2(NextRecordFields, bool(
            std::vector<const Block*>* output_fields,
            std::vector<uint32_t>* indexes));
};

} // namespace io
} // namespace gunir

#endif // GUNIR_IO_MOCK_FIELD_PRODUCTOR_H
