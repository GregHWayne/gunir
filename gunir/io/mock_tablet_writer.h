// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)

#ifndef GUNIR_IO_MOCK_TABLET_WRITER_H
#define GUNIR_IO_MOCK_TABLET_WRITER_H

#include <string>
#include <vector>

#include "thirdparty/gmock/gmock.h"

#include "gunir/io/slice.h"
#include "gunir/io/tablet_writer.h"

namespace gunir {
namespace io {

class MockTabletWriter : public TabletWriter {
public:
    MockTabletWriter() {}
    MOCK_METHOD2(SetBuffer, void(char* buffer, uint32_t len));
    MOCK_METHOD2(BuildTabletSchema,
                 bool(const std::string& table_name, // NO_LINT
                      const SchemaDescriptor& schema_descriptor));
    MOCK_METHOD3(BuildTabletSchema,
                 bool(const std::string& table_name, // NO_LINT
                      const SchemaDescriptor& schema_descriptor,
                      const std::vector<ColumnStaticInfo>& info));
    MOCK_METHOD1(Open, bool(const std::string& file_name));
    MOCK_METHOD0(Close, bool());
    MOCK_METHOD2(Write, bool(const std::vector<const Block*>& fields,
                             const std::vector<uint32_t>& indexes));
    MOCK_METHOD1(GetOutputFileList, void(std::vector<std::string> *file_list));
};

}  // namespace io
}  // namespace gunir

#endif  // GUNIR_IO_MOCK_TABLET_WRITER_H
