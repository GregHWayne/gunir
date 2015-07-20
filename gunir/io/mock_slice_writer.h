// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_IO_MOCK_SLICE_WRITER_H
#define  GUNIR_IO_MOCK_SLICE_WRITER_H

#include <string>
#include <vector>

#include "gunir/io/slice_writer.h"

namespace gunir {
namespace io {

class MockSliceWriter : public SliceWriter {
public:
    MockSliceWriter() {}
    MOCK_METHOD1(SetTabletWriter,
                 void(TabletWriter *writer));
    MOCK_METHOD3(Open,
                 bool(const std::string& table_name, // NO_LINT
                      const SchemaDescriptor& schema_descriptor,
                      const std::string& file_name_prefix));
    MOCK_METHOD1(Close,
                 bool(std::vector<std::string> *file_list)); // NO_LINT
    MOCK_METHOD1(Write,
                 bool(const std::vector<Slice>& slice)); // NO_LINT
};

}  // namespace io
}  // namespace gunir

#endif  // GUNIR_IO_MOCK_SLICE_WRITER_H
