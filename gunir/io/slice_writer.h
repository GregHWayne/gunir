// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#ifndef GUNIR_IO_SLICE_WRITER_H
#define GUNIR_IO_SLICE_WRITER_H

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"

#include "gunir/io/tablet_schema.pb.h"

namespace gunir {
namespace io {
class Slice;
class TabletWriter;

class SliceWriter {
public:
    SliceWriter();
    virtual ~SliceWriter();

    virtual void SetTabletWriter(TabletWriter *writer);

    virtual bool Open(const std::string& table_name,
                      const SchemaDescriptor& schema_descriptor,
                      const std::string& file_name_prefix);

    // Write all slices of a record.
    virtual bool Write(const std::vector<Slice>& slices);

    virtual bool Close(std::vector<std::string> *file_list);

private:
    toft::scoped_ptr<TabletWriter> m_tablet_writer;
    char* m_buffer;
};

} // namespace io
} // namespace gunir

#endif  // GUNIR_IO_SLICE_WRITER_H
