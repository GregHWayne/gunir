// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#ifndef GUNIR_IO_TABLE_BUILDER_H
#define GUNIR_IO_TABLE_BUILDER_H

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/uncopyable.h"

#include "gunir/io/table_options.pb.h"

namespace gunir {
namespace io {

class TabletWriter;
class FieldProductor;

// TableBuilder focus on converting non-columnar data to one logical table
// which is stored as a number of tablets
// users should provide certain input and output information by Reset and the
// converting will be done by calling CreatTable
class TableBuilder {
public:
    TableBuilder();

    TableBuilder(TabletWriter* tablet_writer,
                 FieldProductor* field_productor);

    ~TableBuilder();

    // generate output file on columnar storage
    // using input data which specified by options
    bool CreateTable(const TableOptions& options);

    bool CreateTable(const TableOptions& options,
                     std::vector<std::string>* output_files);

private:
    // construct schema descriptor and open writer
    bool InitFileSetting(const TableOptions& options);

    // get fields of each record and write them to tablet one by one
    bool ProcessRecords();

private:
    DECLARE_UNCOPYABLE(TableBuilder);

    toft::scoped_ptr<TabletWriter> m_tablet_writer;
    toft::scoped_ptr<FieldProductor> m_field_productor;
    toft::scoped_array<char> m_buffer;
};

}  // namespace io
}  // namespace gunir

#endif  // GUNIR_IO_TABLE_BUILDER_H
