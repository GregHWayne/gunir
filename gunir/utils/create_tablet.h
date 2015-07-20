// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_UTILS_CREATE_TABLET_H
#define  GUNIR_UTILS_CREATE_TABLET_H

#include <string>

#include "toft/base/string/string_piece.h"
#include "toft/base/scoped_ptr.h"
// #include "mapreduce/reducer.h"

#include "gunir/io/tablet_schema.pb.h"

using namespace toft;

namespace gunir {

namespace io {
class RecordDissector;
class TabletWriter;
} // namespace io

class CreateTablet {
public:
    CreateTablet();
    ~CreateTablet();

    bool Setup(const std::string& schema_string,
               const std::string& tablet_name,
               const std::string& output_prefix);

    bool Flush();

    bool WriteMessage(const StringPiece& value);

private:
    scoped_array<char> m_buffer;
    scoped_ptr<io::RecordDissector> m_dissector;
    scoped_ptr<io::TabletWriter> m_tablet_writer;
};

}  // namespace gunir
#endif  // GUNIR_UTILS_CREATE_TABLET_H
