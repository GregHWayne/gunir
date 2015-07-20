// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
#ifndef GUNIR_IO_HISTORY_INFO_IO_H
#define GUNIR_IO_HISTORY_INFO_IO_H

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"
#include "toft/storage/file/file.h"
#include "thirdparty/protobuf/message.h"

namespace gunir {
namespace io {

class HistoryInfoWriter {
public:
    HistoryInfoWriter();
    ~HistoryInfoWriter();

    bool Reset(toft::File *file);

    bool WriteMessage(const ::google::protobuf::Message& message);
    bool WriteRecord(const char *data, uint32_t size);
    bool WriteRecord(const toft::StringPiece& data);

private:
    bool Write(const char *data, uint32_t size);

private:
    toft::File *m_file;
};

class HistoryInfoReader {
public:
    HistoryInfoReader();
    ~HistoryInfoReader();

    bool Reset(toft::File *file);
    // return 1 stands for ok, 0 for no more data, -1 for error.
    int Next();

    bool ReadMessage(::google::protobuf::Message *message);
    bool ReadRecord(const char **data, uint32_t *size);
    bool ReadRecord(toft::StringPiece *data);

private:
    bool Read(char *data, uint32_t size);

private:
    toft::File *m_file;
    toft::scoped_array<char> m_buffer;
    uint32_t m_buffer_size;
    uint32_t m_data_size;
};

} // namespace io
} // namespace gunir
#endif  // GUNIR_IO_HISTORY_INFO_IO_H
