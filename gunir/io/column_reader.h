// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
// ColumnReader is the interface for operating the tablet data for reading.

#ifndef GUNIR_IO_COLUMN_READER_H
#define GUNIR_IO_COLUMN_READER_H

#include <stdint.h>
#include <string>

#include "toft/base/scoped_ptr.h"
#include "toft/storage/file/file.h"
#include "toft/system/atomic/atomic.h"
#include "toft/container/blocking_queue.hpp"
#include "toft/system/memory/mempool.h"

#include "gunir/io/block.h"
#include "gunir/io/column_metadata.pb.h"

namespace gunir {
namespace io {

// Read the column(field) block for tablet data file.
class ColumnReader {
public:
    ColumnReader(const ColumnStaticInfo& info, MemPool* mempool);
    ~ColumnReader();
    bool Open(const std::string& filename, int64_t offset, int64_t size);
    bool Close();
    bool NextBlock();
    void GetBlock(Block *bobj);
    int GetRepetitionLevel() const;
    int NextRepetitionLevel() const;
    int GetDefinitionLevel() const;
    bool GetColumnType(ColumnType *column_type) const;
    bool GetColumnName(std::string *column_name) const;
    int GetMaxDefinitionLevel() const;
    int64_t GetColumnReadBytes() const;

private:
    ErrorCode ReadBlock(char* buffer, int32_t buf_len);
    void SetBlockNull(Block *block);
    bool LoadColumnData(char* buffer, int64_t offset,
                        int64_t size, uint32_t retry_times = 0);
    void ReadCallback(uint32_t retry_times,
                      int64_t read_size, uint32_t error_code);
    bool GetAvailableBuffLen(int32_t* buff_len);

private:
    bool m_is_empty;
    Atomic<bool> m_finish_read;
    ColumnStaticInfo m_static_info;
    char *m_buffer;
    char *m_start_buffer_ptr;
    char *m_end_buffer_ptr;
    int64_t m_load_size;
    int64_t m_remain_size;
    int64_t m_size;
    int64_t m_offset;
    MemPool* m_mempool;

    Block m_block;
    Block m_next_block;
    uint32_t m_flag;
    scoped_ptr<File> m_file;
    BlockingQueue<char*> m_queue;

    static const uint32_t kMaxRetryTimes = 3;
};
} // namepsace io
} // namespace gunir

#endif  // GUNIR_IO_COLUMN_READER_H
