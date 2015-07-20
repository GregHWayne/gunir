// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//

#include "gunir/io/column_reader.h"

#include "toft/base/closure.h"
#include "toft/system/threading/this_thread.h"
#include "thirdparty/glog/logging.h"

DEFINE_int32(gunir_column_read_block_size, 1024 * 1024, "");
// DEFINE_int64(gunir_blocking_queue_time);

using namespace toft;

namespace gunir {
namespace io {

ColumnReader::ColumnReader(const ColumnStaticInfo& info, MemPool* mempool)
    : m_is_empty(false), m_finish_read(false), m_static_info(info),
      m_buffer(NULL), m_start_buffer_ptr(NULL),
      m_end_buffer_ptr(NULL), m_load_size(0),
      m_remain_size(0), m_size(0),
      m_offset(0), m_mempool(mempool) {
}

ColumnReader::~ColumnReader() {
    Close();
}

bool ColumnReader::Open(const std::string& filename,
                        int64_t offset,
                        int64_t size) {
    m_file.reset(File::Open(filename.c_str(), "r"));
//     m_file.reset(File::Open(filename.c_str(),
//                             File::ENUM_FILE_OPEN_MODE_R
//                             | File::ENUM_FILE_IO_NON_BLOCKING));
    if (m_file.get() == NULL) {
        LOG(ERROR) << "Open file error";
        return false;
    }
    m_buffer = static_cast<char*>(m_mempool->Allocate(size));
    m_start_buffer_ptr = m_buffer;
    m_end_buffer_ptr = m_buffer;
    m_remain_size = size;
    m_size = size;
    m_offset = offset;
    bool ret = LoadColumnData(m_start_buffer_ptr,
                              offset,
                              FLAGS_gunir_column_read_block_size,
                              0);
    if (!ret) {
        return false;
    }

    // Column reader holds two blocks: the block and the next block.
    // While opening, the block is null, and the next block will read
    // the first block data in buffer.
    return NextBlock();
}

void ColumnReader::ReadCallback(uint32_t retry_times,
                                int64_t read_size,
                                uint32_t error_code) {
    if (read_size < 0 || error_code != 0) {
        LOG(WARNING) << "Async read error with error_code: " << error_code <<
            "; Retry times: " << retry_times;
        if (retry_times > kMaxRetryTimes) {
            // Push NULL when read failed more than kMaxRetryTimes
            m_queue.Push(NULL);
            return;
        }
        retry_times++;
    } else {
        retry_times = 0;
        m_load_size += read_size;
        m_queue.Push(m_buffer + m_load_size);
    }
    // m_finish_read is true when Close method is called;
    // m_load_size == m_size when all the data are loaded;
    if (!m_finish_read && m_load_size < m_size) {
        // If load failed, the callback function will not be called
        bool load_success = false;
        uint32_t submit_retry_times = 0;
        while (!load_success) {
            if (submit_retry_times > kMaxRetryTimes) {
                m_queue.Push(NULL);
                break;
            }
            load_success = LoadColumnData(m_buffer + m_load_size,
                                          m_offset + m_load_size,
                                          FLAGS_gunir_column_read_block_size,
                                          retry_times);
            if (!load_success) {
                submit_retry_times++;
                ThisThread::Sleep(10);
            }
        }
    } else {
        // Push NULL when all the data are read or no need to read anymore.
        m_queue.Push(NULL);
    }
}

bool ColumnReader::LoadColumnData(char* buffer, int64_t offset,
                                  int64_t size, uint32_t retry_times) {
    if (m_load_size + size > m_size) {
        size = m_size - m_load_size;
    }
#if 0
    Closure<void (int64_t, unsigned int)> *callback =
        NewClosure(this, &ColumnReader::ReadCallback, retry_times);
    uint32_t error_code;
    int64_t ret =
        m_file->AsyncReadFrom(buffer, size, offset, callback,
                              File::kDefaultAsyncTimeout, &error_code);
    if (-1 == ret) {
        LOG(ERROR) << "Read tablet data file error: "
            << File::GetErrorCodeString(error_code);
        return false;
    }
#else
    if (!m_file->Seek(offset, SEEK_SET)) {
        LOG(ERROR) << "error in file offset: " << offset;
        return false;
    }
    int64_t read_count = m_file->Read(buffer, size);
    CHECK(read_count == size);
    m_end_buffer_ptr += read_count;
    m_load_size += read_count;
//     m_remain_size -= read_count;
#endif
    return true;
}

bool ColumnReader::Close() {
    if (!m_finish_read) {
        m_finish_read = true;
//         while (m_queue.Pop(&m_end_buffer_ptr,
//                                      FLAGS_gunir_blocking_queue_time)) {
//            // When NULL poped, it means the load data thread has complete work
//             if (m_end_buffer_ptr == NULL) {
//                 break;
//             }
//         }
    }
    if (m_buffer != NULL) {
        m_mempool->Free(m_buffer);
        m_buffer = NULL;
    }
    m_remain_size = 0;
    if (m_file.get() != NULL) {
        if (!m_file->Close()) {
            LOG(ERROR) << "Close tablet file error.";
            return false;
        }
        m_file.reset();
    }
    return true;
}

bool ColumnReader::GetAvailableBuffLen(int32_t* buff_len) {
    bool load_success = LoadColumnData(m_buffer + m_load_size,
                                       m_offset + m_load_size,
                                       FLAGS_gunir_column_read_block_size);
    CHECK(load_success);
    *buff_len = m_end_buffer_ptr - m_start_buffer_ptr;
    return true;
}

bool ColumnReader::NextBlock() {
    m_block = m_next_block;
    if (m_remain_size > 0) {
        int32_t buf_len = m_end_buffer_ptr - m_start_buffer_ptr;
        ErrorCode error_code = ReadBlock(m_start_buffer_ptr, buf_len);
        while (kOk != error_code) {
            if (error_code == kBufNotEnough) {
                if (!GetAvailableBuffLen(&buf_len)) {
                    return false;
                }
                error_code = ReadBlock(m_start_buffer_ptr, buf_len);
                continue;
            }
            LOG(ERROR) << "Read block error.";
            return false;
        }
        return true;
    } else if (!m_is_empty && m_remain_size == 0) {
        SetBlockNull(&m_next_block);
        m_is_empty = true;
        return true;
    } else {
        return false;
    }
}

void ColumnReader::GetBlock(Block *block) {
    *block = m_block;
}

int ColumnReader::GetRepetitionLevel() const {
    return m_block.GetRepLevel();
}

int ColumnReader::NextRepetitionLevel() const {
    return m_next_block.GetRepLevel();
}

int ColumnReader::GetDefinitionLevel() const {
    return m_block.GetDefLevel();
}

int ColumnReader::GetMaxDefinitionLevel() const {
    return m_static_info.max_definition_level();
}

int64_t ColumnReader::GetColumnReadBytes() const {
    return m_load_size;
}

bool ColumnReader::GetColumnType(ColumnType *column_type) const {
    *column_type = m_static_info.column_type();
    return true;
}

bool ColumnReader::GetColumnName(std::string *column_name) const {
    *column_name = m_static_info.column_name();
    return true;
}

ErrorCode ColumnReader::ReadBlock(char* buffer, int32_t buf_len) {
    CHECK_NOTNULL(buffer);
    CHECK_GE(buf_len, 0);
    uint32_t read_len = 0;
    ErrorCode error_code
        = m_next_block.LoadFromString(buffer, buf_len, &read_len);
    if (error_code == kOk) {
        m_start_buffer_ptr += read_len;
        m_remain_size -= read_len;
    }
    return error_code;
}

void ColumnReader::SetBlockNull(Block *block) {
    block->SetValueType(Block::TYPE_NULL);
    block->SetRepLevel(0);
    block->SetDefLevel(0);
}
} // namepsace io
} // namespace gunir
