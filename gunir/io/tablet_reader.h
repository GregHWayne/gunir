// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
// Description:
// TabletReader is responsible for reading the blocks of tablet.
// It will generate slice while reading each time.
//
#ifndef GUNIR_IO_TABLET_READER_H
#define GUNIR_IO_TABLET_READER_H

#include <map>
#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/uncopyable.h"
#include "toft/storage/file/file.h"
#include "toft/system/memory/mempool.h"

#include "gunir/io/column_metadata.pb.h"
#include "gunir/io/tablet_schema.pb.h"

using namespace toft;

namespace gunir {
namespace io {
class Block;
class ColumnReader;
class Slice;

class TabletReader {
    DECLARE_UNCOPYABLE(TabletReader);

public:
    explicit TabletReader(MemPool* mempool);
    ~TabletReader();
    // Read all columns.
    bool Init(const std::string& tablet_file);
    // Projection-reading: Only picking some of the columns to read.
    bool Init(const std::string& tablet_file,
              const std::vector<std::string>& column_names);
    bool Close();
    bool Next();
    Slice* GetSlice();
    void GetTabletSchema(TabletSchema* tablet_schema);
    int64_t GetTabletReadBytes() const;

    bool ReadColumn(const std::string& column_name, Block* block);
    int MaxDefLevel(const std::string& column_name);
    int NextRepLevel(const std::string& column_name);

private:
    bool LoadTabletSchema();
    bool GetNeedColumnMetaData(const std::vector<std::string>& column_names,
                               std::vector<ColumnMetaData*>* meta);
    bool InitColumnReaders(const std::vector<ColumnMetaData*>& metas);
    bool LoadColumnData(char* buffer, int64_t offset, int64_t size);

    bool Fetch();
    int GetFetchLevel();
    bool DestroyColumnReaders();

    void FreeBuffer();

private:
    int m_fetch_level;

    std::string m_tablet_file_name;
    std::vector<ColumnReader*> m_column_readers;
    std::map<std::string, ColumnReader*> m_column_map;

    MemPool* m_mempool;
    scoped_ptr<TabletSchema> m_tablet_schema;
    scoped_ptr<File> m_file;
    scoped_ptr<Slice> m_slice;

    int64_t m_tablet_read_bytes;
};

}  // namespace io
}  // namespace gunir

#endif  // GUNIR_IO_TABLET_READER_H
