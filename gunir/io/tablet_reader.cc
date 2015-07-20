// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
#include "gunir/io/tablet_reader.h"

#include "toft/base/scoped_ptr.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "gunir/io/block.h"
#include "gunir/io/column_reader.h"
#include "gunir/io/slice.h"

namespace gunir {
namespace io {

TabletReader::TabletReader(MemPool* mempool)
    : m_mempool(mempool) {}

TabletReader::~TabletReader() {
    DestroyColumnReaders();
}

bool TabletReader::Init(const std::string& tablet_file) {
    const std::vector<std::string> column_names;
    return Init(tablet_file, column_names);
}

bool TabletReader::Init(const std::string& tablet_file,
                        const std::vector<std::string>& column_names) {
    m_fetch_level = 0;
    m_tablet_file_name = tablet_file;

    // 1. Open the tablet data file
    m_file.reset(File::Open(m_tablet_file_name, "r"));
    if (m_file.get() == NULL) {
        LOG(ERROR) << "Open file error";
        return false;
    }

    // 2. Load the tablet schema first.
    if (!LoadTabletSchema()) {
        LOG(ERROR) << "TabletReader get the tablet schema info error.";
        return false;
    }

    // 3. get the need column metas
    std::vector<ColumnMetaData*> metas;
    if (!GetNeedColumnMetaData(column_names, &metas)) {
        return false;
    }

    // 4. Init the column readers.
    if (!InitColumnReaders(metas)) {
        LOG(ERROR) << "TabletReader init column readers error.";
        return false;
    }

    // 5. prepare slice
    m_slice.reset(new Slice(m_column_readers.size()));
    return true;
}

bool TabletReader::LoadTabletSchema() {
    int64_t position = 0;
    // 1. seek the right pos. to find the tablet shcema length.
    uint32_t schema_size;
    int64_t length = sizeof(schema_size);

    int64_t file_size = toft::File::GetSize(m_tablet_file_name);
    if (file_size < length) {
        LOG(ERROR) << "File size " << file_size << " is less than " << length;
        return false;
    }

    position = file_size - length;
    if (!m_file->Seek(position, SEEK_SET)) {
        LOG(ERROR) << "Load tablet schema error.";
        return false;
    }

    // 2. read the tablet schema length value.
    int64_t ret = m_file->Read(reinterpret_cast<char*>(&schema_size), length);
    if (ret != length) {
        LOG(ERROR) << "Get the schema size return value : " << ret;
        return false;
    }

    // 3. read and parse the tablet schema value.
    position -= schema_size;
    if (!m_file->Seek(position, SEEK_SET)) {
        LOG(ERROR) << "Seek the tablet schema error." << schema_size
            << " | " << file_size;
        return false;
    }

    scoped_array<char> schema_buf;
    schema_buf.reset(new char[schema_size]);
    ret = m_file->Read(schema_buf.get(), schema_size);
    if (ret != schema_size) {
        LOG(ERROR) << "Get the schema value error.";
        return false;
    }
    LOG(INFO) << "here 4";

    // 4. parse the tablet schema pb.
    m_tablet_schema.reset(new TabletSchema());
    if (!m_tablet_schema->ParseFromArray(schema_buf.get(), schema_size)) {
        LOG(ERROR) << "Parse the tablet schema data error.";
        return false;
    }
    LOG(INFO) << "here 5";

    return true;
}

bool TabletReader::GetNeedColumnMetaData(
        const std::vector<std::string>& column_names,
        std::vector<ColumnMetaData*>* meta) {
    if (column_names.size() == 0) {
        meta->resize(m_tablet_schema->column_metadatas_size());
        for (int i = 0; i < m_tablet_schema->column_metadatas_size(); ++i) {
            ColumnMetaData* c_meta
                = m_tablet_schema->mutable_column_metadatas(i);
            (*meta)[c_meta->static_info().column_index()] = c_meta;
        }
        return true;
    }

    meta->resize(column_names.size());
    for (size_t i = 0; i < column_names.size(); ++i) {
        const std::string& name1 = column_names[i];
        int j = 0;
        for (; j < m_tablet_schema->column_metadatas_size(); ++j) {
            ColumnMetaData* c_meta
                = m_tablet_schema->mutable_column_metadatas(j);
            const std::string& name2 = c_meta->static_info().column_name();
            if (name1.compare(name2) == 0) {
                (*meta)[i] = c_meta;
                break;
            }
        }
        if (j == m_tablet_schema->column_metadatas_size()) {
            LOG(ERROR) << "Get unknown column :" << name1;
            return false;
        }
    }

    return true;
}

bool TabletReader::InitColumnReaders(const std::vector<ColumnMetaData*>& metas) {
    m_tablet_read_bytes = 0;
    m_column_map.clear();

    for (uint32_t i = 0; i < metas.size(); ++i) {
        const std::string& column_name = metas[i]->static_info().column_name();
        ColumnReader *creader = new ColumnReader(metas[i]->static_info(), m_mempool);
        m_column_readers.push_back(creader);
        m_column_map[column_name] = creader;

        // read column data from file
        int64_t offset = metas[i]->dynamic_info().start_position();
        int64_t size = metas[i]->dynamic_info().length();

        // open column reader with buffer
        if (!creader->Open(m_tablet_file_name, offset, size)) {
            LOG(ERROR) << "Column reader open error.";
            return false;
        }
    }

    return true;
}

bool TabletReader::Close() {
    // 1. Destroy the column readers.
    if (!DestroyColumnReaders()) {
        LOG(ERROR) << "Destroy column reader error.";
        return false;
    }

    // 2. Close the tablet data file.
    if (!m_file->Close()) {
        LOG(ERROR) << "Close tablet file error.";
        return false;
    }
    m_file.reset();

    // 3. release the tablet schema and data buffer.
    m_tablet_schema.reset();

    return true;
}

bool TabletReader::DestroyColumnReaders() {
    bool ret = true;
    std::vector<ColumnReader*>::iterator it;
    for (it = m_column_readers.begin(); it != m_column_readers.end(); it++) {
        if (!(*it)->Close()) {
            LOG(ERROR) << "Column reader close error.";
            ret = false;
        }
        m_tablet_read_bytes += (*it)->GetColumnReadBytes();
        delete *it;
        *it = NULL;
    }
    m_column_readers.clear();

    return ret;
}

bool TabletReader::Next() {
    m_slice->Reset();

    bool has_data = Fetch();

    if (!has_data) {
        return false;
    }

    int select_level = 0;
    for (uint32_t i = 0; i < m_column_readers.size(); ++i) {
        if (m_slice->HasBlock(i)) {
            Block *bobj = m_slice->MutableBlock(i);
            m_column_readers[i]->GetBlock(bobj);

            if (bobj->GetRepLevel() > select_level) {
                select_level = bobj->GetRepLevel();
            }
        }
    }
    return true;
}

Slice* TabletReader::GetSlice() {
    return m_slice.get();
}

void TabletReader::GetTabletSchema(TabletSchema* tablet_schema) {
    CHECK_NOTNULL(tablet_schema);
    tablet_schema->CopyFrom(*(m_tablet_schema.get()));
}

int64_t TabletReader::GetTabletReadBytes() const {
    return m_tablet_read_bytes;
}

bool TabletReader::Fetch() {
    bool has_more_slices = false;
    int next_level = 0;

    std::vector<ColumnReader*>::iterator it;
    for (uint32_t i = 0; i < m_column_readers.size(); ++i) {
        ColumnReader *reader = m_column_readers[i];
        if (reader->NextRepetitionLevel() >= m_fetch_level) {
            bool has_data_in_reader = reader->NextBlock();
            if (has_data_in_reader) {
                m_slice->SetHasBlock(i);
            }
            has_more_slices = has_more_slices | has_data_in_reader;
        }
        if (reader->NextRepetitionLevel() >= next_level) {
            next_level = reader->NextRepetitionLevel();
        }
    }
    m_fetch_level = next_level;
    return has_more_slices;
}

int TabletReader::GetFetchLevel() {
    return m_fetch_level;
}

bool TabletReader::ReadColumn(const std::string& column_name, Block* block) {
    std::map<std::string, ColumnReader*>::iterator it;
    ColumnReader* reader = NULL;
    if ((it = m_column_map.find(column_name)) != m_column_map.end()) {
        reader = it->second;
        if (!reader->NextBlock()) {
            return false;
        }
        reader->GetBlock(block);
        return true;
    }
    LOG(ERROR) << "Tablet reader cannot find the column name: " << column_name;
    return false;
}

int TabletReader::MaxDefLevel(const std::string& column_name) {
    ColumnReader* reader = m_column_map[column_name];
    return reader->GetMaxDefinitionLevel();
}

int TabletReader::NextRepLevel(const std::string& column_name) {
    ColumnReader* reader = m_column_map[column_name];
    return reader->NextRepetitionLevel();
}

} // namespace io
} // namespace gunir
