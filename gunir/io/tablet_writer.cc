// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#include "gunir/io/tablet_writer.h"

#include <vector>

#include "toft/base/string/number.h"
#include "toft/storage/path/path.h"
#include "toft/crypto/uuid/uuid.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "gunir/io/data_holder.h"
#include "gunir/io/block.h"
#include "gunir/io/column_metadata.pb.h"
#include "gunir/io/column_writer.h"
#include "gunir/io/tablet_schema.pb.h"
#include "gunir/utils/proto_message.h"

namespace gunir {
namespace io {

TabletWriter::TabletWriter()
    : m_write_size(0),
      m_tablet_schema_size(0) {
}

TabletWriter::~TabletWriter() {
    DestroyColumnWriters();
}

void TabletWriter::SetBuffer(char* buffer, uint32_t len) {
    m_tablet_buffer_size = len;
    m_data_holder.reset(new DataHolder(buffer, len));
}

bool TabletWriter::BuildTabletSchema(const std::string& table_name,
                                     const SchemaDescriptor& schema_descriptor) {
    // parse schema_descriptor to ColumnStaticInfo
    // with ProtoMessage method GetSchemaColumnStat
    ProtoMessage des_message;
    if (!des_message.CreateMessageByFileDescriptorSet(
            schema_descriptor.description(),
            schema_descriptor.record_name())) {
        LOG(ERROR) << "des_message.CreateMessageByFileDescriptorSet error";
        return false;
    }
    std::vector<io::ColumnStaticInfo> info;
    des_message.GetSchemaColumnStat(&info);

    return BuildTabletSchema(table_name, schema_descriptor, info);
}

bool TabletWriter::BuildTabletSchema(const std::string& table_name,
    const SchemaDescriptor& schema_descriptor,
    const std::vector<ColumnStaticInfo>& info) {

    m_tablet_schema.reset(new TabletSchema());
    m_tablet_schema->set_table_name(table_name);
    m_tablet_schema->mutable_schema_descriptor()->CopyFrom(schema_descriptor);

    std::vector<ColumnStaticInfo>::const_iterator it;
    for (it = info.begin(); it != info.end(); ++it) {
        ColumnMetaData *cmd = m_tablet_schema->add_column_metadatas();
        cmd->mutable_static_info()->CopyFrom(*it);
        cmd->mutable_dynamic_info()->set_start_position(0);
        cmd->mutable_dynamic_info()->set_length(0);
    }

    return true;
}

bool TabletWriter::Open(const std::string& file_name) {
    m_file_name = file_name;
    m_dump_files.clear();

    if (!InitColumnWriters()) {
        LOG(ERROR) << "Init column writers error.";
        return false;
    }

    m_tablet_schema_size = static_cast<uint32_t>(m_tablet_schema->ByteSize());
    m_write_size = GetTabletSchemaSize();
    return true;
}

bool TabletWriter::Close() {
    bool ret = true;
    if (m_write_size > GetTabletSchemaSize()) {
        // Dump the data still in mem pool when closing.
        if (!Dump()) {
            LOG(ERROR) << "Write tablet error.";
            ret = false;
        }
    }

    DestroyColumnWriters();

    return ret;
}

bool TabletWriter::Write(const std::vector<const Block*>& blocks,
                         const std::vector<uint32_t>& indexes) {
    DCHECK_EQ(blocks.size(), indexes.size());

    // 1, compute the total block size
    uint32_t total_block_size = 0;
    for (uint32_t i = 0; i < blocks.size(); ++i) {
        total_block_size += blocks[i]->GetBlockSerializedSize();
    }

    // 2, dump tablet if total size is big enough
    if (m_write_size + total_block_size >= m_tablet_buffer_size) {
        if (!Dump()) {
            LOG(ERROR) << "Write tablet error.";
            return false;
        }
    }

    // 3, write blocks
    for (uint32_t i = 0; i < blocks.size(); ++i) {
        if (!Write(*blocks[i], indexes[i])) {
            LOG(ERROR) << "Write std::vector<Block> error.";
            return false;
        }
    }

    DCHECK_LT(m_write_size, m_tablet_buffer_size);
    return true;
}

void TabletWriter::GetOutputFileList(
    std::vector<std::string> *file_list) const {
    *file_list = m_dump_files;
}

bool TabletWriter::Write(const Block& block, uint32_t index) {
    if (m_column_writers.size() <= index) {
        LOG(ERROR) << "Unknown column index.";
        return false;
    }

    // get buffer to write
    uint32_t serialize_length = block.GetBlockSerializedSize();
    char* buffer = m_data_holder->Reserve(serialize_length);
    DCHECK_NOTNULL(buffer);

    // write to buffer
    uint32_t value_len = 0;
    if (!block.SerializeToString(buffer, &value_len)) {
        LOG(ERROR) << "Serialize Block error.";
        return false;
    }
    DCHECK_EQ(serialize_length, value_len);

    // add buffer to column_writer
    m_column_writers[index]->Write(buffer, value_len);
    m_write_size += serialize_length;

    return true;
}

bool TabletWriter::InitColumnWriters() {
    // 1. Init ColumnWriters.
    int writer_number = m_tablet_schema->column_metadatas_size();
    m_column_writers.resize(writer_number, NULL);

    for (int i = 0; i < writer_number; ++i) {
        ColumnWriter *column_writer = new ColumnWriter(
            m_tablet_schema->mutable_column_metadatas(i)
            ->mutable_dynamic_info());

        // Put the column writer into the right position
        // accroding to column index information.
        int position =
            m_tablet_schema->column_metadatas(i).static_info().column_index();
        // The check for the column index.
        if (m_column_writers[position] != NULL) {
            delete column_writer;
            LOG(ERROR) << "The column index is error.";
            return false;
        } else {
            m_column_writers[position] = column_writer;
        }
    }

    return true;
}

void TabletWriter::DestroyColumnWriters() {
    // 1. Release the column writers.
    std::vector<ColumnWriter*>::iterator it;
    for (it = m_column_writers.begin(); it != m_column_writers.end(); ++it) {
        delete *it;
    }
    m_column_writers.clear();
}

bool TabletWriter::Dump() {
    // 1. Open the file to write.
    toft::scoped_ptr<toft::File> file;
    std::string current_file_name = m_file_name
        + "_" + NumberToString(m_dump_files.size());

    LOG(INFO) << "Start dump file : " << current_file_name;

    std::string temp_file_name = toft::Path::GetDirectory(current_file_name)
        + "temp_" + toft::CreateCanonicalUUIDString();

    file.reset(File::Open(temp_file_name, "w"));
    CHECK_NOTNULL(file.get());

    // 2. Dump value data.
    std::vector<ColumnWriter*>::iterator it;
    for (it = m_column_writers.begin(); it != m_column_writers.end(); ++it) {
        if (!(*it)->Flush(file.get())) {
            file->Close();
            File::Delete(temp_file_name);
            return false;
        }
    }

    // 3. Dump tablet schema.
    std::string str;
    m_tablet_schema->SerializeToString(&str);
    uint32_t len = str.length();
    if (-1 == file->Write(str.c_str(), len)) {
        LOG(ERROR) << "Write tablet schema error.";
        file->Close();
        File::Delete(temp_file_name);
        return false;
    }
    if (-1 == file->Write(reinterpret_cast<const void*>(&len), sizeof(len))) {
        LOG(ERROR) << "Write tablet schema length error.";
        file->Close();
        File::Delete(temp_file_name);
        return false;
    }
    DCHECK_EQ(m_write_size, file->Tell());

    // 4. Close the file.
    if (-1 == file->Close()) {
        LOG(ERROR) << "Close tablet file error.";
        File::Delete(temp_file_name);
        return false;
    }
    file.reset();

    // 5. Rename the file.
    if (File::Rename(temp_file_name, current_file_name)) {
        m_dump_files.push_back(current_file_name);
    }

    // 6. Reset data_holder
    m_data_holder->Reset();
    m_write_size = GetTabletSchemaSize();
    LOG(INFO) << "Finished dump file : " << current_file_name;
    return true;
}

uint32_t TabletWriter::GetTabletSchemaSize() const {
    return m_tablet_schema_size + sizeof(m_tablet_schema_size);
}

}  // namespace io
}  // namespace gunir
