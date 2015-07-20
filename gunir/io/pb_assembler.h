// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef GUNIR_IO_PB_ASSEMBLER_H
#define GUNIR_IO_PB_ASSEMBLER_H

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "thirdparty/protobuf/descriptor.h"
#include "thirdparty/protobuf/message.h"

#include "gunir/io/automaton.h"
#include "gunir/io/slice_column_reader.h"
#include "gunir/io/tablet_reader.h"
#include "gunir/utils/proto_message.h"

namespace gunir {
namespace io {

class PbRecordAssembler {
public:
    PbRecordAssembler();
    ~PbRecordAssembler();

    bool Init(TabletReader* tablet_reader);

    bool Init(SliceColumnReader* slice_column_reader,
              const SchemaDescriptor& schema_descriptor);

    bool AssembleRecord(google::protobuf::Message* message);

    const google::protobuf::Message* GetProtoMessage() const;

private:
    typedef ::std::string string;
    typedef google::protobuf::Message Message;
    typedef google::protobuf::Descriptor Descriptor;
    typedef google::protobuf::FieldDescriptor FieldDescriptor;
    typedef google::protobuf::Reflection Reflection;

    bool InitProtoMessage(const SchemaDescriptor& schema_descriptor);

    Message* MoveToLevel(Message* message,
                         const string& full_path,
                         int def_level,
                         int new_repeat);

    void AppendValue(Message* message,
                     const string& field_name,
                     const Block& block);

    bool ReadColumn(const string& field, Block* block);

    int RepLevel(const Block& block);

    int DefLevel(const Block& block);

    int NextRepLevel(const string& field);

    int MaxDefLevel(const string& field);

    string GetName(const string& field);

    string GetPath(const string& field);

private:
    TabletReader* m_tablet_reader;
    SliceColumnReader* m_slice_column_reader;
    std::vector<ColumnStaticInfo> m_column_static_info;
    scoped_ptr<ProtoMessage> m_proto_message;
    scoped_ptr<Automaton> m_automaton;
};

} // namespace io
} // namespace gunir

#endif // GUNIR_IO_PB_ASSEMBLER_H
