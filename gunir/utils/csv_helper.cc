// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
#include "gunir/utils/csv_helper.h"

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/algorithm.h"
#include "toft/storage/file/file.h"

DECLARE_string(gunir_csv_delim);

using namespace toft;

namespace gunir {
bool CsvHelper::CreateMessageByCsvFile(const std::string& csv_file,
                                       const std::string& message) {
    scoped_ptr<File> file(File::Open(csv_file.c_str(), "r"));
    if (file.get() == NULL) {
        LOG(ERROR) << "Open file " << csv_file << " error";
        return false;
    }

    std::string read_line;
    int32_t ret = file->ReadLine(&read_line);
    if (ret < 0) {
        LOG(ERROR) << "Load schema from " <<  csv_file << " error !";
        return false;
    }

    return CreateMessageByCsvString(read_line, message);
}

bool CsvHelper::CreateMessageByCsvString(const std::string& csv_string,
                                         const std::string& message) {
    std::vector<std::string> columns;
    SplitStringKeepEmpty(csv_string, FLAGS_gunir_csv_delim, &columns);
    if (columns.size() == 0) {
        LOG(ERROR) << "Parse no columns ";
        return false;
    }

    google::protobuf::FileDescriptorSet proto_set;
    google::protobuf::FileDescriptorProto* file = proto_set.add_file();
    file->set_name("from_csv");
    google::protobuf::DescriptorProto* descriptor = file->add_message_type();
    descriptor->set_name(message);
    for (size_t i = 0; i < columns.size(); ++i) {
        std::string column = columns[i];
        StringTrim(&column, '\n');
        StringTrim(&column);
        if (column.length() == 0) {
            LOG(ERROR) << "Parse one empty column ";
            return false;
        }
        CreateFieldByString(descriptor->add_field(),
                            column,
                            i + 1);
    }

    proto_set.SerializeToString(&m_descriptor_string);
    return true;
}

void CsvHelper::CreateFieldByString(google::protobuf::FieldDescriptorProto* f,
                                    const std::string& column,
                                    uint32_t number) {
    f->set_name(column);
    f->set_number(number);
    f->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
    f->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
}

}  // namespace gunir
