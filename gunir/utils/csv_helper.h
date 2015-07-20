// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_UTILS_CSV_HELPER_H
#define  GUNIR_UTILS_CSV_HELPER_H

#include <string>

#include "thirdparty/protobuf/descriptor.pb.h"

namespace gunir {

class CsvHelper {
public:
    bool CreateMessageByCsvFile(const std::string& csv_file,
                                const std::string& message);
    bool CreateMessageByCsvString(const std::string& csv_string,
                                  const std::string& message);
    std::string GetFileDescriptorSetString() const {
        return m_descriptor_string;
    }

private:
    void CreateFieldByString(google::protobuf::FieldDescriptorProto* f,
                             const std::string& column,
                             uint32_t number);

private:
    std::string  m_descriptor_string;
};

}  // namespace gunir
#endif  // GUNIR_UTILS_CSV_HELPER_H
