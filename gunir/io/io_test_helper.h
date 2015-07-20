// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
#ifndef GUNIR_IO_IO_TEST_HELPER_H
#define GUNIR_IO_IO_TEST_HELPER_H

#include <stdint.h>

#include <string>
#include <vector>

#include "gunir/io/testdata/document.pb.h"

namespace gunir {
namespace io {

class IOTestHelper {
public:
    IOTestHelper();
    ~IOTestHelper();

    void BuildRecordIOMessageTestFile(uint32_t num = 10);
    void GetRecordIOMessageTestFileName(std::string *file_name) const;
    void BuildTabletTestFile(const std::string& table_name,
                             std::vector<std::string> *output_files);

    Document RandDocument();

private:
    Name RandName();
    Language RandLanguage();
    Links RandLinks();
    uint32_t RandNumber();

private:
    std::string m_recordio_msg_file_name;
    uint32_t m_file_count;
};

} // namespace io
} // namespace gunir
#endif  // GUNIR_IO_IO_TEST_HELPER_H
