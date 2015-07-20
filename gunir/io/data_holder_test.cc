// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <memory.h>
#include <string>

#include "toft/base/string/compare.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/data_holder.h"

using namespace toft;

namespace gunir {

class DataHolderTest : public ::testing::Test {
};

TEST_F(DataHolderTest, WriteTrue) {
    uint32_t length = 1024;
    char hold_buffer[1024];
    DataHolder data_holder(hold_buffer, length);

    std::string str = "hello world! hello data holder!";
    char *data_ptr = data_holder.Write(str.data(), str.size());

    EXPECT_NE(static_cast<char*>(NULL), data_ptr);
    EXPECT_EQ(0, CompareByteString(str.data(), str.size(),
                                   data_ptr, str.size()));
}

TEST_F(DataHolderTest, WriteFalse) {
    uint32_t length = 1024;
    char hold_buffer[1024];
    DataHolder data_holder(hold_buffer, length);

    char buffer[1025];
    memset(buffer, 0, length + 1);

    char *data_ptr = data_holder.Write(buffer, length);
    EXPECT_NE(static_cast<char*>(NULL), data_ptr);

    data_holder.Reset();
    data_ptr = data_holder.Write(buffer, length + 1);
    EXPECT_EQ(static_cast<char*>(NULL), data_ptr);
}

} // namespace gunir
