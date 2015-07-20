// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
//

#include <string>

#include "gunir/io/slice.h"

#include "toft/base/string/compare.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"

namespace gunir {
namespace io {

class SliceTest : public ::testing::Test {
protected:
    virtual void SetUp() {}
    virtual void TearDown() {}
};

bool operator==(const Block& obj1, const Block& obj2) {
    if (obj1.IsNull() != obj2.IsNull()) {
        LOG(ERROR) << "IsNull is not the same. " << obj1.IsNull()
                   << " vs " << obj2.IsNull();
        return false;
    }
    if (obj1.GetRepLevel() != obj2.GetRepLevel()) {
        LOG(ERROR) << "GetRepLevel is not the same. " << obj1.GetRepLevel()
                   << " vs " << obj2.GetRepLevel();
        return false;
    }
    if (obj1.GetDefLevel() != obj2.GetDefLevel()) {
        LOG(ERROR) << "GetDefLevel is not the same. " << obj1.GetDefLevel()
                   << " vs " << obj2.GetDefLevel();
        return false;
    }
    StringPiece sp1 = obj1.GetValue();
    StringPiece sp2 = obj2.GetValue();
    if (obj1.GetValueType() != obj2.GetValueType()) {
        LOG(ERROR) << "GetValueType is not the same. " << obj1.GetValueType()
                   << " vs " << obj2.GetValueType();
        return false;
    }
    if (obj1.GetBlockSerializedSize() != obj2.GetBlockSerializedSize()) {
        LOG(ERROR) << "GetBlockSerializedSizeis not the same. "
                   << obj1.GetBlockSerializedSize()
                   << " vs " << obj2.GetBlockSerializedSize();
        return false;
    }
    if (CompareByteString(sp1.as_string(), sp2.as_string()) != 0) {
        LOG(ERROR) << "GetValue is not the same. " << sp1.as_string()
                   << " vs " << sp2.as_string();
        return false;
    }

    return true;
}

bool operator==(const Slice& obj1, const Slice& obj2) {
    if (obj1.GetCount() != obj2.GetCount()) {
        LOG(ERROR) << "Count is not the same. " << obj1.GetCount()
                   << " vs " << obj2.GetCount();
        return false;
    }
    if (obj1.GetSliceStructureType() != obj2.GetSliceStructureType()) {
        LOG(ERROR) << "slice type is not the same. "
                   << obj1.GetSliceStructureType()
                   << " vs " << obj2.GetSliceStructureType();
        return false;
    }
    for (uint32_t i = 0; i < obj1.GetCount(); ++i) {
        if (obj1.HasBlock(i) != obj2.HasBlock(i)) {
            LOG(ERROR) << "HasBlock is not the same. " << obj1.HasBlock(i)
                       << " vs " << obj2.HasBlock(i);
            return false;
        }
        if (!obj1.HasBlock(i)) {
            continue;
        }
        if (!(*(obj1.GetBlock(i)) == *(obj2.GetBlock(i)))) {
            LOG(ERROR) << "GetBlock is not the same. Index " << i << ".";
            return false;
        }
    }
    return true;
}

TEST_F(SliceTest, SerializeToStringWithBigErrorBlock) {
    Slice sobj(1);
    char *buf = new char[(1 << Block::kMaxValueBit) + 1024];
    uint32_t value_len = 0;

    uint32_t index = 0;
    sobj.SetHasBlock(index);
    Block *bobj = sobj.MutableBlock(index);
    bobj->SetRepLevel((1 << Block::kMaxLevelBit) + 1);
    EXPECT_FALSE(sobj.SerializeToString(buf, 1024, &value_len));

    bobj->SetRepLevel(1 << Block::kMaxLevelBit);
    bobj->SetDefLevel((1 << Block::kMaxLevelBit) + 1);
    EXPECT_FALSE(sobj.SerializeToString(buf, 1024, &value_len));

    bobj->SetDefLevel(1 << Block::kMaxLevelBit);
    bobj->SetValueType(Block::TYPE_STRING);
    StringPiece sp;
    sp.set(&sp, (1 << Block::kMaxValueBit) + 1);
    bobj->SetValue(sp);
    EXPECT_FALSE(sobj.SerializeToString(buf,
                                        (1 << Block::kMaxValueBit) + 1024,
                                        &value_len));
    delete[] buf;
}

TEST_F(SliceTest, SerializeToStringWithNullBlock) {
    Slice sobj(100);

    uint32_t index = 45;
    sobj.SetHasBlock(index, true);
    Block *bobj = sobj.MutableBlock(index);
    bobj->SetRepLevel(13);
    bobj->SetDefLevel(10);

    char buf[1024];
    uint32_t value_len = 0;
    EXPECT_TRUE(sobj.SerializeToString(buf, 1024, &value_len));

    Slice sobj2;
    EXPECT_EQ(kOk, sobj2.LoadFromString(buf, 1024, &value_len));
    EXPECT_TRUE(sobj2 == sobj);
    EXPECT_EQ(sobj2.ByteSize(), value_len);

    EXPECT_EQ(static_cast<uint32_t>(100), sobj2.GetCount());
    EXPECT_TRUE(sobj2.HasBlock(index));
    EXPECT_FALSE(NULL == sobj2.MutableBlock(index));
    EXPECT_EQ(Slice::TYPE_UNKOWN, sobj2.GetSliceStructureType());

    const Block *bobj2 = sobj2.GetBlock(index);
    EXPECT_TRUE(bobj2->IsNull());
    EXPECT_EQ(13, bobj2->GetRepLevel());
    EXPECT_EQ(10, bobj2->GetDefLevel());
    EXPECT_EQ(Block::TYPE_NULL, bobj2->GetValueType());

    EXPECT_EQ(sobj.ByteSize(), value_len);
}

TEST_F(SliceTest, SerializeToStringWithValueBlock) {
    Slice sobj(10);

    uint32_t index = 4;
    sobj.SetHasBlock(index, true);
    Block *bobj = sobj.MutableBlock(index);
    bobj->SetRepLevel(16);
    bobj->SetDefLevel(12);
    StringPiece sp(reinterpret_cast<const unsigned char*>(&index), sizeof(index));
    bobj->SetValueType(Block::TYPE_UINT32);
    bobj->SetValue(sp);

    char buf[1024];
    uint32_t value_len = 0;
    EXPECT_TRUE(sobj.SerializeToString(buf, 1024, &value_len));

    char result_binary[] = {
        0x00, 0x00, 0x00, 0x00, 0x08, 0x00, // head
        0x10, 0x0c, 0x02, 0x00, 0x00, 0x00, 0x00, // block 4
    };

    uint32_t temp_value = 0;
    temp_value |= 0x00000000; // TYPE_UNKOWN
    temp_value |= 0x0000000a; // count : 10
    memcpy(result_binary, &temp_value, sizeof(temp_value));
    memcpy(result_binary + 9, &index, sizeof(index));

    EXPECT_EQ(sizeof(result_binary), value_len);
    EXPECT_TRUE(CompareByteString(result_binary, sizeof(result_binary),
                                  buf, value_len) == 0);

    EXPECT_EQ(sobj.ByteSize(), value_len);
}

TEST_F(SliceTest, SerializeToStringWithStringBlock) {
    Slice sobj(512);
    const char *str = "welcome to use gunir.";

    uint32_t index = 256;
    sobj.SetHasBlock(index, true);
    Block *bobj = sobj.MutableBlock(index);
    bobj->SetRepLevel(32);
    bobj->SetDefLevel(24);
    StringPiece sp(str, strlen(str));
    bobj->SetValueType(Block::TYPE_STRING);
    bobj->SetValue(sp);

    uint32_t buf_len = 10 * 1024 * 1024;
    char *buf = new char[buf_len];
    uint32_t value_len = 0;
    EXPECT_TRUE(sobj.SerializeToString(buf, buf_len, &value_len));
    EXPECT_EQ(sobj.ByteSize(), value_len);

    Slice sobj2;
    EXPECT_EQ(kOk, sobj2.LoadFromString(buf, buf_len, &value_len));
    EXPECT_EQ(sobj2.ByteSize(), value_len);

    EXPECT_TRUE(sobj == sobj2);
    delete []buf;
}

TEST_F(SliceTest, SerializeToStringAndLoadFromString) {
    Slice sobj(10);

    uint32_t index = 0;
    sobj.SetHasBlock(index, true);
    Block *bobj = sobj.MutableBlock(index);
    bobj->SetValueType(Block::TYPE_NULL);

    index = 1;
    sobj.SetHasBlock(index, true);
    bobj = sobj.MutableBlock(index);
    bobj->SetRepLevel(16);
    bobj->SetDefLevel(12);
    StringPiece sp(reinterpret_cast<const unsigned char*>(&index),
                   sizeof(index));
    bobj->SetValueType(Block::TYPE_UINT32);
    bobj->SetValue(sp);

    uint32_t buf_len = 10 * 1024 * 1024;
    char *buf = new char[buf_len];
    uint32_t value_len = 0;
    EXPECT_TRUE(sobj.SerializeToString(buf, buf_len, &value_len));
    EXPECT_EQ(sobj.ByteSize(), value_len);

    Slice sobj2;
    EXPECT_EQ(kOk, sobj2.LoadFromString(buf, buf_len, &value_len));
    EXPECT_EQ(sobj2.ByteSize(), value_len);

    EXPECT_TRUE(sobj == sobj2);
    delete []buf;
}

TEST_F(SliceTest, StringValue) {
    uint32_t count = 256;
    Slice sobj(count);
    sobj.SetSliceStructureType(Slice::TYPE_PB);

    const char *buf[] = {
        "test data.",
        "hello world!",
        "gunir\nis good!",
        "slice should serialized before sending.",
    };
    uint32_t index = 0;
    for (uint32_t i = 0; i < count; ++i) {
        if (i % 2 == 0) {
            sobj.SetHasBlock(i, true);
            Block *bobj = sobj.MutableBlock(i);
            if (i % 4 == 0) {
                bobj->SetRepLevel(index % 64);
                bobj->SetDefLevel(index % 64);
                StringPiece sp(buf[index % 4], strlen(buf[index % 4]));
                index++;
                bobj->SetValueType(Block::TYPE_STRING);
                bobj->SetValue(sp);
            } else {
                bobj->SetRepLevel(i % 64);
                bobj->SetDefLevel(i % 64);
                StringPiece sp;
                sp.set(&index, sizeof(index));
                bobj->SetValueType(Block::TYPE_UINT32);
                bobj->SetValue(sp);
            }
        }
    }

    uint32_t buf_len = 10 * 1024 * 1024;
    char *result = new char[buf_len];
    uint32_t value_len = 0;
    EXPECT_TRUE(sobj.SerializeToString(result, buf_len, &value_len));
    EXPECT_EQ(sobj.ByteSize(), value_len);

    Slice sobj2;
    EXPECT_EQ(kOk, sobj2.LoadFromString(result, buf_len, &value_len));
    EXPECT_EQ(sobj2.ByteSize(), value_len);

    EXPECT_TRUE(sobj == sobj2);

    delete []result;
}

TEST_F(SliceTest, OperateCount) {
    uint32_t count = 100;
    Slice sobj(count);
    EXPECT_EQ(count, sobj.GetCount());
}

TEST_F(SliceTest, OperateHasBlock) {
    Slice sobj(100);
    for (uint32_t i = 0; i < 100; ++i) {
        EXPECT_FALSE(sobj.HasBlock(i));
        sobj.SetHasBlock(i, true);
        EXPECT_TRUE(sobj.HasBlock(i));
        sobj.SetHasBlock(i, false);
        EXPECT_FALSE(sobj.HasBlock(i));
    }
}

TEST_F(SliceTest, OperateSliceStructureType) {
    Slice sobj;
    EXPECT_EQ(Slice::TYPE_UNKOWN, sobj.GetSliceStructureType());
    sobj.SetSliceStructureType(Slice::TYPE_NON_NESTED);
    EXPECT_EQ(Slice::TYPE_NON_NESTED, sobj.GetSliceStructureType());
    sobj.SetSliceStructureType(Slice::TYPE_PB);
    EXPECT_EQ(Slice::TYPE_PB, sobj.GetSliceStructureType());
    sobj.SetSliceStructureType(Slice::TYPE_UNKOWN);
    EXPECT_EQ(Slice::TYPE_UNKOWN, sobj.GetSliceStructureType());
}

TEST_F(SliceTest, OperateBlock) {
    Slice sobj(10);
    for (uint32_t i = 0; i < 10; ++i) {
        EXPECT_EQ(NULL, sobj.GetBlock(i));
    }
    for (uint32_t i = 0; i < 10; ++i) {
        sobj.SetHasBlock(i, true);
        Block *bobj = sobj.MutableBlock(i);

        bobj->SetRepLevel(static_cast<int>(i));
        bobj->SetDefLevel(static_cast<int>(i));
        StringPiece sp(reinterpret_cast<const unsigned char*>(&i), sizeof(i));
        bobj->SetValueType(Block::TYPE_UINT32);
        bobj->SetValue(sp);
    }
    for (uint32_t i = 0; i < 10; ++i) {
        const Block *null_obj = NULL;
        EXPECT_NE(null_obj, sobj.GetBlock(i));
        EXPECT_TRUE(sobj.HasBlock(i));

        const Block *bobj = sobj.GetBlock(i);
        EXPECT_FALSE(bobj->IsNull());
        EXPECT_EQ(static_cast<int>(i), bobj->GetRepLevel());
        EXPECT_EQ(static_cast<int>(i), bobj->GetDefLevel());
        StringPiece sp = bobj->GetValue();
        EXPECT_EQ(i, *(reinterpret_cast<const uint32_t*>(sp.data())));
        EXPECT_EQ(sizeof(i), sp.size());
        EXPECT_EQ(Block::TYPE_UINT32, bobj->GetValueType());
    }
}

TEST_F(SliceTest, Reset) {
    Slice sobj(10);
    for (uint32_t i = 0; i < 10; ++i) {
        EXPECT_EQ(NULL, sobj.GetBlock(i));
    }
    for (uint32_t i = 0; i < 10; ++i) {
        sobj.SetHasBlock(i, true);
        Block *bobj = sobj.MutableBlock(i);

        bobj->SetRepLevel(static_cast<int>(i));
        bobj->SetDefLevel(static_cast<int>(i));
        StringPiece sp(reinterpret_cast<const unsigned char*>(&i), sizeof(i));
        bobj->SetValueType(Block::TYPE_UINT32);
        bobj->SetValue(sp);
    }

    sobj.Reset();
    EXPECT_EQ(sobj.GetCount(), 10U);

    for (uint32_t i = 0; i < 10; ++i) {
        EXPECT_EQ(NULL, sobj.GetBlock(i));
        sobj.SetHasBlock(i, true);
        const Block *bobj = sobj.GetBlock(i);
        EXPECT_TRUE(bobj->IsNull());
        EXPECT_EQ(bobj->GetRepLevel(), 0);
        EXPECT_EQ(bobj->GetDefLevel(), 0);
    }
}

TEST_F(SliceTest, DebugString) {
    Slice sobj(2);
    for (uint32_t i = 0; i < 2; ++i) {
        sobj.SetHasBlock(i, true);
        Block *bobj = sobj.MutableBlock(i);

        bobj->SetRepLevel(static_cast<int>(i));
        bobj->SetDefLevel(static_cast<int>(i));
        StringPiece sp(reinterpret_cast<const unsigned char*>(&i), sizeof(i));
        bobj->SetValueType(Block::TYPE_UINT32);
        bobj->SetValue(sp);
    }

    std::string content = "----Slice DebugString----\nBlock 0:\n\
Repeated Level: 0\nDefinition Level: 0\nValue Type: TYPE_UINT32\n\
Value: 2\n--------\nBlock 1:\nRepeated Level: 1\nDefinition Level: 1\n\
Value Type: TYPE_UINT32\nValue: 2\n----End DebugString----\n";
    std::string debug_string = sobj.DebugString();
    EXPECT_EQ(content, debug_string);
    LOG(ERROR) << debug_string;
}

} // namespace io
} // namespace gunir
