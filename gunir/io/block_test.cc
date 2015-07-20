// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
//

#include <string>

#include "toft/base/string/compare.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/block.h"

namespace gunir {
namespace io {

class BlockTest : public ::testing::Test {
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

TEST_F(BlockTest, CopyConstructor) {
    Block bobj;
    Block bobj2 = bobj;
    EXPECT_TRUE(bobj == bobj2);

    bobj.SetRepLevel(18);
    bobj.SetDefLevel(12);
    StringPiece sp;
    sp.set(&bobj, sizeof(bobj));
    bobj.SetValueType(Block::TYPE_UNDEFINED);
    bobj.SetValue(sp);

    Block bobj3 = bobj;
    EXPECT_TRUE(bobj == bobj3);
    EXPECT_FALSE(bobj2 == bobj3);
}

TEST_F(BlockTest, Reset) {
    Block bobj;

    bobj.SetRepLevel(1);
    bobj.SetDefLevel(1);
    StringPiece value;
    value.set(&bobj, sizeof(bobj));
    bobj.SetValueType(Block::TYPE_UNDEFINED);
    bobj.SetValue(value);

    bobj.Reset();

    EXPECT_TRUE(bobj.IsNull());
    EXPECT_EQ(bobj.GetRepLevel(), 0);
    EXPECT_EQ(bobj.GetDefLevel(), 0);
    value = bobj.GetValue();
    EXPECT_TRUE(value.empty());
}

TEST_F(BlockTest, SerializeToStringNullBlock) {
    Block bobj;

    char buf[1024];
    memset(buf, 0, 1024);
    uint32_t value_len = 0;
    EXPECT_TRUE(bobj.SerializeToString(buf, &value_len));
}

TEST_F(BlockTest, ErrorSerializeToString) {
    Block bobj;
    bobj.SetRepLevel(257);

    char buf[1024];
    memset(buf, 0, 1024);
    uint32_t value_len = 0;
    EXPECT_FALSE(bobj.SerializeToString(buf, &value_len));

    bobj.SetRepLevel(0);
    StringPiece sp;
    sp.set(&bobj, (1 << Block::kMaxValueBit) + 1);
    bobj.SetValueType(Block::TYPE_STRING);
    bobj.SetValue(sp);
    EXPECT_FALSE(bobj.SerializeToString(buf, &value_len));
}

TEST_F(BlockTest, SerializeToStringAndLoadFromString) {
    Block bobj;

    bobj.SetRepLevel(18);
    bobj.SetDefLevel(12);
    StringPiece sp;
    sp.set(&bobj, sizeof(bobj));
    bobj.SetValueType(Block::TYPE_UNDEFINED);
    bobj.SetValue(sp);

    char buf[1024];
    memset(buf, 0, 1024);
    uint32_t value_len = 0;
    EXPECT_TRUE(bobj.SerializeToString(buf, &value_len));

    Block bobj2;
    uint32_t read_len = 0;
    EXPECT_EQ(kOk, bobj2.LoadFromString(buf, 1024, &read_len));

    EXPECT_EQ(read_len, value_len);
    EXPECT_TRUE(bobj == bobj2);
}

TEST_F(BlockTest, DefaultArgs) {
    Block obj;
    EXPECT_TRUE(obj.IsNull());
    EXPECT_EQ(Block::TYPE_NULL, obj.GetValueType());
    EXPECT_EQ(0, obj.GetRepLevel());
    EXPECT_EQ(0, obj.GetDefLevel());
}

TEST_F(BlockTest, SetArgs) {
    Block *obj = new Block();
    EXPECT_TRUE(obj->IsNull());
    EXPECT_EQ(Block::TYPE_NULL, obj->GetValueType());
    EXPECT_EQ(0, obj->GetRepLevel());
    EXPECT_EQ(0, obj->GetDefLevel());

    obj->SetRepLevel(32);
    obj->SetDefLevel(32);
    obj->SetValueType(Block::TYPE_INT32);

    EXPECT_FALSE(obj->IsNull());
    EXPECT_EQ(Block::TYPE_INT32, obj->GetValueType());
    EXPECT_EQ(32, obj->GetRepLevel());
    EXPECT_EQ(32, obj->GetDefLevel());

    delete obj;
}

TEST_F(BlockTest, DefaultValue) {
    Block *obj = new Block();

    uint32_t size = 0;
    StringPiece sp;
    EXPECT_EQ(NULL, sp.data());
    EXPECT_EQ(size, sp.size());

    sp = obj->GetValue();
    EXPECT_EQ(NULL, sp.data());
    EXPECT_EQ(size, sp.size());

    delete obj;
}

TEST_F(BlockTest, SetValue) {
    Block obj;

    uint32_t size = 0;
    StringPiece sp;
    EXPECT_EQ(NULL, sp.data());
    EXPECT_EQ(size, sp.size());

    sp = obj.GetValue();
    EXPECT_EQ(NULL, sp.data());
    EXPECT_EQ(size, sp.size());

    std::string buf = "hello world!";
    StringPiece sp2(buf);
    obj.SetValueType(Block::TYPE_STRING);
    obj.SetValue(sp2);

    sp = obj.GetValue();
    EXPECT_EQ(buf.c_str(), sp.data());
    EXPECT_EQ(buf.size(), sp.size());
}

TEST_F(BlockTest, ValueTypeAndValueTypeLength) {
    EXPECT_EQ(sizeof(int32_t), // NO_LINT
              Block ::kValueTypeLength[Block::TYPE_INT32]);
    EXPECT_EQ(sizeof(int64_t), // NO_LINT
              Block::kValueTypeLength[Block::TYPE_INT64]);
    EXPECT_EQ(sizeof(uint32_t), // NO_LINT
              Block::kValueTypeLength[Block::TYPE_UINT32]);
    EXPECT_EQ(sizeof(uint64_t), // NO_LINT
              Block::kValueTypeLength[Block::TYPE_UINT64]);
    EXPECT_EQ(sizeof(int32_t), // NO_LINT
              Block::kValueTypeLength[Block::TYPE_SINT32]);
    EXPECT_EQ(sizeof(int64_t), // NO_LINT
              Block::kValueTypeLength[Block::TYPE_SINT64]);
    EXPECT_EQ(sizeof(int32_t), // NO_LINT
              Block::kValueTypeLength[Block::TYPE_FIXED32]);
    EXPECT_EQ(sizeof(int64_t), // NO_LINT
              Block::kValueTypeLength[Block::TYPE_FIXED64]);
    EXPECT_EQ(sizeof(int32_t), // NO_LINT
              Block::kValueTypeLength[Block::TYPE_SFIXED32]);
    EXPECT_EQ(sizeof(int64_t), // NO_LINT
              Block::kValueTypeLength[Block::TYPE_SFIXED64]);
    EXPECT_EQ(sizeof(float), // NO_LINT
              Block::kValueTypeLength[Block::TYPE_FLOAT]);
    EXPECT_EQ(sizeof(double), // NO_LINT
              Block::kValueTypeLength[Block::TYPE_DOUBLE]);
    EXPECT_EQ(sizeof(bool), // NO_LINT
              Block::kValueTypeLength[Block::TYPE_BOOL]);

    uint32_t len = 0;
    EXPECT_EQ(len, Block::kValueTypeLength[Block::TYPE_STRING]);
    EXPECT_EQ(len, Block::kValueTypeLength[Block::TYPE_BYTES]);
    EXPECT_EQ(len, Block::kValueTypeLength[Block::TYPE_UNDEFINED]);
}

TEST_F(BlockTest, DEBUGSTRING) {
    std::string value = "hello world!";
    Block bobj;
    bobj.SetRepLevel(1);
    bobj.SetDefLevel(2);
    bobj.SetValueType(Block::TYPE_STRING);
    bobj.SetValue(value);
    std::string debug_string = bobj.DebugString();

    std::string content = "Repeated Level: 1\nDefinition Level: 2\n\
Value Type: TYPE_STRING\nValue: hello world!\n";
    EXPECT_EQ(content, debug_string);
    LOG(ERROR) << debug_string;
}

} // namespace io
} // namespace gunir
