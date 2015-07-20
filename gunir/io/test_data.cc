// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#include "gunir/io/test_data.h"

#include "toft/base/string/number.h"
#include "toft/system/time/timestamp.h"

using namespace google;
using namespace toft;

namespace gunir {
namespace io {

const int kUniqueValueCount = 10;
const int kMaxRepeatedSize = 3;
const int kMaxStringLength = 20;
const int kFloatRange = 10;

const double kOptionalProb = 0.8;
const double kRepeatedProb = 0.6;

TestData::TestData()
    : m_readable(true),
      m_unique_value_count(kUniqueValueCount),
      m_max_repeated_size(kMaxRepeatedSize),
      m_max_string_length(kMaxStringLength),
      m_float_range(kFloatRange) {
}

TestData::~TestData() {
    for (size_t i = 0; i < m_data.size(); i++) {
        delete m_data[i];
    }
}

const protobuf::Message& TestData::operator[](uint32_t index) const {
    return *m_data[index];
}

int TestData::size() const {
    return m_data.size();
}

void TestData::Reset(const Message& message) {
    Reset(&message);
}

void TestData::Reset(const Message* message) {
    m_random.reset(new PseudoRandom(GetTimestamp()));
    m_proto_type.reset(message->New());
}

void TestData::GenerateMessages(uint32_t message_number) {
    for (uint32_t i = 0; i < message_number; i++) {
        Message* new_message = m_proto_type->New();
        CreateMessage(new_message);
        m_data.push_back(new_message);
    }
}

void TestData::CreateMessage(Message* message) {
    const Descriptor* descriptor = message->GetDescriptor();
    const Reflection* reflection = message->GetReflection();

    for (int i = 0; i < descriptor->field_count(); i++) {
        const FieldDescriptor* field = descriptor->field(i);
        int field_size = 0;
        if (field->is_repeated()) {
            while (field_size < m_max_repeated_size && RandomRepeated())
                field_size++;
        } else {
            field_size = field->is_required() || RandomOptional();
        }

        for (int j = 0; j < field_size; j++) {
            if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
                Message* sub_message = field->is_repeated()
                    ? reflection->AddMessage(message, field)
                    : reflection->MutableMessage(message, field);
                CreateMessage(sub_message);
            } else {
                AddValue(message, field);
            }
        }
    }
}

void TestData::AddValue(Message* message, const FieldDescriptor* field) {
    const Reflection* reflection = message->GetReflection();
    switch (field->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32: {
            int32_t value;
            value = m_readable
                ? RandomUInt32() % m_unique_value_count
                : RandomInt32();
            field->is_repeated()
                ? reflection->AddInt32(message, field, value)
                : reflection->SetInt32(message, field, value);
            break;
        }

        case FieldDescriptor::CPPTYPE_INT64: {
            int64_t value;
            value = m_readable
                ? RandomUInt64() % m_unique_value_count
                : RandomInt64();
            field->is_repeated()
                ? reflection->AddInt64(message, field, value)
                : reflection->SetInt64(message, field, value);
            break;
        }

        case FieldDescriptor::CPPTYPE_UINT32: {
            uint32_t value;
            value = m_readable
                ? RandomUInt32() % m_unique_value_count
                : RandomUInt32();
            field->is_repeated()
                ? reflection->AddUInt32(message, field, value)
                : reflection->SetUInt32(message, field, value);
            break;
        }

        case FieldDescriptor::CPPTYPE_UINT64: {
            uint64_t value;
            value = m_readable
                ? RandomUInt64() % m_unique_value_count
                : RandomUInt64();
            field->is_repeated()
                ? reflection->AddUInt64(message, field, value)
                : reflection->SetUInt64(message, field, value);
            break;
        }

        case FieldDescriptor::CPPTYPE_FLOAT: {
            float value;
            value = m_readable
                ? RandomUInt32() % (m_float_range * 10) / 10.0
                : RandomFloat();
            field->is_repeated()
                ? reflection->AddFloat(message, field, value)
                : reflection->SetFloat(message, field, value);
            break;
        }

        case FieldDescriptor::CPPTYPE_DOUBLE: {
            double value;
            value = m_readable
                ? RandomUInt32() % (m_float_range * 10) / 10.0
                : RandomDouble();
            field->is_repeated()
                ? reflection->AddDouble(message, field, value)
                : reflection->SetDouble(message, field, value);
            break;
        }

        case FieldDescriptor::CPPTYPE_BOOL: {
            field->is_repeated()
                ? reflection->AddBool(message, field, RandomBool())
                : reflection->SetBool(message, field, RandomBool());
            break;
        }

        case FieldDescriptor::CPPTYPE_ENUM: {
            field->is_repeated()
                ? reflection->AddEnum(message, field, RandomEnum(field))
                : reflection->SetEnum(message, field, RandomEnum(field));
            break;
        }

        case FieldDescriptor::CPPTYPE_STRING: {
            string value;
            uint32_t suffix = RandomUInt32() % m_unique_value_count;
            value = m_readable
                ? field->name() + NumberToString(suffix)
                : RandomString();
            field->is_repeated()
                ? reflection->AddString(message, field, value)
                : reflection->SetString(message, field, value);
            break;
        }

        default:
            LOG(ERROR) << "unknown type";
    }
}

bool TestData::RandomOptional() {
    return RandomProb() < kOptionalProb;
}

bool TestData::RandomRepeated() {
    return RandomProb() < kRepeatedProb;
}

double TestData::RandomProb() {
    return m_random->NextDouble();
}

int32_t TestData::RandomInt32() {
    return RandomUInt32();
}

int64_t TestData::RandomInt64() {
    return RandomUInt64();
}

uint32_t TestData::RandomUInt32() {
    return m_random->NextUInt32();
}

uint64_t TestData::RandomUInt64() {
    uint64_t value = RandomUInt32();
    value =  (value << 32) + RandomUInt32();
    return value;
}

double TestData::RandomDouble() {
    uint64_t value = RandomUInt64();
    return *reinterpret_cast<double*>(&value);
}

float TestData::RandomFloat() {
    uint32_t value = RandomUInt32();
    return *reinterpret_cast<float*>(&value);
}

bool TestData::RandomBool() {
    return RandomUInt32() % 2;
}

const protobuf::EnumValueDescriptor* TestData::RandomEnum(
                                const FieldDescriptor* field) {
    const EnumDescriptor* enum_desc = field->enum_type();
    int random_index = RandomUInt32() % enum_desc->value_count();
    return enum_desc->value(random_index);
}

std::string TestData::RandomString() {
    int length = RandomUInt32() % m_max_string_length + 1;
    char* buffer = new char[length];
    m_random->NextBytes(buffer, length);

    string value = buffer;
    delete[] buffer;
    return value;
}

void TestData::SetSeed(int64_t seed) {
    m_random->SetSeed(seed);
}

void TestData::SetReadable(bool readable) {
    m_readable = readable;
}

void TestData::SetUniqueValueCount(int unique_value_count) {
    m_unique_value_count = unique_value_count;
}

void TestData::SetMaxRepeatedSize(int max_repeated_size) {
    m_max_repeated_size = max_repeated_size;
}

void TestData::SetMaxStringLength(int max_string_length) {
    m_max_string_length = max_string_length;
}

void TestData::SetFloatRange(int float_range) {
    m_float_range = float_range;
}

} // namespace io
} // namespace gunir
