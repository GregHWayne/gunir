// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Created: 04/13/12
// Description:

#include "gunir/io/automaton.h"

#include "toft/base/string/algorithm.h"
#include "toft/base/string/compare.h"
#include "toft/base/string/number.h"

using namespace toft;

namespace gunir {
namespace io {

using std::map;
using std::string;

const char* Automaton::kDelimiter = ".";
const char* Automaton::kFinishState = "--FINISH--";

Automaton::Automaton()
    : m_descriptor(NULL) {
}

Automaton::~Automaton() {
}

bool Automaton::Init(const Descriptor* descriptor) {
    m_transition.clear();
    m_back_edge.clear();
    m_field_index.clear();
    m_descriptor = descriptor;
    return m_descriptor != NULL;
}

void Automaton::ConstructFSM() {
    StringVector fields;
    ParseField(m_descriptor, m_descriptor->name(), &fields);
    ConstructFSM(fields);
}

void Automaton::ConstructFSM(const StringVector& fields) {
    ResetTransition(fields);
    m_root_field = fields[0];
    int size = fields.size();
    for (int i = 0; i < size; i++) {
        const string& field = fields[i];
        const string& barrier = (i == size - 1) ? kFinishState : fields[i+1];
        int max_level = GetMaxRepLevel(field);
        int barrier_level = GetCommonRepLevel(field, barrier);

        for (int level = 0; level <= barrier_level; level++) {
            AddTransition(field, level, barrier, false);
        }

        for (int j = 0; j <= i; j++) {
            const string& pre_field = fields[j];
            int back_level = GetCommonRepLevel(field, pre_field);
            if (back_level > barrier_level) {
                AddTransition(field, back_level, pre_field, true);
            }
        }

        for (int level = max_level - 1; level > barrier_level; level--) {
            AddTransition(field, level, GetTransition(field, level+1), true);
        }
    }
}

void Automaton::AddTransition(const string& field,
                              int level,
                              const string& destination,
                              bool is_back_edge) {
    if (GetTransition(field, level) == "") {
        m_transition[field][level] = destination;
        m_back_edge[field][level] = is_back_edge;
    }
}

const string& Automaton::GetRootField() {
    return m_root_field;
}

uint32_t Automaton::GetFieldIndex(const string& field) {
    return m_field_index[field];
}

const string& Automaton::GetTransition(const string& field, int level) {
    return m_transition[field][level];
}

bool Automaton::IsBackEdge(const string& field, int level) {
    return m_back_edge[field][level];
}

string Automaton::ToString() {
    string result;
    map<string, StringVector>::iterator it;
    for (it = m_transition.begin(); it != m_transition.end(); it++) {
        string field = it->first;
        StringVector& transition = it->second;
        for (uint32_t i = 0; i < transition.size(); i++) {
            result += "(" + field + ", " + toft::NumberToString(i) + ")";
            result += " ==> " + transition[i];
            string back = IsBackEdge(field, i)?"true":"false";
            result += " (" + back + ")\n";
        }
    }

    result += "root field : " + m_root_field + "\n";
    return result;
}

void Automaton::ResetTransition(const StringVector& fields) {
    m_transition.clear();
    m_back_edge.clear();
    m_field_index.clear();
    m_root_field = "";
    for (uint32_t i = 0; i < fields.size(); i++) {
        const string& field = fields[i];
        m_transition[field].resize(GetMaxRepLevel(field) + 1);
        m_back_edge[field].resize(GetMaxRepLevel(field) + 1);
        m_field_index[field] = i;
    }
}

int Automaton::GetMaxRepLevel(const string& full_path) {
    string path = StringRemovePrefix(full_path, m_descriptor->name());
    StringVector fields;
    SplitString(path, kDelimiter, &fields);

    int rep = 0;
    const Descriptor* descriptor = m_descriptor;
    for (uint32_t i = 0; i < fields.size(); i++) {
        const FieldDescriptor* field = descriptor->FindFieldByName(fields[i]);
        if (field->is_repeated()) {
            rep++;
        }
        if (i < fields.size() - 1) {
            descriptor = field->message_type();
        }
    }

    return rep;
}

int Automaton::GetCommonRepLevel(const string& field, const string& barrier) {
    if (field == barrier) {
        return GetMaxRepLevel(field);
    }

    int prefix_len = GetCommonPrefixLength(field, barrier);
    if (prefix_len == 0) {
        return 0;
    }

    string prefix = field.substr(0, prefix_len);
    prefix = prefix.substr(0, prefix.rfind(kDelimiter));
    return GetMaxRepLevel(prefix);
}

void Automaton::ParseField(const Descriptor* descriptor,
                           const string& full_path,
                           StringVector* fields) {
    for (int i = 0; i < descriptor->field_count(); i++) {
        const FieldDescriptor* field = descriptor->field(i);
        string field_path = full_path + kDelimiter + field->name();
        if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
            ParseField(field->message_type(), field_path, fields);
        } else {
            fields->push_back(field_path);
        }
    }
}

} // namespace io
} // namespace gunir
