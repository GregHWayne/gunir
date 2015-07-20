// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Created: 04/13/12
// Description:

#ifndef GUNIR_IO_AUTOMATON_H
#define GUNIR_IO_AUTOMATON_H

#include <map>
#include <string>
#include <vector>

#include "thirdparty/protobuf/descriptor.h"


namespace gunir {
namespace io {

class Automaton {
private:
    typedef ::std::string string;
    typedef ::std::vector<string> StringVector;
    typedef google::protobuf::Descriptor Descriptor;
    typedef google::protobuf::FieldDescriptor FieldDescriptor;

public:
    static const char* kDelimiter;
    static const char* kFinishState;

    Automaton();

    ~Automaton();

    bool Init(const Descriptor* descriptor);

    // construct fsm with all qualified fields in the descriptor of Init
    void ConstructFSM();

    // construct fsm with limited fields
    // all field-name should be qualifield in the descriptor of Init
    void ConstructFSM(const StringVector& field_names);

    const string& GetRootField();

    const string& GetTransition(const string& field, int level);

    bool IsBackEdge(const string& field, int level);

    uint32_t GetFieldIndex(const string& field);

    string ToString();

private:
    void AddTransition(const string& field,
                       int level,
                       const string& destination,
                       bool is_back_edge);

    void ResetTransition(const StringVector& fields);

    int GetMaxRepLevel(const string& field);

    int GetCommonRepLevel(const string& field, const string& barrier);

    void ParseField(const Descriptor* descriptor,
                    const string& full_path,
                    StringVector* fields);

private:
    const Descriptor* m_descriptor;
    string m_root_field;
    std::map<string, StringVector> m_transition;
    std::map<string, std::vector<bool> > m_back_edge;
    std::map<string, uint32_t> m_field_index;
};

} // namespace io
} // namespace gunir

#endif // GUNIR_IO_AUTOMATON_H
