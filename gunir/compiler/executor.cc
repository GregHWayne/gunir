// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//

#include "gunir/compiler/executor.h"

#include <iostream>
#include <string>

#include "toft/base/string/number.h"

#include "gunir/compiler/schema_builder.h"
#include "gunir/io/slice.h"


DEFINE_bool(gunir_local_run, false,
            "local run use simple planner, remote run use parallel planner");
DEFINE_int32(gunir_compiler_write_buffer_size, 1024*1024,
             "size of write buffer");

namespace gunir {
namespace compiler {

Executor::Executor()
    : m_block_helper(FLAGS_gunir_compiler_write_buffer_size) {}

bool Executor::Init(const PlanProto& plan_proto,
                    const std::string& result_schema,
                    const std::string& message_name) {
    Plan* plan = Plan::InitPlanFromProto(plan_proto);
    return Init(plan, result_schema, message_name);
}

bool Executor::Init(Plan* plan,
                    const std::string& result_schema,
                    const std::string& message_name) {
    m_plan.reset(plan);

    // Get result columns
    TableSchema schema;
    CHECK(schema.InitSchemaFromFileDescriptorProto(
            result_schema, message_name))
        << "Executor init from schema failed";

    m_column_infos = schema.GetAllColumnInfo();

    for (size_t i = 0; i < m_tuple.size(); ++i) {
        delete m_tuple[i];
    }
    m_tuple.resize(m_column_infos.size());
    for (size_t i = 0; i < m_column_infos.size(); ++i) {
        m_tuple[i] = new DatumBlock(m_column_infos[i].m_type);
    }

    // init slice storage
    m_slice.reset(new io::Slice(m_column_infos.size()));
    return true;
}

Executor::~Executor() {
    for (size_t i = 0; i < m_tuple.size(); ++i) {
        delete m_tuple[i];
    }
}

bool Executor::Run() {
    int64_t count = 0;
    io::Slice* slice;

    m_block_helper.Reset();
    std::vector<io::Slice> slices;

    if (NextSlice(&slice)) {
        if (slice == NULL) {
            return false;
        }
        slices.push_back(*slice);
        count++;
    }

    while (NextSlice(&slice)) {

        if (slice == NULL) {
            return false;
        }
        uint32_t select_level = 0;

        for (size_t i = 0; i < m_tuple.size(); ++i) {
            const DatumBlock* datum_block = m_tuple[i];
            if (select_level < datum_block->m_rep_level) {
                select_level = datum_block->m_rep_level;
            }
        }

        if (select_level == 0) {
            if (!m_container->Insert(slices)) {
                return false;
            }
            slices.clear();
            m_block_helper.Reset();

            // by wk for not clear current slice
            if (!CopyToSlice(m_slice.get())) {
                return false;
            } else {
                slice = m_slice.get();
            }
        }
        slices.push_back(*slice);
        count++;
    }

    if (slices.size() > 0) {
        if (!m_container->Insert(slices)) {
            return false;
        }
        slices.clear();
        m_block_helper.Reset();
    }

    LOG(INFO) << "Execute complete, write slice number " << count;
    return true;
}

bool Executor::NextSlice(io::Slice** slice_ptr) {
    uint32_t select_level;
    bool has_more = m_plan->GetNextTuple(m_tuple, &select_level);

    if (has_more) {

        if (!CopyToSlice(m_slice.get())) {
            *slice_ptr = NULL;
            return true;
        }

        if (FLAGS_gunir_local_run) {
            PrintTuple();
        }
        *slice_ptr = (m_slice.get());
    }
    return has_more;
}

bool Executor::CopyToSlice(io::Slice* slice) {
    io::Block* block;

    for (size_t i = 0; i < m_tuple.size(); ++i) {

        slice->SetHasBlock(i, true);
        block = slice->MutableBlock(i);

        const DatumBlock* datum_block = m_tuple[i];

        block->SetRepLevel(datum_block->m_rep_level);
        block->SetDefLevel(datum_block->m_def_level);

        slice->SetHasBlock(i, datum_block->m_has_block);

        if (datum_block->m_is_null) {
            block->SetValueType(io::Block::TYPE_NULL);
            continue;
        }

        const Datum* datum = &(datum_block->m_datum);
        const char* source_ptr;
        io::Block::VALUE_TYPE type = io::Block::TYPE_UNDEFINED;
        int32_t length;

        switch (datum_block->GetType()) {
        case BigQueryType::INT32:
            source_ptr = reinterpret_cast<const char*>(&(datum->_INT32));
            type = io::Block::TYPE_INT32;
            length = io::Block::kValueTypeLength[type];
            break;

        case BigQueryType::UINT32:
            source_ptr = reinterpret_cast<const char*>(&(datum->_UINT32));
            type = io::Block::TYPE_UINT32;
            length = io::Block::kValueTypeLength[type];
            break;

        case BigQueryType::INT64:
            source_ptr = reinterpret_cast<const char*>(&(datum->_INT64));
            type = io::Block::TYPE_INT64;
            length = io::Block::kValueTypeLength[type];
            break;

        case BigQueryType::UINT64:
            source_ptr = reinterpret_cast<const char*>(&(datum->_UINT64));
            type = io::Block::TYPE_UINT64;
            length = io::Block::kValueTypeLength[type];
            break;

        case BigQueryType::FLOAT:
            source_ptr = reinterpret_cast<const char*>(&(datum->_FLOAT));
            type = io::Block::TYPE_FLOAT;
            length = io::Block::kValueTypeLength[type];
            break;

        case BigQueryType::DOUBLE:
            source_ptr = reinterpret_cast<const char*>(&(datum->_DOUBLE));
            type = io::Block::TYPE_DOUBLE;
            length = io::Block::kValueTypeLength[type];
            break;

        case BigQueryType::BOOL:
            source_ptr = reinterpret_cast<const char*>(&(datum->_BOOL));
            type = io::Block::TYPE_BOOL;
            length = io::Block::kValueTypeLength[type];
            break;

        case BigQueryType::STRING:
            source_ptr = reinterpret_cast<const char*>(datum->_STRING->c_str());
            type = io::Block::TYPE_STRING;
            length = datum->_STRING->length();
            break;
        case BigQueryType::BYTES:
            source_ptr = static_cast<const char*>(datum->_BYTES->m_data);
            length = datum->_BYTES->m_size;
            type = io::Block::TYPE_BYTES;
            break;

        default:
            LOG(FATAL) << "Not supported type:" << type;
            break;
        }
        if (!m_block_helper.SetBlockValue(block, type, source_ptr, length)) {
            LOG(ERROR) << "m_data_holder.Write error";
            return false;
        }
    }
    return true;
}

void Executor::PrintTuple() {
    using namespace std;
    uint32_t select_level = 0;

    cout << "| ";
    for (size_t i = 0; i < m_tuple.size(); ++i) {
        if (m_tuple[i]->m_is_null) {
            std::cout << "null r:" <<
                m_tuple[i]->m_rep_level <<
                " d:" << m_tuple[i]->m_def_level << " | ";
            continue;
        }

        DatumBlock* datum_block = m_tuple[i];
        if (select_level < datum_block->m_rep_level) {
            select_level = datum_block->m_rep_level;
        }

        const Datum& data = m_tuple[i]->m_datum;
        BQType type = m_column_infos[i].m_type;

        switch (type) {
        case BigQueryType::INT32:
            cout << data._INT32;
            break;

        case BigQueryType::UINT32:
            cout << data._UINT32;
            break;

        case BigQueryType::INT64:
            cout << data._INT64;
            break;

        case BigQueryType::UINT64:
            cout << data._UINT64;
            break;

        case BigQueryType::FLOAT:
            cout << data._FLOAT;
            break;

        case BigQueryType::DOUBLE:
            cout << data._DOUBLE;
            break;

        case BigQueryType::BOOL:
            cout << data._BOOL;
            break;

        case BigQueryType::STRING:
            cout << *data._STRING;
            break;

        case BigQueryType::BYTES:
            cout << std::string(static_cast<const char*>(data._BYTES->m_data),
                                data._BYTES->m_size);
            break;

        default:
            cout << "complicate data";
        }

        cout << " r:" << datum_block->m_rep_level
            << " d:" << datum_block->m_def_level
            << " t:" << type
            << " h:" << datum_block->m_has_block << " | ";
    }
    cout << endl;
}

} // namespace compiler
} // namespace gunir

