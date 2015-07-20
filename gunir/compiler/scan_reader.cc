// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
// 

#include "gunir/compiler/scan_reader.h"

#include <string>

#include "toft/base/string/number.h"
#include "toft/base/string/string_piece.h"

#include "gunir/io/block.h"
#include "gunir/io/scanner.h"
#include "gunir/io/slice.h"

namespace gunir {
namespace compiler {

void ScanReader::Reset(io::Scanner* scanner, size_t column_number) {
    m_scanner = scanner;
}

bool ScanReader::Read(const std::vector<BQType>& datum_type_list,
                      const std::vector<DatumBlock*>& datum_block_list,
                      uint32_t* select_level,
                      const std::vector<uint64_t>& affect_ids) {

    io::Slice* slice = NULL;

    if (!m_scanner->NextSlice(&slice)) {
        return false;
    }

    *select_level =  0;

    CHECK(affect_ids.size() == slice->GetCount())
        << affect_ids.size() << " != " << slice->GetCount();

    for (size_t i = 0; i < affect_ids.size(); ++i) {
        const size_t& pos = affect_ids[i];
        datum_block_list[pos]->m_has_block = slice->HasBlock(i);
        if (!slice->HasBlock(i)) {
            // LOG(ERROR) << "has no block " << slice->GetBlock(i);
            continue;
        }
        const io::Block* block = slice->GetBlock(i);
        GetDatumFromBlock(block, datum_type_list[pos], datum_block_list[pos]);
        if (*select_level < datum_block_list[pos]->m_rep_level) {
            *select_level = datum_block_list[pos]->m_rep_level;
        }
    }
    return true;
}

void ScanReader::GetDatumFromBlock(const io::Block* block,
                                   BQType type,
                                   DatumBlock* datum_block) {
    datum_block->m_is_null = block->IsNull();
    datum_block->m_rep_level = block->GetRepLevel();
    datum_block->m_def_level = block->GetDefLevel();

    if (datum_block->m_is_null)
        return;

    Datum* datum = &(datum_block->m_datum);
    io::Block::VALUE_TYPE block_type = block->GetValueType();
    StringPiece value = block->GetValue();

    const char* v_ptr = value.data();

    switch (type) {
    case BigQueryType::INT32:
        if (io::Block::TYPE_INT32 == block_type)
            datum->_INT32 = *(reinterpret_cast<const int32_t*> (v_ptr));
        if (io::Block::TYPE_SINT32 == block_type)
            datum->_INT32 = *(reinterpret_cast<const int32_t*> (v_ptr));
        if (io::Block::TYPE_SFIXED32 == block_type)
            datum->_INT32 = *(reinterpret_cast<const int32_t*> (v_ptr));
        break;

    case BigQueryType::UINT32:
        if (io::Block::TYPE_UINT32 == block_type)
            datum->_UINT32 = *(reinterpret_cast<const uint32_t*> (v_ptr));
        if (io::Block::TYPE_FIXED32 == block_type)
            datum->_INT32 = *(reinterpret_cast<const int32_t*> (v_ptr));
        break;

    case BigQueryType::INT64:
        if (io::Block::TYPE_INT64 == block_type) {
            datum->_INT64 = *(reinterpret_cast<const int64_t*> (v_ptr));
        }
        if (io::Block::TYPE_SINT64 == block_type)
            datum->_INT64 = *(reinterpret_cast<const int64_t*> (v_ptr));
        if (io::Block::TYPE_SFIXED64 == block_type)
            datum->_INT64 = *(reinterpret_cast<const int64_t*> (v_ptr));
        break;

    case BigQueryType::UINT64:
        if (io::Block::TYPE_UINT64 == block_type)
            datum->_UINT64 = *(reinterpret_cast<const uint64_t*> (v_ptr));
        if (io::Block::TYPE_FIXED64 == block_type)
            datum->_INT64 = *(reinterpret_cast<const int64_t*> (v_ptr));
        break;

    case BigQueryType::BOOL:
        datum->_BOOL = *(reinterpret_cast<const bool*> (v_ptr));
        break;

    case BigQueryType::FLOAT:
        datum->_FLOAT = *(reinterpret_cast<const float*> (v_ptr));
        break;

    case BigQueryType::DOUBLE:
        datum->_DOUBLE = *(reinterpret_cast<const double*> (v_ptr));
        break;

    case BigQueryType::STRING:
        *datum->_STRING = value.as_string();
        break;

    case BigQueryType::BYTES:
        *datum->_BYTES =
            BytesStorage(block->GetValue().size(),
                         static_cast<const void *>(block->GetValue().data()));
        break;

    default:
        LOG(FATAL) << "Not supported data type:" << type;
    }
}

} // namespace compiler
} // namespace gunir

