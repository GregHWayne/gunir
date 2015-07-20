// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_SCAN_READER_H
#define  GUNIR_COMPILER_SCAN_READER_H

#include <vector>

#include "toft/base/scoped_ptr.h"

#include "gunir/compiler/big_query_functions.h"
#include "gunir/compiler/parser/column_info.h"
#include "gunir/compiler/plan.h"
#include "gunir/io/tablet_reader.h"

namespace gunir {
namespace io {
class Block;
}  // namespace io

namespace compiler {

class ScanReader {
public:
    ScanReader() {}

    void Reset(io::Scanner* scanner, size_t column_number);

    bool Read(const std::vector<BQType>& datum_type_list,
              const std::vector<DatumBlock*>& datum_block_list,
              uint32_t* select_level,
              const std::vector<uint64_t>& affect_ids);

    void GetDatumFromBlock(const io::Block* block,
                           BQType type,
                           DatumBlock* datum_block);

private:
    io::Scanner* m_scanner;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_SCAN_READER_H

