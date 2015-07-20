// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/io/tablet_scanner.h"

#include "gunir/io/scanner.h"
#include "gunir/io/tablet_reader.h"

namespace gunir {
namespace io {

TabletScanner::TabletScanner(TabletReader* tablet_reader) {
    m_tablet_reader = tablet_reader;
}

TabletScanner::~TabletScanner() {}

bool TabletScanner::NextSlice(Slice** slice) {
    if (m_tablet_reader->Next()) {
        *slice = m_tablet_reader->GetSlice();
        return true;
    }
    return false;
}

} // namespace io
} // namespace gunir
