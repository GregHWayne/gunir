// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_IO_TABLET_SCANNER_H
#define  GUNIR_IO_TABLET_SCANNER_H

#include "gunir/io/scanner.h"

namespace gunir {
namespace io {

class TabletReader;

class TabletScanner : public Scanner {
public:
    explicit TabletScanner(TabletReader* tablet_reader);
    ~TabletScanner();
    bool NextSlice(Slice** slice);

private:
    TabletReader* m_tablet_reader;
};

} // namespace io
} // namespace gunir

#endif  // GUNIR_IO_TABLET_SCANNER_H
