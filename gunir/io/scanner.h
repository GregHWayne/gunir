// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description: Scanner is used to send slices to excutor one by one. It has
// the following two derived classes:
// 1. TabletScanner: used in leafserver, get slices from TabletReader;
// 2. SliceScanner: used in interserver, get slices from child servers.

#ifndef  GUNIR_IO_SCANNER_H
#define  GUNIR_IO_SCANNER_H

namespace gunir {
namespace io {

class Slice;

class Scanner {
public:
    virtual ~Scanner() {}
    virtual bool NextSlice(Slice** slice) = 0;
};

} // namespace io
} // namespace gunir

#endif  // GUNIR_IO_SCANNER_H
