// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "toft/system/threading/this_thread.h"

#include "gunir/gunir_entry.h"

namespace gunir {

GunirEntry::GunirEntry()
    : m_started(false) {}

GunirEntry::~GunirEntry() {}

bool GunirEntry::Start() {
    if (ShouldStart()) {
        return StartServer();
    }
    return false;
}

bool GunirEntry::Run() {
    toft::ThisThread::Sleep(2000);
    return true;
}

bool GunirEntry::Shutdown() {
    if (ShouldShutdown()) {
        ShutdownServer();
        return true;
    }
    return false;
}

bool GunirEntry::ShouldStart() {
    toft::Mutex::Locker lock(&m_mutex);
    if (m_started) {
        return false;
    }
    m_started = true;
    return true;
}

bool GunirEntry::ShouldShutdown() {
    toft::Mutex::Locker lock(&m_mutex);
    if (!m_started) {
        return false;
    }
    m_started = false;
    return true;
}

}  // namespace gunir
