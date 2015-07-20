// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef GUNIR_UTILS_IP_ADDRESS_H
#define GUNIR_UTILS_IP_ADDRESS_H

#include <stdint.h>

#include <string>

namespace gunir {

class IpAddress {
public:
    IpAddress();
    IpAddress(const std::string& ip_port);
    IpAddress(const std::string& ip, const std::string& port);
    IpAddress(const std::string& ip, uint16_t port);

    ~IpAddress() {}

    std::string ToString() const;
    std::string GetIp() const;
    uint16_t GetPort() const;
    std::string GetPortString() const;

    bool IsValid() const {
        return m_valid_address;
    }

    bool Assign(const std::string& ip_port);
    bool Assign(const std::string& ip, const std::string& port);
    bool Assign(const std::string& ip, uint16_t port);

private:
    std::string m_ip;
    uint16_t m_port;

    bool m_valid_address;
};

} // namespace gunir

#endif // GUNIR_UTILS_IP_ADDRESS_H
