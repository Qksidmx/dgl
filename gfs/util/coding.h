//  Rewrite by Jiefeng on 21/07/20

#pragma once

#include <cstdint>
#include "slice.h"
#include "internal_types.h"

namespace skg {
// The maximum length of a varint in bytes for 64-bit.
const unsigned int kMaxVarint64Length = 10;

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
extern char* EncodeVarint32(char* dst, uint32_t value);
extern char* EncodeVarint64(char* dst, uint64_t value);

inline uint32_t DecodeFixed32(const char *ptr) {
    if (kLittleEndian) {
        // Load the raw bytes
        uint32_t result;
        memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
        return result;
    } else {
        return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
                | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
                | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
                | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
    }
}

inline uint64_t DecodeFixed64(const char *ptr) {
    if (kLittleEndian) {
        // Load the raw bytes
        uint64_t result;
        memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
        return result;
    } else {
        uint64_t lo = DecodeFixed32(ptr);
        uint64_t hi = DecodeFixed32(ptr + 4);
        return (hi << 32) | lo;
    }
}

// -- Implementation of the functions declared above
inline void EncodeFixed32(char* buf, uint32_t value) {
    if (kLittleEndian) {
        memcpy(buf, &value, sizeof(value));
    } else {
        buf[0] = value & 0xff;
        buf[1] = (value >> 8) & 0xff;
        buf[2] = (value >> 16) & 0xff;
        buf[3] = (value >> 24) & 0xff;
    }
}

inline void EncodeFixed64(char* buf, uint64_t value) {
    if (kLittleEndian) {
        memcpy(buf, &value, sizeof(value));
    } else {
        buf[0] = value & 0xff;
        buf[1] = (value >> 8) & 0xff;
        buf[2] = (value >> 16) & 0xff;
        buf[3] = (value >> 24) & 0xff;
        buf[4] = (value >> 32) & 0xff;
        buf[5] = (value >> 40) & 0xff;
        buf[6] = (value >> 48) & 0xff;
        buf[7] = (value >> 56) & 0xff;
    }
}

inline char *EncodeVarint64(char *dst, uint64_t v) {
    static const unsigned int B = 128;
    unsigned char *ptr = reinterpret_cast<unsigned char *>(dst);
    while (v >= B) {
        *(ptr++) = (v & (B - 1)) | B;
        v >>= 7;
    }
    *(ptr++) = static_cast<unsigned char>(v);
    return reinterpret_cast<char *>(ptr);
}

inline const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                   uint32_t* value) {
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
        uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return reinterpret_cast<const char*>(p);
        }
    }
    return nullptr;
}

inline const char* GetVarint32Ptr(const char* p,
                                  const char* limit,
                                  uint32_t* value) {
    if (p < limit) {
        uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
        if ((result & 128) == 0) {
            *value = result;
            return p + 1;
        }
    }
    return GetVarint32PtrFallback(p, limit, value);
}

inline const char *GetVarint64Ptr(const char *p, const char *limit, uint64_t *value) {
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint64_t byte = *(reinterpret_cast<const unsigned char *>(p));
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return reinterpret_cast<const char *>(p);
        }
    }
    return nullptr;
}

inline bool GetVarint64(Slice *input, uint64_t *value) {
    const char *p = input->data();
    const char *limit = p + input->size();
    const char *q = GetVarint64Ptr(p, limit, value);
    if (q == nullptr) {
        return false;
    } else {
        *input = Slice(q, static_cast<size_t>(limit - q));
        return true;
    }
}

// Pull the last 8 bits and cast it to a character
inline void PutFixed32(std::string* dst, uint32_t value) {
    if (kLittleEndian) {
        dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
                    sizeof(value));
    } else {
        char buf[sizeof(value)];
        EncodeFixed32(buf, value);
        dst->append(buf, sizeof(buf));
    }
}

inline bool GetFixed32(Slice* input, uint32_t* value) {
    if (input->size() < sizeof(uint32_t)) {
        return false;
    }
    *value = DecodeFixed32(input->data());
    input->remove_prefix(sizeof(uint32_t));
    return true;
}

inline void PutFixed64(std::string* dst, uint64_t value) {
    if (kLittleEndian) {
        dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
                    sizeof(value));
    } else {
        char buf[sizeof(value)];
        EncodeFixed64(buf, value);
        dst->append(buf, sizeof(buf));
    }
}

inline bool GetFixed64(Slice* input, uint64_t* value) {
    if (input->size() < sizeof(uint64_t)) {
        return false;
    }
    *value = DecodeFixed64(input->data());
    input->remove_prefix(sizeof(uint64_t));
    return true;
}

inline void PutVarint32(std::string* dst, uint32_t v) {
    char buf[5];
    char* ptr = EncodeVarint32(buf, v);
    dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline bool GetVarint32(Slice* input, uint32_t* value) {
    const char* p = input->data();
    const char* limit = p + input->size();
    const char* q = GetVarint32Ptr(p, limit, value);
    if (q == nullptr) {
        return false;
    } else {
        *input = Slice(q, static_cast<size_t>(limit - q));
        return true;
    }
}

inline void PutVarint64(std::string* dst, uint64_t v) {
    char buf[10];
    char* ptr = EncodeVarint64(buf, v);
    dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint64Varint64(std::string* dst, uint64_t v1, uint64_t v2) {
    char buf[20];
    char* ptr = EncodeVarint64(buf, v1);
    ptr = EncodeVarint64(ptr, v2);
    dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint64Varint64Varint64(std::string* dst, uint64_t v1, uint64_t v2, uint64_t v3) {
    char buf[30];
    char* ptr = EncodeVarint64(buf, v1);
    ptr = EncodeVarint64(ptr, v2);
    ptr = EncodeVarint64(ptr, v3);
    dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
    PutVarint32(dst, static_cast<uint32_t>(value.size()));
    dst->append(value.data(), value.size());
}

inline bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
    uint32_t len = 0;
    if (GetVarint32(input, &len) && input->size() >= len) {
        *result = Slice(input->data(), len);
        input->remove_prefix(len);
        return true;
    } else {
        return false;
    }
}

}
