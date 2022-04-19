#ifndef STARKNOWLEDGEGRAPHDATABASE_BITINPUTSTREAM_H
#define STARKNOWLEDGEGRAPHDATABASE_BITINPUTSTREAM_H

#include <cstdint>

namespace skg {
class BitInputStream {
public:
    virtual
    ~BitInputStream() = default;

    virtual
    int32_t Read() = 0;
};

class FileBitInputStream : public BitInputStream {
public:
    FileBitInputStream() : m_f(nullptr), m_buf(0), bufi(7) {
    }

    ~FileBitInputStream() {
        this->Close();
    }

    Status Open(const std::string &filename) {
        m_f = fopen(filename.c_str(), "rb");
        if (m_f == nullptr) {
            return Status::IOError("Can NOT open file: {}");
        }
        return Status::OK();
    }

    Status Close() {
        if (m_f != nullptr) {
            fclose(m_f);
            m_f = nullptr;
        }
        return Status::OK();
    }

    /**
     * 从缓冲区中读出一个bit, 设置 *bit 的最后一位
     * @param bit
     * @return
     */
    int32_t Read() override {
        int32_t bit = 0; // 初始化为0x0
        if (bufi == 7) {
            const size_t nRead = fread(&m_buf, sizeof(uint8_t), 1, m_f);
            if (nRead != 1) {
                return -1;
            }
            bufi = 0;
            bit = ((m_buf >> bufi) & 1);
        } else {
            bufi++;
            bit = ((m_buf >> bufi) & 1);
        }
        return bit;
    }

private:

    FILE *m_f;
    uint8_t m_buf;
    uint32_t bufi;
};

class BufferedBitInputStream {
public:
    BufferedBitInputStream(const std::vector<uint8_t> &bytes)
            : m_bytes(bytes), m_byte_idx(0), m_bit_idx(0) {
    }

    int32_t Read() {
        int32_t bit = 0; // 初始化为0x0
        if (m_byte_idx >= m_bytes.size()) {
            return -1;
        }
        if (m_bit_idx == 7) {
            bit = ((m_bytes[m_byte_idx] >> m_bit_idx) & 1);
            m_bit_idx = 0;
            m_byte_idx++;
        } else {
            bit = ((m_bytes[m_byte_idx] >> m_bit_idx) & 1);
            m_bit_idx++;
        }
        return bit;
    }

    Status Seek(size_t pos) {
        if (pos > m_bytes.size() * 8) {
            return Status::IOError("seek overflow");
        }
        m_byte_idx = pos / 8;
        m_bit_idx = pos % 8;
        return Status::OK();
    }

private:
    std::vector<uint8_t> m_bytes;
    size_t m_byte_idx;
    uint32_t m_bit_idx;
};

class BufferedBitInputStreamView {
public:
    BufferedBitInputStreamView(const std::vector<uint8_t> &bytes)
            : m_bytes(bytes), m_byte_idx(0), m_bit_idx(0) {
        if (!m_bytes.empty()) {
            m_current_byte = m_bytes[0];
        } else {
            m_current_byte = 0;
        }
    }

    inline
    int32_t Read() {
        int32_t bit = 0; // 初始化为0x0
        if (m_byte_idx >= m_bytes.size()) {
            return -1;
        }
        if (m_bit_idx == 7) {
            bit = ((m_current_byte >> m_bit_idx) & 1);
            m_bit_idx = 0;
            m_byte_idx++;
            m_current_byte = m_bytes[m_byte_idx];
        } else {
            bit = ((m_current_byte >> m_bit_idx) & 1);
            m_bit_idx++;
        }
        return bit;
    }

    inline
    Status Seek(size_t pos) {
        if (pos > m_bytes.size() * 8) {
            return Status::IOError("seek overflow");
        }
        m_byte_idx = pos / 8;
        m_bit_idx = pos % 8;
        m_current_byte = m_bytes[m_byte_idx];
        return Status::OK();
    }

private:
    const std::vector<uint8_t> &m_bytes;
    uint8_t m_current_byte;
    size_t m_byte_idx;
    uint32_t m_bit_idx;
};
}

#endif //STARKNOWLEDGEGRAPHDATABASE_BITINPUTSTREAM_H
