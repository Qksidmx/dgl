
#ifndef STARKNOWLEDGEGRAPHDATABASE_BITOUTPUTSTREAM_H
#define STARKNOWLEDGEGRAPHDATABASE_BITOUTPUTSTREAM_H

#include <cstdint>

namespace skg {
class BitOutputStream {
public:
    virtual
    ~BitOutputStream() = default;

    virtual
    Status Write(const uint32_t bit) = 0;
};

class FileBitOutputStream {
public:
    FileBitOutputStream()
            : m_buf(0x0), m_nbufbits(sizeof(m_buf) * 8), m_bufidx(0), m_f(nullptr) {
    }

    ~FileBitOutputStream() {
        this->Close();
    }

    Status Open(const std::string &filename) {
        m_f = fopen(filename.c_str(), "wb");
        if (m_f == nullptr) {
            return Status::IOError("Can NOT open file");
        }
        return Status::OK();
    }

    Status Close() {
        if (m_f != nullptr) {
            this->Flush();
            fclose(m_f);
            m_f = nullptr;
        }
        return Status::OK();
    }

    Status Write(const uint32_t bit) {
        assert((bit & 0xFFFFFFFE) == 0); // check 值为 0/1
        m_buf |= ((bit & 0x01) << m_bufidx);
        m_bufidx++;
        if (m_bufidx == m_nbufbits) {
            return this->Flush();
        }
        return Status::OK();
    }

private:
    uint8_t m_buf;
    const size_t m_nbufbits;
    uint32_t m_bufidx;

    FILE *m_f;

    Status Flush() {
        if (m_bufidx != 0) {
            assert(m_f != nullptr);
            fwrite(&m_buf, sizeof(m_buf), 1, m_f);
            fflush(m_f);
            m_buf = 0x0;
            m_bufidx = 0;
        }
        return Status::OK();
    }
};

class BufferedBitOutputStream {
public:
    BufferedBitOutputStream()
            : m_buf(0x0), m_nbufbits(sizeof(m_buf) * 8), m_bufidx(0), m_bytes() {
    }

    inline
    Status Write(const uint32_t bit) {
        assert((bit & 0xFFFFFFFE) == 0); // check 值为 0/1
        m_buf |= ((bit & 0x01) << m_bufidx);
        m_bufidx++;
        if (m_bufidx == m_nbufbits) {
            return this->Flush();
        }
        return Status::OK();
    }

    /**
     * 提取缓存的byte串, 清空缓存
     * @param bytes
     * @return
     */
    Status ExtractBytesArray(std::vector<uint8_t> *bytes) {
        bytes->clear();
        this->Flush();
        bytes->swap(m_bytes);
        return Status::OK();
    }

    size_t length() const {
        return m_bytes.size() * 8 + m_bufidx;
    }

private:
    uint8_t m_buf;
    const size_t m_nbufbits;
    uint32_t m_bufidx;

    std::vector<uint8_t> m_bytes;

    Status Flush() {
        if (m_bufidx != 0) {
            m_bytes.emplace_back(m_buf);
            m_buf = 0x0;
            m_bufidx = 0;
        }
        return Status::OK();
    }
};
}

#endif //STARKNOWLEDGEGRAPHDATABASE_BITOUTPUTSTREAM_H
