#ifndef STARKNOWLEDGEGRAPHDATABASE_ELIASGAMMASEQ_H
#define STARKNOWLEDGEGRAPHDATABASE_ELIASGAMMASEQ_H

#include <cstdint>
#include <vector>

#include "status.h"
#include "types.h"

#include "BitOutputStream.h"
#include "BitInputStream.h"

namespace skg {

class EliasGammaSeq {
public:

    explicit
    EliasGammaSeq(size_t interval = 128)
            : m_length(0), m_index_interval(interval) {
    }

    Status EncodeSeq(const std::vector<vid_t> &originSeq);

    /**
     * 获取第 index 的值
     */
    vid_t Get(size_t index) const;

    /**
     * 获取第 index && index + 1 的值
     */
    std::pair<vid_t, vid_t> GetTwo(size_t index) const;

    /**
     * 查找 value 值所在的index
     */
    uint32_t GetIndex(vid_t value) const;

    size_t GetBytesSize() const {
        return m_bytes.size()
               + m_index_bit_idx.size() * sizeof(size_t)
               + m_index_values.size() * sizeof(vid_t);
    }

    double CompressionRatio() const {
        // https://en.wikipedia.org/wiki/Data_compression_ratio
        // Data compression ratio is defined as the ratio between the uncompressed size and compressed size
        return (1.0 * length() * sizeof(vid_t)) / GetBytesSize();
    }

    size_t length() const {
        return m_length;
    }

    size_t index_interval() const {
        return m_index_interval;
    }

private:
    size_t m_length;
    std::vector<uint8_t> m_bytes;
    std::vector<size_t> m_index_bit_idx;
    std::vector<vid_t> m_index_values;
    size_t m_index_interval;

    static
    Status Encode(uint32_t x, BufferedBitOutputStream &bos);

    static
    int32_t Decode(BufferedBitInputStreamView &bis);

    friend class EliasGammaSeqSerialization;

    /*
#ifdef ELIAS_GAMMA_DEBUG
public:
    static
    Status DebugEncode(uint32_t x, std::string *os) {
        os->clear(); // 清空原来的值
        Status s;
        if (x == 0) {
            for (size_t i = 0; i < 32; ++i) {
                os->append(1, '0');
            }
            return s;
        }
        size_t lg = static_cast<size_t>(log2(x));
        for (size_t i = 0; i < lg; ++i) {
            os->append(1, '0');
            if (!s.ok()) { return s; }
        }
#if 0
        // 需要翻转输出
        std::string buf;
        while (x) {
            buf.append(1, '0' + (x & 0x01));
            x >>= 1;
        }
        os->append(buf.rbegin(), buf.rend());
#endif
        // 从最高位开始
        os->append(1, '1');
        for (int i = lg - 1; i >= 0; i--) {
            os->append(1, '0' + ((x & (1 << i)) != 0));
        }
        return s;
    }

    static
    Status DebugDecode(uint32_t *value, const std::string &is) {
        if (is.size() == 1 && is[0] == '0') {
            return Status::InvalidArgument(is);
        }
        int i = 0;
        while (is[i] == '0') { ++i; }
        if (i == 0) {
            *value = 1;
            return Status::OK();
        } else {
            int t = i;
            uint32_t xx = 0;
            while (is[i]) {
                if (i >= t * 2 + 1 && is[i] != '\0') {
                    return Status::InvalidArgument(is);
                }
                xx <<= 1;
                xx |= (is[i] - '0');
                i++;
            }
            *value = xx;
            return Status::OK();
        }
    }
#endif
*/
};
}

#endif //STARKNOWLEDGEGRAPHDATABASE_ELIASGAMMASEQ_H
