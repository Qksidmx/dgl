#include "EliasGammaSeq.h"

#include "types.h"
#include "internal_types.h"

namespace skg {

Status EliasGammaSeq::EncodeSeq(const std::vector<vid_t> &originSeq) {
    Status s;
    uint32_t prev = 0;
    BufferedBitOutputStream bits;
    size_t last_index_entry = 0;
    size_t index_size = static_cast<size_t>(ceil(1.0 * originSeq.size() / m_index_interval));
    size_t index_idx = 0;

    std::vector<size_t> index_bit_idx(index_size);
    std::vector<vid_t> index_values(index_size);

    for (size_t i = 0; i < originSeq.size(); ++i) {
        vid_t x = originSeq[i];

        uint32_t delta = ((i == 0) ? (x + 1) : (x - prev));
        assert(delta > 0);
        s = Encode(delta, bits);
        if (!s.ok()) { return s; }
        if (i == 0 || (i - last_index_entry >= m_index_interval && index_idx < index_size)) {
            index_values[index_idx] = originSeq[i];
            index_bit_idx[index_idx] = bits.length();
            index_idx += 1;
            last_index_entry = i;
        }

        prev = x;
    }

    while (index_idx < index_size) {
        index_values[index_idx] = originSeq[originSeq.size() - 1];
        index_bit_idx[index_idx] = bits.length();
        index_idx += 1;
    }

    m_length = originSeq.size();
    bits.ExtractBytesArray(&m_bytes);
    m_index_bit_idx.swap(index_bit_idx);
    m_index_values.swap(index_values);
    return s;
}

uint32_t EliasGammaSeq::Get(size_t index) const {
    Status s;
    size_t index_idx = (index / m_index_interval);
    size_t cur_idx = index_idx * m_index_interval;
    size_t bit_idx = m_index_bit_idx[index_idx];
    uint32_t value = m_index_values[index_idx];

    BufferedBitInputStreamView bis(m_bytes);
    s = bis.Seek(bit_idx); // 偏移
    while (cur_idx < index) {
        int32_t delta = EliasGammaSeq::Decode(bis);
        if (delta < 0) { return -1; }
        value += delta;
        ++cur_idx;
    }

    return value;
}

std::pair<uint32_t, uint32_t> EliasGammaSeq::GetTwo(size_t index) const {
    Status s;
    size_t index_idx = (index / m_index_interval);
    size_t cur_idx = index_idx * m_index_interval;
    size_t bit_idx = m_index_bit_idx[index_idx];
    uint32_t value = m_index_values[index_idx];

    std::pair<uint32_t, uint32_t> result;
    BufferedBitInputStreamView bis(m_bytes);
    s = bis.Seek(bit_idx); // 偏移
    while (cur_idx < index + 1) {
        if (cur_idx == index) {
            result.first = value;
        }
        int32_t delta = EliasGammaSeq::Decode(bis);
        if (delta < 0) {
            result.second = -1;
            return result;
        }
        value += delta;
        ++cur_idx;
    }

    result.second = value;
    return result;
}

uint32_t EliasGammaSeq::GetIndex(uint32_t value) const {
    Status s;
//    fmt::print("finding index of: {}\n", value);
    auto index_idx_iter = std::upper_bound(m_index_values.begin(), m_index_values.end(), value);
    if (index_idx_iter == m_index_values.begin()) {
        // everything is larger than value
        return -1;
    }
    size_t index_idx = static_cast<size_t>(index_idx_iter - 1 - m_index_values.begin());
//    fmt::print("index_idx: {}\n", index_idx);
    size_t cur_idx = index_idx * m_index_interval;
    size_t bit_idx = m_index_bit_idx[index_idx];
    uint32_t cumlant = m_index_values[index_idx];
//    fmt::print("first value: {}\n", cumlant);

    BufferedBitInputStreamView bis(m_bytes);
    s = bis.Seek(bit_idx); // 偏移
    if (!s.ok()) { return -1; }
    while (cumlant < value) {
        int32_t delta = EliasGammaSeq::Decode(bis);
        if (delta < 0) { return -1; }
        cumlant += delta;
//        fmt::print("delta: {}, cumulant: {}\n", delta, cumlant);
        ++cur_idx;
    }
    if (cumlant > value) {
        return -1;
    } else {
        return cur_idx;
    }
}

Status EliasGammaSeq::Encode(uint32_t x, BufferedBitOutputStream &bos) {
    Status s;
    if (x == 0) {
        for (size_t i = 0; i < 32; ++i) {
            s = bos.Write(0);
            if (!s.ok()) { return s; }
        }
        return s;
    }
    size_t lg = static_cast<size_t>(log2(x));
    for (size_t i = 0; i < lg; ++i) {
        s = bos.Write(0);
        if (!s.ok()) { return s; }
    }
    bos.Write(1);
    for (int i = lg - 1; i >= 0; i--) {
        bos.Write((x & (1 << i)) != 0);
    }
    return s;
}

int32_t EliasGammaSeq::Decode(BufferedBitInputStreamView &bis) {
    int numZeros = 0;
    int32_t bit;
    do {
        bit = bis.Read();
        if (bit == -1) { return -1; }
        numZeros++;
        // value == 0的情况
        if (numZeros == 32) { return 0; }
    } while (bit == 0);
    --numZeros; // 最后一个bit为非0

    // 连续0后, 后续的numZeros+1个bit数据, 为原来数值的二进制表示
    // 跳出上面do-while循环时, 已经读入第一个非0的bit, 再读入numZeros个bit
    uint32_t value = bit;
    for (int32_t i = 0; i < numZeros; ++i) {
        bit = bis.Read();
        if (bit == -1) { return -1; }
        value = (value << 1) | bit;
    }
    return value;
}

}
