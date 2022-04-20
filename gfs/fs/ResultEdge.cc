#include <ctime>
#include <utility>

#include "ResultEdge.h"

#include "util/types.h"
#include "fmt/format.h"
#include "fmt/time.h"

namespace skg {
// ====== ID类型的边查询结果 ====== //
ResultEdge::ResultEdge()
        : ResultEdge(0, 0, 0.0f, 0, nullptr, 0, PropertiesBitset_t()) {
}

ResultEdge::ResultEdge(vid_t src_, vid_t dst_,
                       EdgeWeight_t weight_, EdgeTag_t tag_,
                       const char *column_bytes_, const size_t column_bytes_len_,
                       const PropertiesBitset_t &bitset)
        : ResultEdge(src_, dst_, "", "", weight_, tag_, column_bytes_, column_bytes_len_, bitset) {
}

ResultEdge::ResultEdge(
        vid_t src_, vid_t dst_, std::string s_src_,std::string s_dst_,
        EdgeWeight_t weight_, EdgeTag_t tag_,
        const char *column_bytes_, const size_t column_bytes_len_,
        const PropertiesBitset_t &bitset)
        : buf(2UL + column_bytes_len_ + s_src_.size() + s_dst_.size(), '\x00'),
          src(src_), dst(dst_),
          column_bytes_len(column_bytes_len_),
          weight(weight_), tag(tag_), m_properties_bitset(bitset) {
    assert(column_bytes_ != nullptr);
    assert(column_bytes_len_ < SKG_MAX_EDGE_PROPERTIES_BYTES);
    char *bufPtr = const_cast<char *>(buf.data());
    uint8_t len = s_src_.size() & 0xff;
    memcpy(bufPtr, &len, 1); bufPtr += 1;
    len = s_dst_.size() & 0xff;
    memcpy(bufPtr, &len, 1); bufPtr += 1;
    memcpy(bufPtr, column_bytes_, column_bytes_len_); bufPtr += column_bytes_len_;
    memcpy(bufPtr, s_src_.data(), s_src_.size()); bufPtr += s_src_.size();
    memcpy(bufPtr, s_dst_.data(), s_dst_.size()); bufPtr += s_dst_.size();

    m_properties_bitset = bitset;
}

bool ResultEdge::operator<(const ResultEdge &rhs) const {
    if (tag == rhs.tag) {
        if (src == rhs.src) {
            return dst < rhs.dst;
        } else {
            return src < rhs.src;
        }
    } else {
        return tag < rhs.tag;
    }
}

ResultEdge &ResultEdge::operator=(ResultEdge &&rhs) noexcept {
    if (this != &rhs) {
        buf.swap(rhs.buf);
        src = rhs.src;
        dst = rhs.dst;
        weight = rhs.weight;
        tag = rhs.tag;
        column_bytes_len = rhs.column_bytes_len;
        m_properties_bitset = rhs.m_properties_bitset;
    }
    return *this;
}

ResultEdge::ResultEdge(ResultEdge &&rhs) noexcept {
    *this = std::move(rhs);
}

ResultEdge::ResultEdge(const ResultEdge &rhs)
        : buf(rhs.buf), src(rhs.src), dst(rhs.dst),
          column_bytes_len(rhs.column_bytes_len),
          weight(rhs.weight), tag(rhs.tag),
          m_properties_bitset(rhs.m_properties_bitset)
{
}

ResultEdge &ResultEdge::operator=(const ResultEdge &rhs) {
    if (this != &rhs) {
        src = rhs.src;
        dst = rhs.dst;
        weight = rhs.weight;
        tag = rhs.tag;
        column_bytes_len = rhs.column_bytes_len;
        buf = rhs.buf;
        m_properties_bitset = rhs.m_properties_bitset;
    }
    return *this;
}

const std::string ResultEdge::s_src() const {
    return std::string(buf, 2+column_bytes_len, s_src_size());
}

const std::string ResultEdge::s_dst() const {
    return std::string(buf, 2+column_bytes_len+s_src_size(), s_dst_size());
}

const char *ResultEdge::column_bytes() const {
    return buf.data() + 2;
}

void ResultEdge::set_vertex(const std::string &s_src_, const std::string &s_dst_) {
    assert(s_src_size() == 0);
    assert(s_src_.size() <= 0xff);
    assert(s_dst_size() == 0);
    assert(s_dst_.size() <= 0xff);
    buf.resize(2 + column_bytes_len + s_src_.size() + s_dst_.size());
    buf[0] = s_src_.size() & 0xff;
    buf[1] = s_dst_.size() & 0xff;
    char *bufPtr = const_cast<char *>(buf.data()) + 2 + column_bytes_len;
    memcpy(bufPtr, s_src_.data(), s_src_.size()); bufPtr += s_src_.size();
    memcpy(bufPtr, s_dst_.data(), s_dst_.size()); bufPtr += s_dst_.size();
}

}
