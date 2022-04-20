#ifndef STARKNOWLEDGEGRAPHDATABASE_RESULTEDGE_H
#define STARKNOWLEDGEGRAPHDATABASE_RESULTEDGE_H

#include <string>

#include "util/types.h"

namespace skg {

struct ResultEdge {
public:

    // 0                  ~ 1                  size-of-s_src
    // 1                  ~ 2                  size-of-s_dst
    // 2                  ~ 2+column_bytes_len column_bytes
    // 2+column_bytes_len ~                    s_src,s_dst
    std::string buf;

    // src vertex (long-type)
    vid_t src;
    // dst vertex (long-type)
    vid_t dst;
    size_t column_bytes_len;
    // weight of edge
    EdgeWeight_t weight;
    // type of edge
    EdgeTag_t tag;
#ifdef SKG_PROPERTIES_SUPPORT_NULL
    // 标志属性是否有值
    PropertiesBitset_t m_properties_bitset;
#endif

    const std::string s_src() const;
    const std::string s_dst() const;

    const char *column_bytes() const;

    ResultEdge();

    ResultEdge(vid_t src_, vid_t dst_,
               EdgeWeight_t weight_, EdgeTag_t tag_,
               const char *column_bytes_, const size_t column_bytes_len_,
               const PropertiesBitset_t &bitset);

    ResultEdge(vid_t src_, vid_t dst_,std::string s_src_,std::string s_dst_,
               EdgeWeight_t weight_, EdgeTag_t tag_,
               const char *column_bytes_, const size_t column_bytes_len_,
               const PropertiesBitset_t &bitset);

    void set_vertex(const std::string &s_src_, const std::string &s_dst_);

    bool IsPropertySet(uint32_t i) const {
        return m_properties_bitset.IsPropertySet(i);
    }
private:
    uint8_t s_src_size() const {
        return static_cast<uint8_t>(buf[0]);
    }

    uint8_t s_dst_size() const {
        return static_cast<uint8_t>(buf[1]);
    }
public:
    bool operator<(const ResultEdge &rhs) const;

    ResultEdge(const ResultEdge&rhs);
    ResultEdge& operator=(const ResultEdge&rhs);

    ResultEdge(ResultEdge &&rhs) noexcept;
    ResultEdge& operator=(ResultEdge &&rhs) noexcept;
};
}

#endif //STARKNOWLEDGEGRAPHDATABASE_RESULTEDGE_H
