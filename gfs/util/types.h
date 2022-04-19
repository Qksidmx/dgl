#ifndef STARKNOWLEDGEGRAPH_SKG_TYPES_HPP
#define STARKNOWLEDGEGRAPH_SKG_TYPES_HPP


#include <cstddef>
#include <cstdint>
#include <cassert>
#include <cstring>
#include <vector>
#include <string>
#include <climits>

namespace skg {

    // 节点id类型, 暂时为 uint32_t
    typedef uint32_t vid_t;
    static const vid_t INVALID_VID = ((vid_t)-1);
    //typedef uint64_t vid_t;

    typedef std::vector<uint8_t> Bytes;

    enum class ColumnType: unsigned char {
        NONE = 0,

        TAG = 1,
        WEIGHT = 2,

        INT = 3, INT32 = 3,
        LONG= 8, INT64 = 8,
        FLOAT = 4, FLOAT32 = 4,
        DOUBLE = 7, FLOAT64 = 7,
        FIXED_BYTES = 5,
        TIME = 9,
        VARCHAR =10,
        // 可以把多列合并为 column group. 列存储的优化策略之一.
        // 一些经常会存放在一起的数据(如经度 + 纬度), 通过 column group 增强数据缓存命中的局部性.
        // 参照 Bigtable: A Distributed Storage System for Structured Data. 6 Refinements - Locality groups
        GROUP = 255,
    };

    // 节点/边的类型标识
    typedef uint8_t EdgeTag_t;
    static const EdgeTag_t INVALID_TAG = ((EdgeTag_t)-1);
    static const int64_t MAX_TIMESTAMP = LLONG_MAX;

    // 边的权重类型
    typedef float EdgeWeight_t;

    // 每一类边的属性最多能有多少列
    static const uint32_t SKG_MAX_EDGE_PROPERTIES_SIZE = 64;
    // 每一类边的所有属性列最多能存多少字节
    static const uint32_t SKG_MAX_EDGE_PROPERTIES_BYTES = 1024;

    // 边的label
    struct EdgeLabel {
        std::string edge_label;
        std::string src_label;
        std::string dst_label;

        EdgeLabel(const std::string &edge, const std::string &src, const std::string &dst)
                : edge_label(edge), src_label(src), dst_label(dst) {
        }

        EdgeLabel() : EdgeLabel("", "", "") { }

        bool operator==(const EdgeLabel &rhs) const {
            return (edge_label == rhs.edge_label) && (src_label == rhs.src_label) && (dst_label == rhs.dst_label);
        }
        bool operator!=(const EdgeLabel &rhs) const {
            return !(*this == rhs);
        }
        EdgeLabel& operator=(const EdgeLabel &rhs) {
            edge_label = rhs.edge_label;
            src_label = rhs.src_label;
            dst_label = rhs.dst_label;
            return *this;
        }
        std::string ToString() const;
    };
    // 节点的label
    typedef std::string VertexLabel;

    struct PropertiesBitset_t {
        uint8_t m_bitset[SKG_MAX_EDGE_PROPERTIES_SIZE / (sizeof(uint8_t) * 8)];

        PropertiesBitset_t() {
            Clear();
        }

        void Clear() {
            memset(m_bitset, 0, SKG_MAX_EDGE_PROPERTIES_SIZE / (sizeof(uint8_t) * 8));
        }

        inline
        bool IsPropertySet(uint32_t i) const {
#ifdef SKG_PROPERTIES_SUPPORT_NULL
            assert(i < SKG_MAX_EDGE_PROPERTIES_SIZE);
            const uint32_t byte = 0x01u << (i % 8);
            return (m_bitset[i / 8] & byte) != 0;
#else
            return true;
#endif
        }

        inline
        void SetProperty(uint32_t i) {
#ifdef SKG_PROPERTIES_SUPPORT_NULL
            assert(i < SKG_MAX_EDGE_PROPERTIES_SIZE);
            const uint32_t byte = 0x01u << (i % 8);
            m_bitset[i / 8] |= (byte & 0xff);
#endif
        }

        inline
        void ClearProperty(uint32_t i) {
#ifdef SKG_PROPERTIES_SUPPORT_NULL
            assert(i < SKG_MAX_EDGE_PROPERTIES_SIZE);
            const uint32_t byte = 0x01u << (i % 8);
            m_bitset[i / 8] &= (~(byte & 0xff));
#endif
        }

    public:
        PropertiesBitset_t& operator=(const PropertiesBitset_t &rhs) {
            if (this != &rhs) {
                memcpy(m_bitset, rhs.m_bitset, sizeof(m_bitset));
            }
            return *this;
        }

        PropertiesBitset_t(const PropertiesBitset_t &rhs) {
            memcpy(m_bitset, rhs.m_bitset, sizeof(m_bitset));
        }
    };

}


#endif  // STARKNOWLEDGEGRAPH_SKG_TYPES_HPP



