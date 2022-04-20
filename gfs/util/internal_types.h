#ifndef STARKNOWLEDGEGRAPH_SKG_INTERNAL_TYPES_HPP
#define STARKNOWLEDGEGRAPH_SKG_INTERNAL_TYPES_HPP

#include <cstdint>


#include "types.h"
#include "status.h"
#include "fmt/format.h"
#include "skglogger.h"
#include <sstream>
#include "rapidjson/document.h"

#if defined(__GNUC__) || defined(__clang__)
#define DEPRECATED __attribute__((deprecated))
#elif defined(_MSC_VER)
#define DEPRECATED __declspec(deprecated)
#else
#pragma message("WARNING: You need to implement DEPRECATED for this compiler")
#define DEPRECATED
#endif

#ifdef __GNUC__
#define VARIABLE_IS_NOT_USED __attribute__ ((unused))
#else
#define VARIABLE_IS_NOT_USED
#endif

#if defined(__GNUC__) && __GNUC__ >= 4
    #define LIKELY(x)   (__builtin_expect((x), 1))
    #define UNLIKELY(x) (__builtin_expect((x), 0))
#else
    #define LIKELY(x)   (x)
    #define UNLIKELY(x) (x)
#endif

namespace skg {

    // 节点属性数据存储, 每次容量满后, 增长的大小
    const static size_t CAPACITY_EXTEND_BUCKET = 50000;

    inline vid_t GetNextStorageCapacity(size_t max_vertex_id) {
        return static_cast<vid_t>(
                ((max_vertex_id / CAPACITY_EXTEND_BUCKET) + 1) * CAPACITY_EXTEND_BUCKET
        );
    }

#define MAX_PATH_LEN 256

    // 索引存储shard中第n条边（非文件偏移量）,uint32_t足够
    // 最高位(1 << 31)用于标识边是否被删除
    // ((1<<30)-1)用于代表索引不存在
    // 因此每个shard中最多边数为 (1<<30)-2 约10亿条
    typedef uint32_t idx_t;
    static const idx_t INDEX_NOT_EXIST = (static_cast<idx_t>(-1) >> 1);
    static const idx_t INDEX_MASK = (static_cast<idx_t>(-1) >> 1);  // 除去最高位
    static const idx_t DELETE_MASK = (1u << (8 * sizeof(idx_t) - 1)); // 最高位

    // 图的最大节点数目
    static const vid_t MAX_VERTICES_ID = 4000000000UL;
    // bulkload过程, mapper与worker之间数据传输, 每个包的记录数
    static const uint32_t MAX_RECORD_ONE_PACKAGE = 500;
    //IDENCode 过程中，一个包最大发送的请求条数
    static const uint32_t MAX_RECORD_ONE_IDENCODE_PACKAGE = 100;
    // bulkload过程, 每行最大长度
    static const size_t SKG_MAX_FILELINE_BUFF_SIZE = 4096;
    // 网络IO buff size
    static const size_t SKG_IOBUFF_SIZE = 40000;
    VARIABLE_IS_NOT_USED static const char *SKG_MSG_OK = "OK";
    VARIABLE_IS_NOT_USED static const size_t SKG_MSG_OK_LEN = strlen(SKG_MSG_OK);
    VARIABLE_IS_NOT_USED static const char *SKG_MSG_START = "START";
    VARIABLE_IS_NOT_USED static const size_t SKG_MSG_START_LEN = strlen(SKG_MSG_START);
    const static uint32_t MIN_SHARD_ID = 1;
    // 常量 byte 转 mb
    static const float MB_BYTES = 1.0f * 1024 * 1024;
    // 常量 us(microseconds) 转 ms(millisecond)
    static const uint32_t MICRO_PER_MS = 1000UL;
    // 常量 us(microseconds) 转 s(second)
    static const uint32_t MICRO_PER_SECOND = 1000UL * MICRO_PER_MS;
    // 节点全局属性 label
    VARIABLE_IS_NOT_USED static const char *SKG_GLOBAL_LABEL = "_global";
    VARIABLE_IS_NOT_USED static const EdgeTag_t SKG_GLOBAL_LABEL_TAG = 0;
    VARIABLE_IS_NOT_USED const static char* SKG_LABEL = "label";
    VARIABLE_IS_NOT_USED static const char *SKG_VERTEX_COLUMN_NAME_TAG = "vtag";
    VARIABLE_IS_NOT_USED static const char *SKG_VERTEX_COLUMN_NAME_DEGREE = "degree";
    VARIABLE_IS_NOT_USED static const char *SKG_EMPTY_COLUMN= "SKG_EMPTY_COLUMN";
    VARIABLE_IS_NOT_USED static const char *SKG_VERTEX_COLUMN_NAME_BITSET = "bitset";

    // 边的属性 BLOCK 大小默认为1MB
    static const size_t BASIC_BLOCK_SIZE = 1024 * 1024;

    struct interval_t {
        bool stopper() const {
            return first == 0 && second == 0;
        }
        static
        const interval_t &get_stopper() {
            static interval_t stopper(0, 0);
            return stopper;
        }
    public:
        vid_t first;
        vid_t second;
        interval_t(const vid_t first_, const vid_t last_): first(first_), second(last_) {
            if (UNLIKELY(first > second)) {
                SKG_LOG_ERROR("trying to create interval {0}--{1}, illegal {0} > {1}\n", first, second);
                assert(first <= second);
            }
        }

        interval_t(const std::pair<vid_t, vid_t> &p): interval_t(p.first, p.second) {
        }
        interval_t(): interval_t(0, 0) {
        }

        inline
        size_t GetNumVertices() const {
            return second + 1 - first;
        }

        inline 
        size_t get_vnum() const {
            return second - first + 1;
        }

        inline 
        size_t get_span() const {
            return second - first;
        }

        inline
        bool Contain(const vid_t vid) const {
            return (first <= vid && vid <= second);
        }

        std::string to_str() {
            std::stringstream ss;
            ss << first << "," << second;
            return ss.str();
        }

        inline
        vid_t GetIndex(const vid_t vid) const {
            assert(vid >= first);
            assert(vid <= second);
            return vid - first;
        }

        inline
        void ExtendTo(const vid_t vid) {
            assert(vid >= first);
            second = std::max(vid, second);
        }

        template <typename Writer>
        void Serialize(Writer &writer) const {
            writer.StartArray();
            writer.Uint64(first);
            writer.Uint64(second);
            writer.EndArray();
        }

        Status ParseFromRapidJsonValue(const rapidjson::Value &jVal) {
            if (!jVal.IsArray() || jVal.Size() != 2 || !jVal[0].IsUint64() || !jVal[1].IsUint64()) {
                return Status::InvalidArgument("invalid json interval");
            }
            first = static_cast<vid_t>(jVal[0].GetUint64());
            second = static_cast<vid_t>(jVal[1].GetUint64());
            return Status::OK();
        }

        inline
        void write(fmt::Writer &w) const {
            w.write("{}~{}", first, second);
        }

        inline
        bool operator==(const interval_t &rhs) const {
            return this->first == rhs.first && this->second == rhs.second;
        }

        inline bool operator!=(const interval_t &rhs) const {
            return !(*this == rhs);
        }

        inline bool operator<(const interval_t &rhs) const {
            return first < rhs.first;
        }

        inline interval_t& operator=(const interval_t &rhs) = default;
    };

    // ==== helper functions of interval_t ====//

    void format_arg(fmt::BasicFormatter<char> &f,
                    const char *&format_str,
                    const interval_t &i);

    // ==== helper functions of interval_t ====//

    // 前置声明
    class MemoryEdge;

#pragma pack(push)
#pragma pack(1)  // 设定为1字节对齐
    struct PersistentEdge {
        vid_t src;
        vid_t dst;
        EdgeWeight_t weight;
        EdgeTag_t tag;
    private:
        idx_t m_next;  // 最高位 (1 << sizeof(idx_t)) 用于标志是否被删除的边
#ifdef SKG_PROPERTIES_SUPPORT_NULL
        // 标志属性是否有值
        PropertiesBitset_t m_properties_bitset;

        friend class MemoryEdge;
#endif
    public:
        explicit PersistentEdge(vid_t _src, vid_t _dst, EdgeWeight_t _weight, EdgeTag_t _tag, idx_t _next=INDEX_NOT_EXIST)
                : src(_src), dst(_dst), weight(_weight), tag(_tag), m_next(_next),
                  m_properties_bitset() {
        }

        idx_t next() const {
            return m_next & INDEX_MASK;
        }

        bool deleted() const {
            return (m_next & DELETE_MASK) != 0;
        }

        void SetDelete() {
            m_next |= DELETE_MASK;
        }

        void CopyFrom(const MemoryEdge &edge);

        /**** 判断属性是否有设值的 API ****/

        inline
        bool IsPropertySet(uint32_t i) const {
            return m_properties_bitset.IsPropertySet(i);
        }

        inline
        void SetProperty(uint32_t i) {
            return m_properties_bitset.SetProperty(i);
        }

        inline
        void ClearProperty(uint32_t i) {
            return m_properties_bitset.ClearProperty(i);
        }
    };
#pragma pack(pop)


    struct MemoryEdge {
    public:
        vid_t src;
        vid_t dst;
        EdgeWeight_t weight;
        EdgeTag_t tag;
    private:
        Bytes m_col_data;
#ifdef SKG_PROPERTIES_SUPPORT_NULL
        // 标志属性是否有值
        PropertiesBitset_t m_properties_bitset;

        friend class PersistentEdge;
#endif
    public:
        MemoryEdge(const vid_t src_, const vid_t dst_, EdgeWeight_t weight_, EdgeTag_t tag_, size_t col_bytes)
                : src(src_), dst(dst_), weight(weight_), tag(tag_), m_col_data(col_bytes, 0),
                  m_properties_bitset() {
        }

        MemoryEdge(size_t bytes_size) : MemoryEdge(0, 0, 0, 0, bytes_size) {
        }

        MemoryEdge() : MemoryEdge(0) {
        }

        // copy function
//        MemoryEdge(const MemoryEdge &rhs) = delete;
//        MemoryEdge& operator=(const MemoryEdge &rhs) = delete;
        MemoryEdge(const MemoryEdge &rhs)
                : src(rhs.src), dst (rhs.dst),
                  weight(rhs.weight), tag(rhs.tag),
                  m_col_data(rhs.m_col_data) {
            m_properties_bitset = rhs.m_properties_bitset;
        }
        MemoryEdge& operator=(const MemoryEdge &rhs) {
            if (this != &rhs) {
                src = rhs.src;
                dst = rhs.dst;
                weight = rhs.weight;
                tag = rhs.tag;
                m_col_data = rhs.m_col_data;
                m_properties_bitset = rhs.m_properties_bitset;
            }
            return *this;
        }

        // move copy function
        MemoryEdge& operator=(MemoryEdge &&rhs) noexcept {
            if (this != &rhs) {
                src = rhs.src;
                dst = rhs.dst;
                weight = rhs.weight;
                tag = rhs.tag;
                std::swap(m_col_data, rhs.m_col_data);
                m_properties_bitset = rhs.m_properties_bitset;
            }
            return *this;
        }
        MemoryEdge(MemoryEdge && rhs) noexcept
                : src(rhs.src), dst(rhs.dst),
                  weight(rhs.weight), tag(rhs.tag), m_col_data() {
            *this = std::move(rhs);
        }

        bool IsStopper() const {
            return (src == 0 && dst == 0);
        }

        void CopyFrom(const PersistentEdge &edge, const Bytes &bytes_) {
            src = edge.src;
            dst = edge.dst;
            weight = edge.weight;
            tag = edge.tag;
            static_assert(sizeof(m_properties_bitset) == sizeof(edge.m_properties_bitset),
                "edge properties bitset size is not match");
            m_properties_bitset = edge.m_properties_bitset;
            m_col_data.reserve(bytes_.size());
            m_col_data = bytes_;
        }

        inline friend bool operator < (const MemoryEdge &lhs, const MemoryEdge &rhs) {
            // 先按src, 再按dst排序
            if (lhs.src == rhs.src) {
                return lhs.dst < rhs.dst;
            } else {
                return lhs.src < rhs.src;
            }
        }

        void SetData(const void *value, const size_t offset, const size_t value_bytes) {
            assert(offset + value_bytes <= m_col_data.size());
            memcpy(m_col_data.data() + offset, value, value_bytes);
        }

        void GetData(const size_t offset, const size_t value_bytes, void *value) const {
            memcpy(value, m_col_data.data() + offset, value_bytes);
        }

        const Bytes& GetColsData() const {
            return m_col_data;
        }

        Bytes& GetColsData() {
            return m_col_data;
        }

        static
        const MemoryEdge &GetStopper() {
            static MemoryEdge stopper_instance(0, 0, 0, 0, 0);
            return stopper_instance;
        }

        /**** 判断属性是否有设值的 API ****/
        inline
        bool IsPropertySet(uint32_t i) const {
            return m_properties_bitset.IsPropertySet(i);
        }

        inline
        void SetProperty(uint32_t i) {
            return m_properties_bitset.SetProperty(i);
        }

        inline
        void ClearProperty(uint32_t i) {
            return m_properties_bitset.ClearProperty(i);
        }

        inline
        void CopyProperty(const PropertiesBitset_t &bitset) {
            m_properties_bitset = bitset;
        }
    };

    struct MemoryEdgeSortedFunc { // 先按src, 再按dst排序
        bool operator() (const MemoryEdge &lhs, const MemoryEdge &rhs) const {
            if (lhs.src == rhs.src) {
                return lhs.dst < rhs.dst;
            } else {
                return lhs.src < rhs.src;
            }
        }
    };

    struct MemoryEdgeDstLessFunc {
        bool operator() (const MemoryEdge &lhs, const MemoryEdge &rhs) const {
            return lhs.dst < rhs.dst;
        }
    };

    struct MemoryEdgeDstSortedFunc { // 按照dst排序
        bool operator() (const MemoryEdge &lhs, const MemoryEdge &rhs) const {
            return lhs.dst > rhs.dst;
        }
    };

    /**
      * PairContainer encapsulates a pair of values of some type.
      * Useful for bulk-synchronuos computation.
      */
    template<typename ET>
    struct PairContainer {
        ET left;
        ET right;

        PairContainer() {
            left = ET();
            right = ET();
        }

        explicit
        PairContainer(int) {
            // TODO init by edge-weight
            left = ET();
            right = ET();
        }

        PairContainer(const ET &a, const ET &b) {
            left = a;
            right = b;
        }

        ET &oldval(int iter) {
            return (iter % 2 == 0 ? left : right);
        }

        void set_newval(int iter, ET x) {
            if (iter % 2 == 0) {
                right = x;
            } else {
                left = x;
            }
        }
    };

    struct shard_index {
        uint32_t vertexid;
        unsigned long filepos;
        unsigned long edgecounter;
        shard_index(uint32_t vertexid, unsigned long filepos, unsigned long edgecounter) : vertexid(vertexid), filepos(filepos), edgecounter(edgecounter) {}
    };

    inline
    const char *ColTypeToCString(ColumnType type) {
        switch (type) {
            case ColumnType::TAG:
                return "tag";
            case ColumnType::WEIGHT:
                return "weight";
            case ColumnType::INT32:
                return "int32";
            case ColumnType::INT64:
                return "int64";
            case ColumnType::FLOAT32:
                return "float32";
            case ColumnType::FLOAT64:
                return "float64";
            case ColumnType::FIXED_BYTES:
                return "fixed";
            case ColumnType::TIME:
                return "time";
            case ColumnType::VARCHAR:
                return "varchar";
            case ColumnType::GROUP:
                return "group";
            default:
                return "unknown";
        }
        return "unknown";
    }

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

static const bool kLittleEndian = PLATFORM_IS_LITTLE_ENDIAN;
#undef PLATFORM_IS_LITTLE_ENDIAN

}

#endif
