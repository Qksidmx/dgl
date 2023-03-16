#ifndef GFS_INTERNAL_SCHEMA_H
#define GFS_INTERNAL_SCHEMA_H

#include <cstdint>
#include <sstream>
#include "gfs_logger.h"


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


#ifndef __BYTE_ORDER
#define __BYTE_ORDER BYTE_ORDER
#endif
#ifndef __LITTLE_ENDIAN
#define __LITTLE_ENDIAN LITTLE_ENDIAN
#endif
#if defined(OS_MACOSX)
#include <machine/endian.h>
#endif

#if defined(__GNUC__) && __GNUC__ >= 4
    #define LIKELY(x)   (__builtin_expect((x), 1))
    #define UNLIKELY(x) (__builtin_expect((x), 0))
#else
    #define LIKELY(x)   (x)
    #define UNLIKELY(x) (x)
#endif

namespace gfs {

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
    static const size_t GFS_MAX_FILELINE_BUFF_SIZE = 4096;
    // 网络IO buff size
    static const size_t GFS_IOBUFF_SIZE = 40000;
    VARIABLE_IS_NOT_USED static const char *GFS_MSG_OK = "OK";
    VARIABLE_IS_NOT_USED static const size_t GFS_MSG_OK_LEN = strlen(GFS_MSG_OK);
    VARIABLE_IS_NOT_USED static const char *GFS_MSG_START = "START";
    VARIABLE_IS_NOT_USED static const size_t GFS_MSG_START_LEN = strlen(GFS_MSG_START);
    const static uint32_t MIN_SHARD_ID = 1;
    // 常量 byte 转 mb
    static const float MB_BYTES = 1.0f * 1024 * 1024;
    // 常量 us(microseconds) 转 ms(millisecond)
    static const uint32_t MICRO_PER_MS = 1000UL;
    // 常量 us(microseconds) 转 s(second)
    static const uint32_t MICRO_PER_SECOND = 1000UL * MICRO_PER_MS;
    // 节点全局属性 label
    VARIABLE_IS_NOT_USED static const char *GFS_GLOBAL_LABEL = "_global";
    VARIABLE_IS_NOT_USED static const EdgeTag_t GFS_GLOBAL_LABEL_TAG = 0;
    VARIABLE_IS_NOT_USED const static char* GFS_LABEL = "label";
    VARIABLE_IS_NOT_USED static const char *GFS_VERTEX_COLUMN_NAME_TAG = "vtag";
    VARIABLE_IS_NOT_USED static const char *GFS_VERTEX_COLUMN_NAME_DEGREE = "degree";
    VARIABLE_IS_NOT_USED static const char *GFS_EMPTY_COLUMN= "GFS_EMPTY_COLUMN";
    VARIABLE_IS_NOT_USED static const char *GFS_VERTEX_COLUMN_NAME_BITSET = "bitset";

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
                GFS_LOG_ERROR("trying to create interval {0}--{1}, illegal {0} > {1}\n", first, second);
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

        /*Status ParseFromRapidJsonValue(const rapidjson::Value &jVal) {
            if (!jVal.IsArray() || jVal.Size() != 2 || !jVal[0].IsUint64() || !jVal[1].IsUint64()) {
                return Status::InvalidArgument("invalid json interval");
            }
            first = static_cast<vid_t>(jVal[0].GetUint64());
            second = static_cast<vid_t>(jVal[1].GetUint64());
            return Status::OK();
        }*/

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
    struct MemoryEdge;
    struct PersistentEdge;

    template <typename EdgeDataType>
    class _edge {

    public:
        vid_t vertexid; // Source or Target vertex id. Clear from context.
        EdgeDataType * data_ptr;

        _edge() {}
        _edge(vid_t _vertexid, EdgeDataType * edata_ptr) : vertexid(_vertexid), data_ptr(edata_ptr) {
        }

#ifndef DYNAMICEDATA
        EdgeDataType get_data() {
            return * data_ptr;
        }

        void set_data(EdgeDataType x) {
            assert(false);
            *data_ptr = x;
        }
#else
        EdgeDataType * get_vector() {  // EdgeDataType is a chivector
            return data_ptr;
        }
#endif

        /**
          * Returns id of the endpoint of this edge.
          */
        vid_t vertex_id() {
            return vertexid;
        }


    }  __attribute__((packed));

    template <typename ET>
    bool eptr_less(const _edge<ET> &a, const _edge<ET> &b) {
        return a.vertexid < b.vertexid;
    }


#ifdef SUPPORT_DELETIONS

    /*
     * Hacky support for edge deletions.
     * Edges are deleted by setting the value of the edge to a special
     * value that denotes it was deleted.
     * In the future, a better system could be designed.
     */

    // This is hacky...
    static inline bool VARIABLE_IS_NOT_USED is_deleted_edge_value(int val);
    static inline bool VARIABLE_IS_NOT_USED is_deleted_edge_value(bool val) {
        return val;
    }

    static inline bool VARIABLE_IS_NOT_USED is_deleted_edge_value(int val);
    static inline bool VARIABLE_IS_NOT_USED is_deleted_edge_value(int val) {
        return 0xffffffff == (unsigned int)val;
    }

    static inline bool VARIABLE_IS_NOT_USED is_deleted_edge_value(vid_t val);
    static inline bool VARIABLE_IS_NOT_USED is_deleted_edge_value(vid_t val) {
        return 0xffffffffu == val;
    }


    static inline bool VARIABLE_IS_NOT_USED is_deleted_edge_value(float val);
    static inline bool VARIABLE_IS_NOT_USED is_deleted_edge_value(float val) {
        return !(val < 0 || val > 0);
    }

    static void VARIABLE_IS_NOT_USED remove_edgev(_edge<bool> * e);
    static void VARIABLE_IS_NOT_USED remove_edgev(_edge<bool> * e) {
        e->set_data(true);
    }

    static void VARIABLE_IS_NOT_USED remove_edgev(_edge<vid_t> * e);
    static void VARIABLE_IS_NOT_USED remove_edgev(_edge<vid_t> * e) {
        e->set_data(0xffffffff);
    }

    static void VARIABLE_IS_NOT_USED remove_edgev(_edge<int> * e);
    static void VARIABLE_IS_NOT_USED remove_edgev(_edge<int> * e) {
        e->set_data(0xffffffff);
    }
#endif

    template <typename VertexDataType, typename EdgeDataType>
    class _internal_vertex {

    public:   // Todo, use friend
        int inc;
        volatile int outc;

        vid_t vertexid;

    protected:
        _edge<EdgeDataType> * inedges_ptr;
        _edge<EdgeDataType> * outedges_ptr;


    public:
        bool modified;
        VertexDataType * dataptr;


        /* Accessed directly by the engine */
        bool scheduled;
        bool parallel_safe;

#ifdef SUPPORT_DELETIONS
        int deleted_inc;
        int deleted_outc;
#endif


        _internal_vertex() : inc(0), outc(0) {
#ifdef SUPPORT_DELETIONS
            deleted_outc = deleted_inc = 0;
#endif
            dataptr = NULL;
        }


        _internal_vertex(vid_t _id, _edge<EdgeDataType> * iptr, _edge<EdgeDataType> * optr, int indeg, int outdeg) : vertexid(_id), inedges_ptr(iptr), outedges_ptr(optr) {
            inc = 0;
#ifdef LOAD_OUTEDGE
            outc = 0;
#else
            outc = outdeg;
#endif
            scheduled = false;
            modified = false;
            parallel_safe = true;
            dataptr = NULL;
#ifdef SUPPORT_DELETIONS
            deleted_inc = 0;
            deleted_outc = 0;
#endif
        }

        virtual ~_internal_vertex() {}


        vid_t id() const {
            return vertexid;
        }

        int num_inedges() const {
            return inc;

        }
        int num_outedges() const {
            return outc;
        }
        int num_edges() const {
            return inc + outc;
        }

        inline void add_inedge(vid_t src, EdgeDataType * ptr) {
            if (inedges_ptr != NULL)
                inedges_ptr[inc] = _edge<EdgeDataType>(src, ptr);
            inc++;  // Note: do not move inside the brackets, since we need to still keep track of inc even if inedgeptr is null!
            assert(src != vertexid);
        }

        inline void _outedge(vid_t dst, EdgeDataType * ptr) {
#ifdef LOAD_OUTEDGE
            if (outedges_ptr != NULL)
                outedges_ptr[outc] = _edge<EdgeDataType>(dst, ptr);
            outc++;
            assert(dst != vertexid);
#else
            assert(false);
#endif
        }


    };

    template <typename VertexDataType, typename EdgeDataType >
    class _vertex : public _internal_vertex<VertexDataType, EdgeDataType> {

    public:

        _vertex() : _internal_vertex<VertexDataType, EdgeDataType>() { }

        _vertex(vid_t _id, _edge<EdgeDataType> * iptr, _edge<EdgeDataType> * optr, int indeg, int outdeg) : _internal_vertex<VertexDataType, EdgeDataType>(_id, iptr, optr, indeg, outdeg) {}

        virtual ~_vertex() {}

        /**
          * Returns ith edge of a vertex, ignoring
          * edge direction.
          */
        _edge<EdgeDataType> * edge(int i) {
            if (i < this->inc) return inedge(i);
            else return outedge(i - this->inc);
        }


        _edge<EdgeDataType> * inedge(int i) {
            assert(i >= 0 && i < this->inc);
            return &this->inedges_ptr[i];
        }

	//added by Jiefeng Cheng, 4/2/2015
        _edge<EdgeDataType> * inedge_ptr() {
            return this->inedges_ptr;
        }
        _edge<EdgeDataType> * outedge_ptr() {
            return this->outedges_ptr;
        }

        _edge<EdgeDataType> * outedge(int i) {
#ifdef LOAD_OUTEDGE
            assert(i >= 0 && i < this->outc);
            return &this->outedges_ptr[i];
#else
            assert(false);
            return NULL;
#endif
        }

        _edge<EdgeDataType> * random_outedge() {
            if (this->outc == 0) return NULL;
            return outedge((int) (std::abs(random()) % this->outc));
        }

        /**
          * Get the value of vertex
          */
#ifndef DYNAMICVERTEXDATA
        VertexDataType get_data() {
            return *(this->dataptr);
        }
#else
        // VertexDataType must be a chivector
        VertexDataType * get_vector() {
            this->modified = true;  // Assume vector always modified... Temporaryh solution.
            return this->dataptr;
        }
#endif

        /**
          * Modify the vertex value. The new value will be
          * stored on disk.
          */
        virtual void set_data(VertexDataType d) {
            *(this->dataptr) = d;
            this->modified = true;
        }

        // TODO: rethink
        static bool computational_edges() {
            return false;
        }
        static bool read_outedges() {
            return true;
        }


        /**
         * Sorts all the edges. Note: this will destroy information
         * about the in/out direction of an edge. Do use only if you
         * ignore the edge direction.
         */
        void VARIABLE_IS_NOT_USED sort_edges_indirect() {
            // Check for deleted edges first...
            if (this->inc != (this->outedges_ptr - this->inedges_ptr)) {
                // Moving
                memmove(&this->inedges_ptr[this->inc], this->outedges_ptr, this->outc * sizeof(_edge<EdgeDataType>));
                this->outedges_ptr = &this->inedges_ptr[this->inc];
            }
            quickSort(this->inedges_ptr, (int) (this->inc + this->outc), eptr_less<EdgeDataType>);

        }


#ifdef SUPPORT_DELETIONS
        void VARIABLE_IS_NOT_USED remove_edge(int i) {
            remove_edgev(edge(i));
        }

        void VARIABLE_IS_NOT_USED remove_inedge(int i) {
            remove_edgev(inedge(i));
        }

        void VARIABLE_IS_NOT_USED remove_outedge(int i) {
            remove_edgev(outedge(i));
        }
#endif


    };

    template <typename EdgeDataType>
        struct edge_with_value {
        vid_t src;
        vid_t dst;
        EdgeDataType value;
        edge_with_value() {}
        edge_with_value(vid_t src, vid_t dst, EdgeDataType value) : src(src), dst(dst), value(value) {
        }

        bool stopper() { return src == 0 && dst == 0; }
    };

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

        friend struct MemoryEdge;
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

        friend struct PersistentEdge;
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
}

#endif
