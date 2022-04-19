#ifndef STARKNOWLEDGEGRAPHDATABASE_GSHARDER_HPP
#define STARKNOWLEDGEGRAPHDATABASE_GSHARDER_HPP

#include <cassert>

#include "metrics/metrics.hpp"
#include "metrics/reps/basic_reporter.hpp"
#include "engine/auxdata/degree_data.hpp"
#include "preprocessing/types.h"
#include "preprocessing/filter.h"
#include "util/kwaymerge.hpp"
#include "storage/meta/Metadata.hpp"
#include "storage/partition/EdgePartition.hpp"
#include "util/radixSort.hpp"

#include "shards/memoryshard.hpp"
#include "shards/slidingshard.hpp"

namespace skg { namespace preprocess {

    struct gshovel_merge_source : public merge_source<MemoryEdge> {
    public:
        enum Flags: uint32_t {
            has_tag = 0x1,
            has_weight = 0x2,
        };
    private:
        // 原存储文件名字
        std::string shovelfile;
        // 文件操作描述符
        int f;
        // 每条序列化后边的二进制数据占用多少字节
        const size_t m_bytes_edge_record;
        // 除去边的两个节点,tag,weight后, 边的属性二进制数据占用多少字节
        size_t m_bytes_edge_cols_data;
        // 文件中存储多少条边
        size_t m_num_edges;

        // 文件读入缓冲区大小(字节数)
        size_t m_bytes_buf_size;
        // 文件中已处理的边数
        size_t idx;
        // 缓冲区
        char * buffer;
        // 已处理的缓冲区
        char * buffer_ptr;

        const uint32_t m_flags;

        bool has_tag_column() const {
            return (m_flags & static_cast<uint32_t>(Flags::has_tag)) != 0;
        }

        bool has_weight_column() const {
            return (m_flags & static_cast<uint32_t>(Flags::has_weight)) != 0;
        }

    public:
        gshovel_merge_source(const std::string &shovelfile, size_t bytes_buf_size, size_t bytes_per_edge_record, uint32_t flags)
                : shovelfile(shovelfile), f(-1),
                  m_bytes_edge_record(bytes_per_edge_record),
                  m_bytes_edge_cols_data(0),
                  m_num_edges(0),
                  m_bytes_buf_size(bytes_buf_size),
                  idx(0), buffer(nullptr), buffer_ptr(nullptr),
                  m_flags(flags) {
            assert(m_bytes_buf_size % m_bytes_edge_record == 0);
            f = open(shovelfile.c_str(), O_RDONLY);

            if (f < 0) {
                SKG_LOG_FATAL("Could not open shovel file: {}, {}({})",
                              shovelfile, errno, strerror(errno));
                assert(f>=0);
            }

            m_bytes_edge_cols_data = m_bytes_edge_record - sizeof(vid_t) - sizeof(vid_t);
            if (has_tag_column()) {
                m_bytes_edge_cols_data -= sizeof(EdgeTag_t);
            }
            if (has_weight_column()) {
                m_bytes_edge_cols_data -= sizeof(EdgeWeight_t);
            }

            buffer = static_cast<char *>(malloc(m_bytes_buf_size));
            m_num_edges = PathUtils::getsize(shovelfile) / m_bytes_edge_record;
            load_next();
        }

        ~gshovel_merge_source() override {
            finish();
        }

        void finish() {
            if (f >= 0) {
                close(f);
                f = -1;
#ifndef SKG_PRESERVE_PREPROCESS_CHUNKS
                SKG_LOG_INFO("Removing shovelfile: {}", shovelfile);
                PathUtils::RemoveFile(shovelfile);
#else
                SKG_LOG_INFO("Closing shovefile: {}", shovelfile);
#endif
            }

            if (buffer != nullptr) {
                free(buffer);
                buffer = nullptr;
            }
        }

        void load_next() {
            // 读入的字节数. 当未读到文件尾的时候, 读入缓冲区大小; 当到文件尾不足缓冲区大小, 只读入到文件尾
            const size_t bytes_to_read = std::min(m_bytes_buf_size,
                                                  ((m_num_edges - idx) * m_bytes_edge_record));
            preada(f, buffer, bytes_to_read, idx * m_bytes_edge_record);
            buffer_ptr = buffer;
        }

        bool has_more() override {
            return idx < m_num_edges;
        }

        MemoryEdge next() override {
            if (buffer_ptr == buffer + m_bytes_buf_size) {  // buffer 中的数据已经处理完, 载入下一段buffer
                load_next();
            }
            MemoryEdge edge(0, 0, 0, 0, m_bytes_edge_cols_data);
            memcpy(&edge.src, buffer_ptr, sizeof(vid_t));
            buffer_ptr += sizeof(vid_t);
            memcpy(&edge.dst, buffer_ptr, sizeof(vid_t));
            buffer_ptr += sizeof(vid_t);
            if (has_tag_column()) {
                memcpy(&edge.tag, buffer_ptr, sizeof(EdgeTag_t));
                buffer_ptr += sizeof(EdgeTag_t);
            }
            if (has_weight_column()) {
                memcpy(&edge.weight, buffer_ptr, sizeof(EdgeWeight_t));
                buffer_ptr += sizeof(EdgeWeight_t);
            }
            if (m_bytes_edge_cols_data > 0) {
                edge.SetData(buffer_ptr, 0, m_bytes_edge_cols_data);
                buffer_ptr += m_bytes_edge_cols_data;
            }
            idx++;
            if (idx == m_num_edges) { // 已经处理完文件中所有边
                finish();
            }
            return edge;
        }
    };


    class GSharder: public merge_sink<MemoryEdge>{
    public:
        explicit
        GSharder(const std::string &basefilename, size_t numedges, vid_t max_vertex_id, uint32_t num_shelters,
                 const std::vector<IEdgeColumnPtr> &cols,
                 const Options &options)
                : m_basefilename(basefilename),
                  m_num_edges(numedges), m_max_vertex_id(max_vertex_id), m_num_shelters(num_shelters),
                  m("GSharder"), m_nshards(1),
                  sharded_edges(0), edges_per_shard(1), shardnum(0), intervals(),
                  sink_buffer(), shard_capacity(1),
                  prevvid(0), cur_interval_start_vid(0),
                  m_options(options), m_num_memory_shards(0), m_level_intervals(), m_cols(cols), m_bytes_per_edge_record(0),
                  degrees(nullptr) {
            // 带边属性的每条边序列化之后, 二进制形式占用的Bytes
            m_bytes_per_edge_record = sizeof(vid_t) + sizeof(vid_t);
            for (const auto &col: m_cols) {
                m_bytes_per_edge_record += col->value_size();
            }
            // 加上边属性所占用的空间
            m_bytes_per_persistent_edge = sizeof(PersistentEdge);
#ifndef SKG_EDGE_DATA_EMBED
            for (const auto &col: m_cols) {
                if (col->isEmbed()) { continue; } // 嵌入到PersistentEdge存储的属性列, 不算到占用空间里面
                m_bytes_per_persistent_edge += col->value_size();
            }
#endif
        }

        virtual ~GSharder() = default;
    public:

        std::vector<interval_t> GetIntervals() const {
            if (!m_level_intervals.empty()) {
                return m_level_intervals;
            } else {
                return {interval_t(0, 0),};
            }
        }

        size_t GetNumMemoryShards() const {
            return m_num_memory_shards;
        }

        vid_t GetMaxVertexID() const {
            return m_max_vertex_id;
        }

        /**
         * Executes sharding.
         * @param nshards_string the number of shards as a number, or "auto" for automatic determination
         */
        uint32_t ExecuteSharding(const std::string &nshards_string) {
            m.start_time("GSharder.ExecuteSharding");

            m_nshards = DetermineNumberOfShards(nshards_string);  // set nshards
            if (m_num_edges > 0) { // trivial case
                WriteAllShards();
            } else {
                std::vector<MemoryEdge> empty_edges;
                interval_t empty_interval(0, 0);
                FinishShard(std::move(empty_edges),  empty_interval);
                intervals.emplace_back(empty_interval);
                m_edges_per_intervals.emplace_back(0);
                m_num_memory_shards = 1;
            }

            m.stop_time("GSharder.ExecuteSharding");

#ifdef COUNT_MEMORY_EDGE_MOVING
            m.add("MemoryEdge::copying_construct", MemoryEdge::copying_construct);
            m.add("MemoryEdge::copying_assign", MemoryEdge::copying_assign);
            m.add("MemoryEdge::moving_construct", MemoryEdge::moving_construct);
            m.add("MemoryEdge::moving_assign", MemoryEdge::moving_assign);
#endif

            /* Print metrics */
            basic_reporter basicrep;
            m.report(basicrep);

            return m_nshards;
        }

    protected:

        uint32_t DetermineNumberOfShards(const std::string &nshards_string) {
            SKG_LOG_INFO("Determining number of shards by options.", "");
            // 预计需要生成多少mb的数据
            const double data_size_mb = m_num_edges * m_bytes_per_persistent_edge * 1.0 / (1024 * 1024);
            SKG_LOG_INFO("Sharding {:.2f}mb data with memory shard size: {}mb",
                         data_size_mb, m_options.memory_shard_size_mb);
            // 计算 memory-shard 个数上限
            uint32_t num_memory_shard = static_cast<uint32_t>(ceil(
                    1.0 * data_size_mb / (m_options.shard_init_per * m_options.memory_shard_size_mb)));
            // 计算每个 memory-shard 大概需要拆分为多少个底层的叶子节点
            // issue #24. Sharding时,最少生成两层的LSM结构. 若只保留一层, 在插入边的时候会退化为对全图 re-sharding
            uint32_t num_leaves_shards = num_memory_shard * m_options.shard_split_factor;
            SKG_LOG_INFO("To be create: #memory shards:{} #leaves shards: {}", num_memory_shard, num_leaves_shards);
            return num_leaves_shards;
        }

        /**
         * Write the shard by sorting the shovel file and compressing the
         * adjacency information.
         * To support different shard types, override this function!
         */
        void WriteAllShards() {
            size_t membudget_mb = (size_t) get_option_int("membudget_mb", 1024);

            // KWAY MERGE
            sharded_edges = 0;
            edges_per_shard = m_num_edges / m_nshards + 1;
            edges_per_memory_shard = m_num_edges / (m_nshards / m_options.shard_split_factor) + 1;
            shard_capacity = edges_per_shard / 2 * 3;  // Shard can go 50% over
            shardnum = 0;
            cur_interval_start_vid = 0;
            sink_buffer.reserve(shard_capacity);
            SKG_LOG_INFO("Total: {}, nshards: {}, edge per shard: {}",
                         m_num_edges, m_nshards, edges_per_shard);

            /* Initialize kway merge sources */
            // membudget_mb 的一半, 平均分配到每个shelter源中, 用于读取文件内容
            size_t B = membudget_mb * 1024 * 1024 / 2 / m_num_shelters;
            while (B % m_bytes_per_edge_record != 0) B++;
            SKG_LOG_INFO("Buffer size in merge phase: {:.2f}mb, bytes_per_edge_record {}",
                         1.0 * B / (1024 * 1024), m_bytes_per_edge_record);
            prevvid = static_cast<vid_t>(-1);
            // 是否含有tag列, weight列
            uint32_t flags = 0x0;
            for (const auto&col: m_cols) {
                if (col->edgeColType() == ColumnType::TAG) {
                    flags |= gshovel_merge_source::has_tag;
                }
                if (col->edgeColType() == ColumnType::WEIGHT) {
                    flags |= gshovel_merge_source::has_weight;
                }
            }
            // k路归并的文件源
            std::vector< merge_source<MemoryEdge> *> sources;
            for(uint32_t i=0; i < m_num_shelters; i++) {
                sources.push_back(
                        new gshovel_merge_source(
                                gshovel_filename(m_basefilename, i), B, m_bytes_per_edge_record, flags));
            }

            kway_merge<MemoryEdge, MemoryEdgeDstSortedFunc> merger(sources, this, MemoryEdgeDstSortedFunc());
            m.start_time("GSharder.merger.merge");
            merger.merge(); // 从所有的shovel文件，做k路归并，生成shard文件
            m.stop_time("GSharder.merger.merge");

            // Delete sources
            for(size_t i=0; i < sources.size(); i++) {
                delete sources[i];
                sources[i] = nullptr;
            }
        }

        /* Begin: Kway -merge sink interface */
        /**
         * 对k个按照 dst 排序的边文件, 进行k路归并时加边操作
         * @param edge 边
         */
//        void add(const MemoryEdge &val) override {
        void add(MemoryEdge &&edge) override {
            // 当前shard边数>平均数时, 创建新的shard (需同时保证同一个dst的边在同一个shard中)
            if (sink_buffer.size() >= edges_per_shard || edge.dst - cur_interval_start_vid > m_options.max_interval_length) {
                if (edge.dst != prevvid) {
                    const size_t diff = edge.dst - cur_interval_start_vid;
                    if (diff > m_options.max_interval_length) {
                        SKG_LOG_WARNING("Creating unexpected shard.", "");
                        m_nshards++;
                    }
                    SKG_LOG_DEBUG("Creating new shard, # buffer: {}, interval {}-{}, new dst {}, diff: {}",
                                  sink_buffer.size(), cur_interval_start_vid, prevvid,
                                  edge.dst, diff);
                    CreateNextShard();
                }
            }

//            sink_buffer.emplace_back(edge);
            sink_buffer.emplace_back(std::move(edge));
            prevvid = edge.dst;
            sharded_edges++;
        }

        /**
         * 所有合并源的数据都已经全部读完, 收尾工作
         */
        void done() override {
            CreateNextShard();  // 生成最后一个shard

            if (m_num_edges != sharded_edges) {
                SKG_LOG_INFO("Shoveled {} but sharded {} edges", m_num_edges, sharded_edges);
            }

            SKG_LOG_INFO("Created {} leaves shards, for {} edges", shardnum, sharded_edges);
            assert(shardnum <= m_nshards);

            Status s;
            if (m_nshards != 1) {
                s = CreateAllMemoryShards(
                        intervals, m_edges_per_intervals, edges_per_memory_shard, m_cols,
                        m_options.shard_split_factor,
                        &m_num_memory_shards, &m_level_intervals);
                if (!s.ok()) {
                    // TODO check status
                }
            } else {
                SKG_LOG_INFO("Dummy shard, nshards = 1", "");
                m_num_memory_shards = 1;
                m_level_intervals = intervals;
            }
        }

        Status CreateAllMemoryShards(const std::vector<interval_t> &leaves_intervals,
                                     const std::vector<size_t> &edges_per_interval,
                                     const size_t num_edges_memory_shard,
                                     const std::vector<IEdgeColumnPtr> &cols,
                                     const uint8_t factor,
                                     size_t *pNumMemoryShards,
                                     std::vector<interval_t> *pLSMIntervals) {
            assert(!leaves_intervals.empty()); // 不为空
            SKG_LOG_INFO("LSM shard, creating memory shards", "");
            // 清理输出变量
            *pNumMemoryShards = 0;
            pLSMIntervals->clear();
            Status s;
            std::set<size_t> invalid_shards;
            // 每factor个子shard组成MemoryShard
            size_t scopeBeg = 0, scopeEnd = 0;
            for (size_t i = 0; i < leaves_intervals.size(); ++i) {
                if ((i % factor == 0 && i != 0) || i == leaves_intervals.size() - 1) {
                    if (i == leaves_intervals.size() - 1) {
                        scopeEnd = i;
                    } else {
                        scopeEnd = i - 1;
                    }
                    if (scopeBeg != scopeEnd) {
                        const interval_t interval(
                                leaves_intervals[scopeBeg].first,
                                leaves_intervals[scopeEnd].second
                        );
                        s = CreateEmptyMemoryShard(interval, cols, pLSMIntervals);
                        if (!s.ok()) {
                            SKG_LOG_ERROR("Error while creating memory shard: {}", s.ToString());
                        }
                    } else {
                        // 单独一个shard作为顶层shard的情况
                        SKG_LOG_WARNING("Stealing shard {} to top level shard.", leaves_intervals[i]);
                        pLSMIntervals->emplace_back(leaves_intervals[i]);
                        invalid_shards.insert(i);
                    }
                    scopeBeg = i;
                }
            }
            *pNumMemoryShards = pLSMIntervals->size();
            for (size_t i = 0; i < leaves_intervals.size(); ++i) {
                if (invalid_shards.find(i) == invalid_shards.end()) {
                    pLSMIntervals->emplace_back(leaves_intervals[i]);
                }
            }
            return Status::OK();
        }

        Status CreateEmptyMemoryShard(
                const interval_t interval,
                const std::vector<IEdgeColumnPtr> &cols,
                std::vector<interval_t> *pLSMIntervals) {
            SKG_LOG_DEBUG("Creating memory shard: {}", interval);
            Status s;
//            Status s = EdgePartition::Create(m_basefilename, interval);
//            if (s.ok()) {
//                pLSMIntervals->emplace_back(interval);
//            }
            return s;
        }

        /* End: Kway -merge sink interface */


        Status CreateNextShard() {
            m.start_time("GSharder.CreateNextShard");
            assert(shardnum < m_nshards);
            // v的interval
            intervals.emplace_back(cur_interval_start_vid, (shardnum == m_nshards - 1 ? m_max_vertex_id : prevvid));
            m_edges_per_intervals.emplace_back(sink_buffer.size());
            cur_interval_start_vid = prevvid + 1;
            // 把当前shard刷入磁盘
            Status s = FinishShard(std::move(sink_buffer), intervals.back());
            if (!s.ok()) { return s; }
            shardnum++;
            // 创建新shard-edges的缓冲区
            sink_buffer.clear();
            m.stop_time("GSharder.CreateNextShard");

            // Adjust edges per shard so that it takes into account how many edges have been spilled now
            SKG_LOG_INFO("Shards: {}/{}({:3.2f}%), remaining edges: {}, edges per shard: {}",
                         shardnum, m_nshards, 100.0 * shardnum / m_nshards,
                         m_num_edges - sharded_edges, edges_per_shard);
            if (shardnum < m_nshards) edges_per_shard = (m_num_edges - sharded_edges) / (m_nshards- shardnum);
            SKG_LOG_INFO("Adjusted edges per shard: {}", edges_per_shard);
            return s;
        }

        /**
         * 把shard(edge-partition)刷入磁盘存储
         * @param shovelbuf
         * @param interval
         */
        Status FinishShard(std::vector<MemoryEdge> &&shovelbuf, const interval_t &interval) {
            skg::Status s;
//            s = skg::EdgePartition::Create(m_basefilename, interval);
            if (!s.ok()) { return s;}
            std::shared_ptr<skg::EdgePartition> pShard;
//            s = skg::EdgePartition::Open(m_basefilename, 0, 0, interval, m_options, &pShard);
            if (!s.ok()) { return s;}
//            s = pShard->MergeEdgesAndFlush(std::move(shovelbuf), interval);
            if (!s.ok()) { return s;}
            return s;
        }

    protected:
        const std::string m_basefilename;
        const size_t m_num_edges;
        const vid_t m_max_vertex_id;
        const uint32_t m_num_shelters;

        metrics m;

        uint32_t m_nshards;

        // shard 处理的边总数量
        size_t sharded_edges;
        // 估算每个shard的边数
        size_t edges_per_shard;
        size_t edges_per_memory_shard;
        // 生成的shard个数
        uint32_t shardnum;
        // 生成的interval
        std::vector<interval_t> intervals;
        std::vector<size_t> m_edges_per_intervals;

        // shard buffer
        std::vector<MemoryEdge> sink_buffer;
        size_t shard_capacity;  // shard 中边的上限(可能动态调整)

        vid_t prevvid;
        vid_t cur_interval_start_vid;

        const Options &m_options;
        size_t m_num_memory_shards;
        std::vector<interval_t> m_level_intervals;
        std::vector<IEdgeColumnPtr> m_cols;
        size_t m_bytes_per_edge_record;
        size_t m_bytes_per_persistent_edge;

        degree * degrees;  // 节点度数
    };
}}
#endif //STARKNOWLEDGEGRAPHDATABASE_JSHARDER_HPP
