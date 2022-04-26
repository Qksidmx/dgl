#ifndef STARKNOWLEDGEGRAPH_OPTIONS_HPP
#define STARKNOWLEDGEGRAPH_OPTIONS_HPP

#include <cstdio>
#include <string>
#include <stdint.h>

namespace skg{

class Options {
public:
    Options()
        : mem_buffer_mb(128),
          mem_table_type(Hash),
        shard_size_mb(1024),
        shard_init_per(0.7),
        shard_split_factor(4),
        edata_cache_mb(128),
        force_create(false),
        id_convert_cache_mb(100),
        id_convert_num_hot_key(50),
        max_interval_length(30000000),
        separator(','),
        sample_rate(100),
        sample_interval(5),
        sample_seed(0),
        parallelism_per_bulkload_worker(1),
        bulkload_router_recv_inproc_timeout_ms(1000),
        id_type(VertexIdType::STRING),
        master_ip(""),
        master_port(0),
        socket_timeout(3),
        user_id(""),
        cluster_id(""),
        max_batch_edges(500),
        max_query_edges(3000),
        grpc_timeout(1),
          use_mmap_read(true),
          use_mmap_populate(false),
          use_mmap_locked(false),
          use_elias_gamma_compress(false),
	master_mt_thread_pool_num(100),
          query_threads(8),
        default_db_dir("./db") {
    }
public:
    //
    unsigned int num_parallel_reads_per_node;
    unsigned int num_parallel_writes_per_node;
    // 缓存的边占用内存空间上限
    size_t mem_buffer_mb;


    enum MemTableType {
        Hash = 0,
        Vec = 1,
    };
    MemTableType mem_table_type;

    // shard 占用空间上限
    size_t shard_size_mb;
    // 静态sharding时, 每个shard中可占用的空间上限
    // 为 shard_size_mb * shard_init_per
    float shard_init_per;
    // shard 占用空间达到上限时, 拆分为多少份
    uint8_t shard_split_factor;
    // 边属性缓存大小
    size_t edata_cache_mb;

    // 强制清空数据重新生成db
    bool force_create;

    // ID转换的cache
    size_t id_convert_cache_mb;

    // ID转换hot-key缓存个数
    size_t id_convert_num_hot_key;

    size_t max_interval_length;

    // Deprecated
    char separator;

    // bulkload 时, 采样比例. 用于减少采样读文件的时间开销
    uint32_t sample_rate;

    // bulkload 时, 节点采样的间隔. 用于减少采样统计的空间开销
    uint32_t sample_interval;

    // bulkload 时, sample 随机种子, 用于复现结果
    uint32_t sample_seed;

    // bulkload 时, 每个 worker 最大可同时生成多少个 partition
    uint32_t parallelism_per_bulkload_worker;

    // bulkload 时, router 从其他线程收包的超时时间
    int32_t bulkload_router_recv_inproc_timeout_ms;

    enum class VertexIdType {
        LONG,
        STRING,
    };

    VertexIdType id_type;

    //分布式数据库打开时，需要指定IP和端口
    std::string master_ip;
    uint32_t master_port;
    //分布式数据库建立连接时，设置的超时时间
    uint32_t socket_timeout;
    std::string user_id;//腾讯云账户id
    std::string cluster_id;//星图集群id
    //分部署图数据库GRPC及返回给客户端一个包最多返回边数
    uint32_t max_batch_edges;
    //查询中一次最多吐给客户端多少条边
    uint32_t max_query_edges;
    //master查询slave使用的grpc时间
    uint32_t grpc_timeout;

    // edgelist / src-index / dst-index 采用 mmap 形式读取
    bool use_mmap_read;

    // edgelist mmap 读时参数, 仅在 use_mmap_read = true 时有效
    bool use_mmap_populate;
    bool use_mmap_locked;

    // out-index 是否采用 elias gamma compress 的形式存储到内存中
    bool use_elias_gamma_compress;
    int master_mt_thread_pool_num;
    uint32_t query_threads;

    // 指定 Write-Ahead logs(WAL)的存储路径
    // 如果为空, 则在 "`GetDBDir()`/journal/" 目录下.
    // 如果非空, 则会存储在制定的文件夹下.
    std::string wal_dir = "";

    int nlimit = 50000;


public:
    size_t init_shard_size_mb() const;

    std::string default_db_dir;
    std::string GetDBDir(const std::string &name) const;

public:
    // Deprecated, use InitAndLoadOptions/LoadOptions instead
    void SetByCmdOptions();

    void LoadOptionsFromFile(const std::string &path);
    void LoadOptions();
};

class BulkUpdateOptions {
public:
    BulkUpdateOptions()
            : num_edges_batch(2000000),
              num_edges_new_shard_threshold(10000),
              split_type(VecSplit) {
    }

public:
    // 每次批量更新处理的数量
    size_t num_edges_batch;
    // 如果插入新的边, 在新的范围区间数量达到阈值, 则生成新的 ShardTree
    // 否则合并到最后一个 ShardTree 中
    size_t num_edges_new_shard_threshold;

    // 拆分数据并行的方法
    enum SplitType {
        VecSplit = 0,
        SortSplit = 1,
    };
    SplitType split_type;

    inline
    void DisableWAL() {
        m_is_wal_enabled = false;
    }

    inline
    bool IsWALEnabled() const {
        return m_is_wal_enabled;
    }

    inline
    void EnableSync() {
        m_is_sync_enabled = true;
    }

    inline
    bool IsSyncEnabled() const {
        return m_is_sync_enabled;
    }
private:
    // 是否不需要写WAL
    bool m_is_wal_enabled;
    // 在 `write()` 后 是否使用 `fsync()` 进行强同步, 默认为 false
    bool m_is_sync_enabled;
};

enum class WALRecoveryMode : char {
    // Original levelDB recovery
    // We tolerate incomplete record in trailing data on all logs
    // Use case : This is legacy behavior
            kTolerateCorruptedTailRecords = 0x00,
    // Recover from clean shutdown
    // We don't expect to find any corruption in the WAL
    // Use case : This is ideal for unit tests and rare applications that
    // can require high consistency guarantee
            kAbsoluteConsistency = 0x01,
    // Recover to point-in-time consistency (default)
    // We stop the WAL playback on discovering WAL inconsistency
    // Use case : Ideal for systems that have disk controller cache like
    // hard disk, SSD without super capacitor that store related data
            kPointInTimeRecovery = 0x02,
    // Recovery after a disaster
    // We ignore any corruption in the WAL and try to salvage as much data as
    // possible
    // Use case : Ideal for last ditch effort to recover data or systems that
    // operate with low grade unrelated data
            kSkipAnyCorruptedRecords = 0x03,
};
}

#endif //STARKNOWLEDGEGRAPH_OPTIONS_HPP
