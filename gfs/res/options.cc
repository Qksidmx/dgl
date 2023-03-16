#include "options.h"
#include "cmdopts.h"
#include "fmt/format.h"

namespace gfs {
    // memory shard size
    size_t Options::init_shard_size_mb() const {
        return static_cast<size_t>(shard_init_per * shard_size_mb);
    }

    std::string Options::GetDBDir(const std::string &name) const {
        return fmt::format("{}/{}/", default_db_dir, name);
    }

    void Options::LoadOptionsFromFile(const std::string &path) {
        Config::instance().ParseFileOpts(path, "");
        LoadOptions();
    }

    void Options::SetByCmdOptions() {
        LoadOptions();
    }

    void Options::LoadOptions() {
        std::string tbl_type = get_option_string("mem-table", "hash");
        if (tbl_type == "vec") {
            mem_table_type = MemTableType::Vec;
        } else if (tbl_type == "hash") {
            mem_table_type = MemTableType::Hash;
        } else {
            mem_table_type = MemTableType::Hash;
        }
        // 插入边的缓存
        mem_buffer_mb = static_cast<size_t>(get_option_int("mem_buffer_mb", 128));
//        SKG_LOG_INFO("set mem_buffer_mb={}", mem_buffer_mb);

        // 每个shard的大小
        shard_size_mb = static_cast<size_t>(get_option_int("shard_size_mb", 1024));
//        SKG_LOG_INFO("set shard_size_mb={}", shard_size_mb);

        // 边属性的缓存大小
        edata_cache_mb = static_cast<size_t>(get_option_int("edata_cache_mb", 128));
//        SKG_LOG_INFO("set edata_cache_mb={}", edata_cache_mb);

        // ID转换的cache
        id_convert_cache_mb = static_cast<size_t>(get_option_int("id_convert_cache_mb", 100));
//        SKG_LOG_INFO("set id_convert_cache_mb={}", id_convert_cache_mb);
        // ID转换的hot-key个数
        id_convert_num_hot_key = static_cast<size_t>(get_option_int("id_convert_num_hot_key", 50));
//        SKG_LOG_INFO("set id_convert_num_hot_key={}", id_convert_num_hot_key);


        socket_timeout = get_option_int("socket_timeout", 3);
        max_batch_edges = get_option_int("max_batch_edges", 500);
        max_query_edges = get_option_int("max_query_edges", 3000);
        grpc_timeout = get_option_int("grpc_timeout", 1);

        max_interval_length = static_cast<size_t>(get_option_int("max_interval_length", 30000000));
//        SKG_LOG_INFO("set max_interval_length={}", max_interval_length);

        sample_rate = static_cast<uint32_t>(get_option_int("sample_rate", 1000));
        sample_interval = static_cast<uint32_t>(get_option_int("sample_interval", 10));

        // bulkload 过程中, worker 生成 partition 的并行度. 最大不超过 shard_split_factor
        parallelism_per_bulkload_worker = static_cast<uint32_t>(get_option_int(
                "parallelism_per_bulkload_worker", 2));
        parallelism_per_bulkload_worker = std::min(
                parallelism_per_bulkload_worker, static_cast<uint32_t >(shard_split_factor));

        bulkload_router_recv_inproc_timeout_ms = get_option_int("bulkload_router_timeout_ms", 1000);

        use_mmap_read = get_option_uint("use_mmap_read", 1) != 0;
        use_mmap_populate = get_option_uint("use_mmap_populate", 0) != 0;
        use_mmap_locked = get_option_uint("use_mmap_locked", 0) != 0;

        use_elias_gamma_compress = get_option_uint("use_elias_gamma_index", 0) != 0;

        query_threads = get_option_uint("query_threads", 8);
        master_mt_thread_pool_num = get_option_uint("master_mt_thread_pool_num",128);
        // 建表的文件夹
        default_db_dir = std::string {get_option_string("db_dir", "db/")};
//        SKG_LOG_INFO("set db_dir={}", default_db_dir);
        nlimit = 1 << 30;
    }
}
