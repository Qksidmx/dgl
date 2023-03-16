#ifndef STARKNOWLEDGEGRAPHDATABASE_SKGFILENAMES_HPP
#define STARKNOWLEDGEGRAPHDATABASE_SKGFILENAMES_HPP

#include <string>
#include <iostream>
#include "string_utils.h"
#include "fmt/format.h"

#include "types.h"
#include "slice.h"
#include "internal_schema.h"
#include "ioutil.h"
#include "pathutils.h"

namespace gfs {

    class DIRNAME {
    public:
	    static VARIABLE_IS_NOT_USED std::string filename_shard_edge_column_block(const std::string &column_dir, size_t blockid) {
		return fmt::format("{}/{}", column_dir, blockid);
	    }
	    static VARIABLE_IS_NOT_USED std::string filename_shard_edge_column_size(
		    const std::string &column_dir, const std::string &colname) {
		return fmt::format("{}/{}.col.sz", column_dir, colname);
	    }
	    static VARIABLE_IS_NOT_USED std::string filename_shard_edge_column_size(const std::string &column_dir) {
		return fmt::format("{}/col.sz", column_dir);
	    }

	    static VARIABLE_IS_NOT_USED std::string filename_shard_edge_column(
		    const std::string &shardfile, const std::string &colname) {
		return fmt::format("{}/{}", dirname_shard_edge_columns(shardfile), colname);
	    }

	    static VARIABLE_IS_NOT_USED std::string dirname_shard_edge_columns (const std::string &shardfile) {
		return fmt::format("{}_col", shardfile);
	    }
	    static VARIABLE_IS_NOT_USED std::string dirname_shard_edge_column_block(
		    const std::string &shardfile, const std::string &colname) {
		return fmt::format("{}/{}", dirname_shard_edge_columns(shardfile), colname);
	    }
        static std::string SKG_ROOT() {
            char * skg_root = getenv("SKG_ROOT");
            if (skg_root != nullptr) {
                return std::string(skg_root);
            } else {
                return ".";
            }
        }

        static std::string ENGINE_BIN() {
            return fmt::format("{}/bin/example_apps", SKG_ROOT());
        }
        /**
         * partition 存放的文件夹
         *
         * note: 如果 shard-id == 0, 则以 interval 命名
         *       否则, 以 partition-id 命名
         * note: shard-tree 的 root 节点, partition-id == 0
         */
        static std::string VARIABLE_IS_NOT_USED partition(
                const std::string &dirname, 
                uint32_t shard_id, uint32_t partition_id,
                const gfs::interval_t &interval) {
            if (shard_id == 0) {
                return fmt::format("{}/partition{}-{}", shardtree(dirname, shard_id), interval.first, interval.second);
            } else {
                return fmt::format("{}/partition{}", shardtree(dirname, shard_id), partition_id);
            }
            //return fmt::format("{}/partition{}-{}", dirname, interval.first, interval.second);
        }

        static std::string VARIABLE_IS_NOT_USED sub_partition(
                const std::string &dirname, 
                uint32_t shard_id, uint32_t partition_id,
                const gfs::interval_t &interval, const gfs::EdgeTag_t tag) {
            return fmt::format("{}-{}", DIRNAME::partition(dirname, shard_id, partition_id, interval), tag);
        }

        /**
         * shard-tree的文件夹
         * shard-tree由多个partition组成
         *
         *      shard-id == 0 用以存储临时的 partition (bulkload 过程中 partition-id 未分配)
         *      存储 db 数据的 shard,  从 shard-id == 1 开始
         * TODO remove interval
         */
        static std::string VARIABLE_IS_NOT_USED shardtree(
                const std::string &dirname, 
                uint32_t shard_id,
                const gfs::interval_t interval=interval_t(0,0)) {
            return fmt::format("{}/shard{}", dirname, shard_id);
            return fmt::format("{}/shard{}-{}", dirname, interval.first, interval.second);
            // 若文件夹以 interval 的 start && end 标识,
            // 在新插入数据后导致 interval 扩展时, 需要把文件夹内所有文件路径都进行修正.
            // 因此改为只以 interval 的 start 标识
            return fmt::format("{}/shard{}", dirname, interval.first);
        }

        static
        std::string VARIABLE_IS_NOT_USED meta(const std::string &db_dir) {
            return fmt::format("{}/{}", db_dir, "meta");
        }

        /**
         * bulk-loading 过程中, 存放临时划分数据的文件夹
         */
        static std::string VARIABLE_IS_NOT_USED partition_tmp(
            const std::string &dirname) {
            return fmt::format("{}/parts/", dirname);
        }

        // 跟随 sub-partition 的边属性列文件夹
        // FILENAME is used in method, defined at bottom
        inline static std::string VARIABLE_IS_NOT_USED sub_partition_edge_columns(
                const std::string &dirname, 
                uint32_t shard_id, uint32_t partition_id,
                const gfs::interval_t &interval, const gfs::EdgeTag_t tag) {
            return fmt::format("{}/elist_col", DIRNAME::sub_partition(dirname, shard_id, partition_id, interval, tag));
        }

        // 跟随 sub-partition 的边属性列(block的文件夹)
        static std::string VARIABLE_IS_NOT_USED sub_partition_edge_columns_blocks(
                const std::string &dirname,
                uint32_t shard_id, uint32_t partition_id,
                const interval_t &interval, const EdgeTag_t tag, const std::string &colname) {
            return fmt::format("{}/{}", DIRNAME::sub_partition_edge_columns(dirname, shard_id, partition_id, interval, tag), colname);
        }

        /**
         * @brief id 映射存储的目录
         */
        static std::string VARIABLE_IS_NOT_USED id_mapping(const std::string &dirname) {
            return fmt::format("{}/id_mapping", dirname);
        }

        /**
         * 节点的属性存储文件夹
         */
        static std::string VARIABLE_IS_NOT_USED vertex_attr(
                            const std::string &dirname) {
            return fmt::format("{}/vdata", dirname);
        }

        /**
         * WAL 的默认存储目录
         */
        static std::string VARIABLE_IS_NOT_USED default_wal_dir(const std::string &dirname) {
            return fmt::format("{}/journal", dirname);
        }
    };

    class FILENAME {
    public:

        static std::string VARIABLE_IS_NOT_USED current_name(const std::string &meta_dirname) {
            return fmt::format("{}/CURRENT", meta_dirname);
        }

        static std::string VARIABLE_IS_NOT_USED intervals(const std::string &meta_dirname) {
            return fmt::format("{}/intervals", meta_dirname);
        }

        /**
         * Returns the number of vertices in a graph.
         */
        static inline
        std::string VARIABLE_IS_NOT_USED num_vertices(const std::string &meta_dirname) {
            return fmt::format("{}/numvertices", meta_dirname);
        }

        static std::string VARIABLE_IS_NOT_USED vertex_attr_conf(const std::string &meta_dirname) {
            return fmt::format("{}/vertex.attr.cnf", meta_dirname);
        }

        static std::string VARIABLE_IS_NOT_USED edge_attr_conf(const std::string &meta_dirname) {
            return fmt::format("{}/edge.attr.cnf", meta_dirname);
        }

        static std::string VARIABLE_IS_NOT_USED meta_journal(const std::string &meta_dirname) {
            return fmt::format("{}/journal", meta_dirname);
        }

        /**
         * Vertex status file
         * 图计算引擎, 用于迭代的节点状态.
         */
        template<typename VStatusType>
        static std::string VARIABLE_IS_NOT_USED vertex_status(const std::string &basefilename) {
            return fmt::format("{}vstatus.{}B.vout", basefilename, sizeof(VStatusType));
        }

        /**
         * 节点的属性存储文件
         */
        static std::string VARIABLE_IS_NOT_USED vertex_attr_data(
                const std::string &basefilename,
                const std::string &label, const std::string &colname) {
            return fmt::format("{}/prop.v.{}.{}", DIRNAME::vertex_attr(basefilename), label, colname);
        }

        /**
         * 节点的属性存储文件 (VarChar)
         */
        static std::string VARIABLE_IS_NOT_USED vertex_attr_varchar_data(
                const std::string &basefilename,
                const std::string &label, const std::string &colname) {
            return fmt::format("{}/prop.v.{}.kv", DIRNAME::vertex_attr(basefilename), label, colname);
        }

        /**
         * 节点的 degree 存储文件
         */
        static std::string VARIABLE_IS_NOT_USED degree(const std::string &basefilename) {
            return FILENAME::vertex_attr_data(basefilename, GFS_GLOBAL_LABEL, GFS_VERTEX_COLUMN_NAME_DEGREE);
        }

        /**
         * @brief 节点的 label-tag
         */
        static std::string VARIABLE_IS_NOT_USED vtag(const std::string &basefilename) {
            return FILENAME::vertex_attr_data(basefilename, GFS_GLOBAL_LABEL, GFS_VERTEX_COLUMN_NAME_TAG);
        }

        // sub-partition 的拓扑结构文件
        static std::string VARIABLE_IS_NOT_USED sub_partition_edgelist(
                const std::string &dirname, 
                uint32_t shard_id, uint32_t partition_id,
                const gfs::interval_t &interval, const gfs::EdgeTag_t tag) {
            return fmt::format("{}/elist", DIRNAME::sub_partition(dirname, shard_id, partition_id, interval, tag));
        }

        // 跟随 sub-partition 的dst索引
        static std::string VARIABLE_IS_NOT_USED sub_partition_dst_idx(
                const std::string &elist_filename) {
            return fmt::format("{}.dst.idx", elist_filename);
        }

        // 跟随 sub-partition 的src索引
        static std::string VARIABLE_IS_NOT_USED sub_partition_src_idx(
                const std::string &elist_filename) {
            return fmt::format("{}.src.idx", elist_filename);
        }

        // 跟随 sub-partition 的边属性列文件
        static VARIABLE_IS_NOT_USED std::string sub_partition_edge_column(
                const std::string &dirname, 
                uint32_t shard_id, uint32_t partition_id,
                const gfs::interval_t &interval, const gfs::EdgeTag_t tag,
                const std::string &colname) {
            return fmt::format("{}/{}",
                               DIRNAME::sub_partition_edge_columns(dirname,shard_id, partition_id, interval, tag), colname);
        }

        // 跟随 sub-partition 的边属性列(block的文件)
        static std::string VARIABLE_IS_NOT_USED sub_partition_edge_column_block(
                const std::string &block_dir, size_t block_id) {
            return fmt::format("{}/{}", block_dir, block_id);
        }

        /**
         * Edge status file
         * 图计算引擎, 用于迭代的边状态.
         */
        template<typename EStatusType>
        static std::string VARIABLE_IS_NOT_USED edge_status(const std::string &basefilename,
                uint32_t shard_id, uint32_t partition_id,
                const gfs::interval_t &interval, const gfs::EdgeTag_t tag, const std::string& statusname) {
            return sub_partition_edge_column(basefilename, shard_id, partition_id, interval, tag, statusname);
        }

        template<typename DataType>
        static size_t datanum_of_file(const std::string &filename, size_t& file_sz) {
            file_sz = PathUtils::getsize(filename);
            assert(file_sz % sizeof(DataType) == 0);
            return file_sz / sizeof(DataType);
        }

        /**
         * WAL 文件名
         */
        static std::string VARIABLE_IS_NOT_USED journal_name(const std::string &wal_dirname, uint64_t file_no) {
            return fmt::format("{}/journal.{:04d}", wal_dirname, file_no);
        }

        static
        bool parse_journal_name(const std::string &filename, const std::string &wal_dirname,
                uint64_t *const file_no) {
            assert(file_no != nullptr);
            Slice slice(filename);
            if (slice.starts_with(wal_dirname)) {
                slice.remove_prefix(wal_dirname.size());
                if (slice.starts_with("/")) { slice.remove_prefix(1); }
            }
            if (slice.starts_with("journal.")) {
                slice.remove_prefix(strlen("journal."));
                uint64_t no = StringUtils::ParseUint64(slice.ToString());
                *file_no = no;
                return true;
            } else {
                return false;
            }
        }
    };

    class ENGINE_IO {
    public:
        static const size_t MAX_BYTE_PER_IO = 1024UL*1024UL*1024UL;
        static bool read_entire(std::string filename, char* buf,
                size_t size_to_read) {
            int fd = read_open_fd(filename.c_str());
            //TODO do not use ioutils 
            preada<char>(fd, buf, size_to_read, 0); 
            return true;
        }

        static bool read_offset(std::string filename, char* buf,
                size_t offset_read, size_t size_to_read) {
            int fd = read_open_fd(filename.c_str());
            preada<char>(fd, buf, size_to_read, offset_read); 
            return true;
        }

        static bool write_entire(std::string filename, char* buf,
                size_t size_to_write) {
            int fd = write_open_fd(filename.c_str());
            pwritea<char>(fd, buf, size_to_write, 0);
            return true;
        }

        static bool write_offset(std::string filename, char* buf,
                size_t offset_write, size_t size_to_write) {
            int fd = write_open_fd(filename.c_str());
            pwritea<char>(fd, buf, size_to_write, offset_write);
            return true;
        }

        static int read_open_fd(const char* filename_cstr) {
#ifdef IO_TRACK
            GFS_LOG_DEBUG("read open {}", filename_cstr);
#endif
            int fd = open(filename_cstr, O_RDONLY);
            if (fd < 0) {
                GFS_LOG_DEBUG("err open [{}]", filename_cstr);
                exit(-1);
            } 
#ifdef IO_TRACK
            GFS_LOG_DEBUG("read open {} with fd {}", filename_cstr, fd);
#endif
            return fd;
        }

        static int write_open_fd(const char* filename_cstr) {
#ifdef IO_TRACK
            GFS_LOG_DEBUG("write open {}", filename_cstr);
#endif
            int fd = open(filename_cstr, O_WRONLY | O_CREAT, 0755);
            if (fd < 0) {
                GFS_LOG_DEBUG("err open [{}]", filename_cstr);
                exit(-1);
            } 
#ifdef IO_TRACK
            GFS_LOG_DEBUG("write open {} with fd {}", filename_cstr, fd);
#endif
            return fd;
        }

        static bool status_ftruncate(const char* filename_cstr, size_t file_size) {
            GFS_LOG_DEBUG("truncate: {} size={}", filename_cstr, file_size);
            checkarray_filesize<char>(filename_cstr, file_size); 
            return true;
        }
    };
}

#endif //STARKNOWLEDGEGRAPHDATABASE_SKGFILENAMES_HPP
