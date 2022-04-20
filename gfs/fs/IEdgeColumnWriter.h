#ifndef STARKNOWLEDGEGRAPHDATABASE_IEDGECOLUMNWRITER_H
#define STARKNOWLEDGEGRAPHDATABASE_IEDGECOLUMNWRITER_H

#include <memory>
#include <vector>
#include <string>
#include <util/ioutil.h>

#include "util/skgfilenames.h"

namespace skg {

    class IEdgeColumnPartitionWriter {
    public:
        explicit
        IEdgeColumnPartitionWriter(const ColumnDescriptor &desc)
                : m_value_size(desc.value_size()), m_id(desc.id())
        {}
        virtual ~IEdgeColumnPartitionWriter() = default;
        virtual Status Open() = 0;
        virtual Status Write(const void *) = 0;
        virtual Status Flush() = 0;
        virtual Status CreateSizeRecord() = 0;
        virtual Status CreateInitialBlocks(const idx_t num_edges) = 0;
        inline size_t value_size() const { return m_value_size; }
        inline int32_t id() const { return m_id; }
    protected:
        const size_t m_value_size;
        const int32_t m_id;
    public:
        // No copying allowed
        IEdgeColumnPartitionWriter(const IEdgeColumnPartitionWriter&) = delete;
        IEdgeColumnPartitionWriter& operator=(const IEdgeColumnPartitionWriter&) = delete;
    };

    class EdgeColumnBlocksPartitionWriter: public IEdgeColumnPartitionWriter {
    private:
        std::string m_dirname;
        size_t m_block_size;
        // 每个block中应该存多少条边的data
        size_t m_edge_data_per_block;
        // 当前正在生成的blockid
        size_t m_blockid;
        size_t m_total_bytes;

        char *m_buf;  // edge-data的buffer
        char *m_buf_prt;
        size_t m_num_block_edges; // 当前buffer中已缓存多少数据
    public:
        EdgeColumnBlocksPartitionWriter(
                const std::string &shardfile, const ColumnDescriptor &desc)
                : IEdgeColumnPartitionWriter(desc),
                  m_dirname(DIRNAME::dirname_shard_edge_column_block(shardfile, desc.colname())),
                  m_block_size(BASIC_BLOCK_SIZE),
                  m_edge_data_per_block(1), m_blockid(0),
                  m_total_bytes(0),
                  m_buf(nullptr), m_buf_prt(nullptr), m_num_block_edges(0) {
            while (m_block_size % value_size() != 0) ++m_block_size;
            m_edge_data_per_block = m_block_size / m_value_size;
            m_buf = new char[m_block_size];
            memset(m_buf, 0, m_block_size);
            this->Reset();
        }

        ~EdgeColumnBlocksPartitionWriter() override {
            assert(m_num_block_edges == 0);  // sanity check, buffer需要完全清空
            delete []m_buf;
            m_buf = nullptr;
            m_buf_prt = nullptr;
        }

        Status Open() override {
            PathUtils::CreateDirIfMissing(m_dirname);
            return Status::OK();
        }

        Status Write(const void *value) override {
            Status s;
            assert((1 + m_num_block_edges) * m_value_size <= m_block_size);
            memcpy(m_buf_prt, value, m_value_size);
            m_buf_prt += m_value_size;
            m_num_block_edges++;
            m_total_bytes += m_value_size;
            // flush while buffer is full
            if (m_num_block_edges == m_edge_data_per_block) {
                // flush current block to file
                s = this->Flush();
                if (!s.ok()) { return s; } // Can NOT flush block data
                // move to next block
                ++m_blockid;
            }
            return s;
        }

        Status Flush() override {
            const std::string blockname = DIRNAME::filename_shard_edge_column_block(m_dirname, m_blockid);
            FILE *f = fopen(blockname.c_str(), "wb");
            if (f == nullptr) {
                return Status::IOError(fmt::format(
                        "Can NOT create file: {}, error: {}({})",
                        blockname, strerror(errno), errno));
            }
            // check write 是否出错; Note 读取数据时, 需要保证以同样方式读取,
            // 原grapchi中以 write_compressed/read_compressed 的方式读取, 若不统一可能会导致读写错误
            const size_t bytes_write = fwrite(m_buf, 1, m_num_block_edges * m_value_size, f);
            fclose(f);
            if (bytes_write != m_num_block_edges * m_value_size) {
                return Status::IOError(fmt::format("Fail to flush block: {}, error: {}({})", blockname, strerror(errno), errno));
            }
            this->Reset();
            return Status::OK();
        }

        Status CreateSizeRecord() override {
            assert(m_num_block_edges == 0);
            const std::string colsize_name = DIRNAME::filename_shard_edge_column_size(m_dirname);
            FILE *f = fopen(colsize_name.c_str(), "wb");
            if (f == nullptr) {
                return Status::IOError(fmt::format("Fail to create file: {}", colsize_name));
            }
            const size_t bytes_write = fwrite(&m_total_bytes, 1, sizeof(m_total_bytes), f);
            fclose(f);
            if (bytes_write != sizeof(m_total_bytes)) {
                return Status::IOError(fmt::format("Fail to write column size: {}", colsize_name));
            }
            return Status::OK();
        }

        Status CreateInitialBlocks(const idx_t num_edges) override {
            Status s;
            size_t remain_edges = num_edges;
            while (true) {
                if (remain_edges >= m_edge_data_per_block) {
                    m_num_block_edges = m_edge_data_per_block;
                } else {
                    m_num_block_edges = remain_edges;
                }
                s = this->Flush();
                if (!s.ok()) { return s; }
                m_blockid++;
                if (remain_edges < m_edge_data_per_block) {
                    break;
                } else {
                    remain_edges -= m_edge_data_per_block;
                }
            }
            m_total_bytes = num_edges * m_value_size;
            s = this->CreateSizeRecord();
            if (!s.ok()) { return s; }
            return s;
        }

    private:
        inline void Reset() {
            m_num_block_edges = 0;
            m_buf_prt = m_buf;
        }
    public:
        EdgeColumnBlocksPartitionWriter(const EdgeColumnBlocksPartitionWriter&) = delete;
        EdgeColumnBlocksPartitionWriter& operator=(const EdgeColumnBlocksPartitionWriter&) = delete;
    };

    class EdgeColumnFragmentFileWriter: public IEdgeColumnPartitionWriter {
    private:
        std::string m_colsize_name;
        std::string m_filename;
        size_t m_total_bytes;
        FILE *m_f;
    public:
        EdgeColumnFragmentFileWriter(
                const std::string &shardfile, const ColumnDescriptor &desc)
                : IEdgeColumnPartitionWriter(desc),
                  m_colsize_name(DIRNAME::filename_shard_edge_column_size(
                        DIRNAME::dirname_shard_edge_columns(shardfile), desc.colname())),
                  m_filename(DIRNAME::filename_shard_edge_column(shardfile, desc.colname())),
                  m_total_bytes(0), m_f(nullptr) {
        }

        ~EdgeColumnFragmentFileWriter() override {
            if (m_f != nullptr) {
                fclose(m_f);
            }
        }

        Status Open() override {
            m_f = fopen(m_filename.c_str(), "wb");
            if (m_f == nullptr) {
                return Status::IOError(fmt::format(
                        "Can NOT create file: {}, error: {}({})", m_filename, strerror(errno), errno));
            }
            return Status::OK();
        }

        Status Write(const void *value) override {
            const size_t bytes_write = fwrite(value, 1, m_value_size, m_f);
            m_total_bytes += m_value_size;
            if (bytes_write != m_value_size) {
                return Status::IOError(fmt::format("Fail write value to: {}, error: {}({})",
                                                   m_filename, strerror(errno), errno));
            }
            return Status::OK();
        }

        Status Flush() override {
            int ret = fflush(m_f);
            if (ret == 0) {
                return Status::OK();
            } else {
                return Status::IOError(fmt::format(
                        "Fail to flush to file: {}, eror: {}({})",
                        m_filename, strerror(errno), errno));
            }
        }

        Status CreateSizeRecord() override {
            FILE *f = fopen(m_colsize_name.c_str(), "wb");
            if (f == nullptr) {
                return Status::IOError(fmt::format("Fail to create file: {}", m_colsize_name));
            }
            const size_t bytes_write = fwrite(&m_total_bytes, 1, sizeof(m_total_bytes), f);
            fclose(f);
            if (bytes_write != sizeof(m_total_bytes)) {
                return Status::IOError(fmt::format("Fail to write column size: {}", m_colsize_name));
            }
            return Status::OK();
        }

        Status CreateInitialBlocks(const idx_t num_edges) override {
            Status s;
            s = PathUtils::CreateFile(m_filename);
            if (!s.ok()) { return s; }
            m_total_bytes = num_edges * m_value_size;
            s = PathUtils::TruncateFile(m_filename, m_total_bytes);
            if (!s.ok()) { return s; }
            s = this->CreateSizeRecord();
            if (!s.ok()) { return s; }
            return s;
        }
    };
}

#endif //STARKNOWLEDGEGRAPHDATABASE_IEDGECOLUMN_H
