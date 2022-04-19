#ifndef STARKNOWLEDGEGRAPHDATABASE_IEDGECOLUMNFRAGMENT_H
#define STARKNOWLEDGEGRAPHDATABASE_IEDGECOLUMNFRAGMENT_H

#include <cassert>
#include <string>
#include <memory>

#include "util/status.h"
#include "fs/ColumnDescriptor.h"

#include "fs/EdgeListFileManager.h"
#include "util/ioutil.h"
#include "BlocksCacheManager.h"

namespace skg {
    class IEdgeColumnPartition {
    protected:
        int32_t m_id;
        // 列名
        std::string m_name;
        // 单个value大小
        const size_t m_value_bytes_size;
        const ColumnType m_type;
    public:
        explicit IEdgeColumnPartition(const ColumnDescriptor &desc)
                : m_id(desc.id()), m_name(desc.colname()),
                  m_value_bytes_size(desc.value_size()), m_type(desc.columnType()) {
        }

        virtual ~IEdgeColumnPartition() = default;

        virtual
        const std::string &name() const {
            assert(!m_name.empty());
            return m_name;
        }

        size_t value_size() const {
            return m_value_bytes_size;
        }

        int32_t id() const {
            return m_id;
        }

        bool isColumnGroup() const {
            return columnType() == ColumnType::GROUP;
        }

        virtual
        Status Create(const std::string &dirname,
                      uint32_t shard_id, uint32_t partition_id,
                      const interval_t &interval,
                      EdgeTag_t tag) = 0;

        virtual
        Status Open() = 0;

        virtual
        Status Flush() = 0;

        virtual
        Status Close() = 0;

        virtual
        Status Get(const idx_t, void *value) = 0;

        virtual
        Status Set(const idx_t idx, const void *value, const size_t nbytes) = 0;

        inline
        ColumnType columnType() const {
            return m_type;
        }

    public:
        // No copying allowed
        IEdgeColumnPartition(const IEdgeColumnPartition &) = delete;
        IEdgeColumnPartition &operator=(const IEdgeColumnPartition &) = delete;
    };

    using IEdgeColumnPartitionPtr = std::shared_ptr<IEdgeColumnPartition>;

    /**
     * 存储一列edgedata中, 属于某个shard的fragment
     * 文件存储形式上, 存储到某个文件夹下, 由N个block组成
     */
    class EdgeColumnBlocksPartition : public IEdgeColumnPartition {
    private:
        // 存储的目录名
        std::string m_dirname;
#if 0
        // 该属性列总大小, 作用未明, 暂时为兼容图计算引擎
        size_t m_total_bytes;
#endif
        // 该属性列, 每个block的文件大小 (未压缩)
        size_t m_block_bytes_size;
        // 每个block存储多少个value
        size_t m_num_value_per_block;
        // 属于哪一个interval
        interval_t m_interval;
    public:
        /* note 只允许 ColumnDescriptorUtils 创建类的对象 */
        friend class ColumnDescriptorUtils;

    protected:
        explicit EdgeColumnBlocksPartition(const ColumnDescriptor &desc)
                : IEdgeColumnPartition(desc), m_dirname(),
#if 0
                  m_total_bytes(0),
#endif
                  m_block_bytes_size(BASIC_BLOCK_SIZE),
                  m_num_value_per_block(1),
                  m_interval(0, 0) {
            while (m_block_bytes_size % m_value_bytes_size != 0) ++m_block_bytes_size;
        }

    public:
        ~EdgeColumnBlocksPartition() override = default;

        Status Create(const std::string &dirname,
                      uint32_t shard_id, uint32_t partition_id,
                      const interval_t &interval,
                      EdgeTag_t tag) override {
            m_interval = interval;
            m_dirname = DIRNAME::sub_partition_edge_columns_blocks(
                    dirname, shard_id, partition_id, interval, tag,
                    name());
#if 0
            const std::string colsize_name = filename_shard_edge_column_size(m_dirname);
            FILE *f = fopen(colsize_name.c_str(), "rb");
            if (f == nullptr) {
                return Status::FileNotFound(fmt::format("Fail to open file: {}", colsize_name));
            }
            size_t bytes_read = fread(&m_total_bytes, 1, sizeof(m_total_bytes), f);
                if (bytes_read != sizeof(m_total_bytes)) {
                    fclose(f);
                    return Status::IOError(fmt::format("Fail to read column size:", colsize_name));
                }
            fclose(f);
#endif
            m_num_value_per_block = m_block_bytes_size / m_value_bytes_size;
            return Status::OK();
        }

        Status Open() override {
            return Status::OK();
        }

        Status Flush() override {
            return BlocksCacheManager::GetInstance()->Flush(m_interval);
        }

        Status Close() override {
            return Status::OK();
        }

        Status Get(const idx_t idx, void *value) override {
            const size_t blockid = idx / m_num_value_per_block;
            const size_t offset = idx % m_num_value_per_block;
            const std::string blockname = FILENAME::sub_partition_edge_column_block(m_dirname, blockid);
            // 通过缓存获取block
            std::shared_ptr<CachedBlock> block;
            Status s = BlocksCacheManager::GetInstance()->Get(m_interval, blockname, &block);
            if (!s.ok()) { return s; }
            memcpy(value, block->data + offset * m_value_bytes_size, m_value_bytes_size);
            return s;
        }

        Status Set(const idx_t idx, const void *value, const size_t nbytes) override {
            const size_t blockid = idx / m_num_value_per_block;
            const size_t offset = idx % m_num_value_per_block;
            const std::string blockname = FILENAME::sub_partition_edge_column_block(m_dirname, blockid);
            // 通过缓存获取block
            std::shared_ptr<CachedBlock> block;
            Status s = BlocksCacheManager::GetInstance()->Get(m_interval, blockname, &block);
            if (!s.ok()) { return s; }
            block->isDirty = true;
            memcpy(block->data + offset * m_value_bytes_size, value, std::min(m_value_bytes_size, nbytes));
            return Status::OK();
        }

    };

    /**
     * 存储一列edgedata中, 属于某个shard的fragment
     * 文件存储形式上, 存储到某个文件夹下, 单一文件
     */
    class EdgeColumnFilePartition : public IEdgeColumnPartition {
    private:
        // 存储的文件名
        std::string m_filename;
        // 文件操作句柄
        FILE *m_f;
        // 该属性列, 每个block的文件大小 (未压缩)
        size_t m_block_bytes_size;
        // 每个block存储多少个value
        size_t m_num_value_per_block;
        // 属于哪一个interval
        interval_t m_interval;
    public:
        /* note 只允许 ColumnDescriptorUtils 创建类的对象 */
        friend class ColumnDescriptorUtils;

    protected:
        explicit EdgeColumnFilePartition(const ColumnDescriptor &desc)
                : IEdgeColumnPartition(desc),
                  m_filename(), m_f(nullptr),
                  m_block_bytes_size(BASIC_BLOCK_SIZE),
                  m_num_value_per_block(1),
                  m_interval(0, 0) {
            while (m_block_bytes_size % m_value_bytes_size != 0) ++m_block_bytes_size;
        }

    public:
        ~EdgeColumnFilePartition() override {
            if (m_f != nullptr) {
                fclose(m_f);
            }
        }

        Status Create(const std::string &dirname,
                      uint32_t shard_id, uint32_t partition_id,
                      const interval_t &interval,
                      EdgeTag_t tag) override {
            m_interval = interval;
            m_filename = FILENAME::sub_partition_edge_column(dirname, shard_id, partition_id, interval, tag, name());
            m_f = fopen(m_filename.c_str(), "r+");
            if (m_f == nullptr) {
                return Status::IOError(fmt::format("Can NOT open file: {}", m_filename));
            }
            m_num_value_per_block = m_block_bytes_size / m_value_bytes_size;
            return Status::OK();
        }

        Status Open() override {
            return Status::OK();
        }

        Status Flush() override {
            return BlocksCacheManager2::GetInstance()->Flush(m_interval);
        }

        Status Close() override {
            return Status::OK();
        }

        Status Get(const idx_t idx, void *value) override {
            const size_t blockid = idx / m_num_value_per_block;
            const size_t offset = idx % m_num_value_per_block;
            const std::string blockname = FILENAME::sub_partition_edge_column_block(m_filename, blockid);
            // 通过缓存获取block
            std::shared_ptr<CachedBlock2> block;
            // 根据 blockname 获取block, 如果不在缓存中, 从 文件句柄 `m_f` 的 blockid 开始的地方, 读入该 block 到缓存中存储
            // TODO 如果是最后一个block, blockid * m_value_bytes_size + m_block_bytes_size
            Status s = BlocksCacheManager2::GetInstance()->Get(m_interval, blockname,
                                           m_f, blockid * m_block_bytes_size, m_block_bytes_size, &block);
            if (!s.ok()) { return s; }
            memcpy(value, block->data + offset * m_value_bytes_size, m_value_bytes_size);
            return s;
        }

        Status Set(const idx_t idx, const void *value, const size_t nbytes) override {
            const size_t blockid = idx / m_num_value_per_block;
            const size_t offset = idx % m_num_value_per_block;
            const std::string blockname = FILENAME::sub_partition_edge_column_block(m_filename, blockid);
            // 通过缓存获取block
            std::shared_ptr<CachedBlock2> block;
            // 根据 blockname 获取block, 如果不在缓存中, 从 文件句柄 `m_f` 的 blockid 开始的地方, 读入该 block 到缓存中存储
            // TODO 如果是最后一个block, blockid * m_value_bytes_size + m_block_bytes_size
            Status s = BlocksCacheManager2::GetInstance()->Get(m_interval, blockname,
                                           m_f, blockid * m_block_bytes_size, m_block_bytes_size, &block);
            if (!s.ok()) { return s; }
            block->isDirty = true;
            memcpy(block->data + offset * m_value_bytes_size, value, std::min(m_value_bytes_size, nbytes));
            return Status::OK();
        }

    };

    class EdgeColumnMMappedFilePartition : public IEdgeColumnPartition {
    protected:
        // 存储的文件名
        std::string m_filename;
        // 属于哪一个interval
        interval_t m_interval;
        char *m_mapped_buf;
        size_t m_mapped_size;
    public:
        /* note 只允许 ColumnDescriptorUtils 创建类的对象 */
        friend class ColumnDescriptorUtils;

    protected:
        explicit EdgeColumnMMappedFilePartition(const ColumnDescriptor &desc)
                : IEdgeColumnPartition(desc),
                  m_filename(),
                  m_interval(0, 0),
                  m_mapped_buf(nullptr), m_mapped_size(0) {
        }

    public:
        ~EdgeColumnMMappedFilePartition() override {
#if 0
            if (m_f >= 0) {
                close(m_f);
            }
#else
            this->Close();
#endif
        }

        Status Create(const std::string &dirname,
                      uint32_t shard_id, uint32_t partition_id,
                      const interval_t &interval,
                      EdgeTag_t tag) override {
            m_interval = interval;
            m_filename = FILENAME::sub_partition_edge_column(dirname, shard_id, partition_id, interval, tag, name());
            return this->Open();
        }

        Status Open() override {
            m_mapped_size = PathUtils::getsize(m_filename);
            // 特殊处理, 0大小的文件只是占位, 不需要mmap到内存中
            if (m_mapped_size == 0) { return Status::OK(); }
            int m_f = open(m_filename.c_str(), O_RDWR);
            if (m_f < 0) {
                return Status::IOError(fmt::format("Can NOT open edge-property file: `{}`, error: {}({})",
                                                   m_filename, strerror(errno), errno));
            }
            m_mapped_buf = static_cast<char *>(
                    mmap(nullptr, m_mapped_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                         m_f, 0));
            close(m_f);
            if (m_mapped_buf == MAP_FAILED) {
                return Status::IOError(fmt::format("Can NOT load {}, error: {}({})",
                                                   m_filename, strerror(errno), errno));
            }
            return Status::OK();
        }

        Status Flush() override {
            if (m_mapped_buf != nullptr && m_mapped_buf != MAP_FAILED) {
                int iRet = msync(m_mapped_buf, m_mapped_size, MS_SYNC);
                if (iRet != 0) {
                    SKG_LOG_ERROR("error msync, {}", strerror(errno));
                    return Status::IOError(fmt::format("error msync, {}", strerror(errno)));
                }
            }
            return Status::OK();
        }

        Status Close() override {
            if (m_mapped_buf != nullptr && m_mapped_buf != MAP_FAILED) {
                int iRet = msync(m_mapped_buf, m_mapped_size, MS_SYNC);
                if (iRet != 0) {
                    SKG_LOG_ERROR("error msync, {}", strerror(errno));
                }
                munmap(m_mapped_buf, m_mapped_size);
            }
            return Status::OK();
        }

        Status Get(const idx_t idx, void *value) override {
            const size_t offset = idx * m_value_bytes_size;
            if (offset + m_value_bytes_size > m_mapped_size) {
                return Status::IOError(
                        fmt::format("Error to read {}bytes data from {}:{}", m_value_bytes_size, m_filename, offset));
            }
            memcpy(value, m_mapped_buf + offset, m_value_bytes_size);
            return Status::OK();
        }

        Status Set(const idx_t idx, const void *value, const size_t nbytes) override {
            const size_t offset = idx * m_value_bytes_size;
            if (offset + m_value_bytes_size > m_mapped_size) {
                return Status::IOError(
                        fmt::format("Error to write {}bytes data to {}:{}", m_value_bytes_size, m_filename, offset));
            }
            if (nbytes < m_value_bytes_size) {
                // 存字符串, 最终需要有'\0'结束符, 先把区域清零 TODO 字符串存取都通过 VarChar 进行存储, 这里可去除覆盖值为 '\0' 的操作
                memset(m_mapped_buf + offset, 0, m_value_bytes_size);
            }
            memcpy(m_mapped_buf + offset, value, std::min(m_value_bytes_size, nbytes));
            return Status::OK();
        }

    };

    // Forward declaration
    class EdgeColumnGroupMMappedFilePartitionView;
    using EdgeColumnGroupMMappedFilePartitionViewPtr = std::shared_ptr<EdgeColumnGroupMMappedFilePartitionView>;

    class EdgeColumnGroupMMappedFilePartition : public EdgeColumnMMappedFilePartition {
        friend class ColumnDescriptorUtils;
        friend class EdgeColumnGroupMMappedFilePartitionView;

    private:
        std::vector<EdgeColumnGroupMMappedFilePartitionViewPtr> m_cols;
        const size_t m_cols_size;
    protected:

        explicit EdgeColumnGroupMMappedFilePartition(
                const ColumnDescriptor &columnGroup):
                EdgeColumnMMappedFilePartition(columnGroup), m_cols(), m_cols_size(columnGroup.m_subConfig.size()) {
            size_t offset = 0;
            for (const auto &subCol : columnGroup) {
                m_cols.push_back(std::make_shared<EdgeColumnGroupMMappedFilePartitionView>(
                        subCol, this
                ));
                offset += subCol.value_size();
            }
        }

    public:
        ~EdgeColumnGroupMMappedFilePartition() {
            // TODO free cols view
        }

        size_t GetNumCols() const {
            return m_cols_size;
        }

        EdgeColumnGroupMMappedFilePartitionViewPtr GetCols(size_t i) {
            return m_cols[i];
        }
    };

    class EdgeColumnGroupMMappedFilePartitionView : public IEdgeColumnPartition {
        size_t m_offset;
        EdgeColumnGroupMMappedFilePartition *m_parent;
    public:
        EdgeColumnGroupMMappedFilePartitionView(
                const ColumnDescriptor &desc,
                EdgeColumnGroupMMappedFilePartition *parent)
                : IEdgeColumnPartition(desc), m_offset(desc.offset()), m_parent(parent) {
        }

        Status Get(const idx_t idx, void *value) override {
            const size_t offset = idx * m_parent->value_size() + m_offset;
            if (offset + m_value_bytes_size > m_parent->m_mapped_size) {
                return Status::IOError(
                        fmt::format("Error to read {}bytes data from {}:{}",
                                    m_value_bytes_size, m_parent->m_filename,
                                    offset));
            }
            memcpy(value, m_parent->m_mapped_buf + offset, m_value_bytes_size);
            return Status::OK();
        }

        Status Set(const idx_t idx, const void *value, const size_t nbytes) override {
            const size_t offset = idx * m_parent->value_size() + m_offset;
            if (offset + m_value_bytes_size > m_parent->m_mapped_size) {
                return Status::IOError(
                        fmt::format("Error to write {}bytes data to {}:{}",
                                    m_value_bytes_size, m_parent->m_filename,
                                    offset));
            }
            memcpy(m_parent->m_mapped_buf + offset, value, std::min(m_value_bytes_size, nbytes));
            return Status::OK();
        }

        Status Create(const std::string &dirname,
                      uint32_t shard_id, uint32_t partition_id,
                      const interval_t &interval,
                      EdgeTag_t tag) override {
            return Status::InvalidArgument();
        }

        Status Open() override {
            return Status::OK();
        }

        Status Close() override {
            return Status::OK();
        }

        Status Flush() override {
            return Status::OK();
        }
    };
}
#endif //STARKNOWLEDGEGRAPHDATABASE_IEDGECOLUMNFRAGMENT_H
