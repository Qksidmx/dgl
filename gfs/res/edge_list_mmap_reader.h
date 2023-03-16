#ifndef STARKNOWLEDGEGRAPHDATABASE_EDGELISTMMAPREADER_H
#define STARKNOWLEDGEGRAPHDATABASE_EDGELISTMMAPREADER_H

#include <sys/mman.h>

#include "edge_list_reader.h"
#include "options.h"

namespace gfs {

class EdgeListMmapReader: public EdgeListReader {

public:
    explicit
    EdgeListMmapReader(
            const std::string &basefile,
            uint32_t shard_id, uint32_t partition_id,
            const interval_t &interval, const EdgeTag_t tag, const Options &options)
            : m_filename(FILENAME::sub_partition_edgelist(basefile, shard_id, partition_id, interval, tag)),
              m_mapped_size(0), m_mapped_edges(nullptr), m_flags(0), m_num_edges(0) {
        if (options.use_mmap_locked) { m_flags |= MMAP_USE_LOCKED; }
        if (options.use_mmap_populate) { m_flags |= MMAP_USE_POPULATE; }
    }

    ~EdgeListMmapReader() override {
        this->Close();
    }

    Status Open() override {
        m_mapped_size = PathUtils::getsize(m_filename);
        // 特殊处理, 0大小的文件只是占位, 不需要mmap到内存中
        if (m_mapped_size == 0) { return Status::OK(); }
        int fd = open(m_filename.c_str(), O_RDWR);
        if (fd < 0) {
            return Status::IOError(fmt::format("edges-partition file: `{}`, error: {}({})",
                                               m_filename, strerror(errno), errno));
        }
        // 设置 mmap 的标志位
        const int mmap_default_flags = MAP_SHARED;
        int mmap_flags = mmap_default_flags;
        if (m_flags & MMAP_USE_LOCKED) { mmap_flags |= MAP_FIXED; }
        if (m_flags & MMAP_USE_POPULATE) { /*mmap_flags |= MAP_POPULATE;*/ }
        m_mapped_edges = static_cast<PersistentEdge *>(mmap(
                nullptr, m_mapped_size, PROT_READ | PROT_WRITE, mmap_flags, fd, 0));
        if (m_mapped_edges == MAP_FAILED) {
            GFS_LOG_WARNING("Can NOT mmap {}, error: {}({}), falling back to default-flags",
                            m_filename, strerror(errno), errno);
            // 回退到默认的标志位再尝试
            m_mapped_edges = static_cast<PersistentEdge *>(mmap(
                    nullptr, m_mapped_size, PROT_READ | PROT_WRITE, mmap_default_flags, fd, 0));
            if (m_mapped_edges == MAP_FAILED) {
                close(fd);  // free file descriptor
                return Status::IOError(fmt::format("Can NOT load {}, error: {}({})",
                                                   m_filename, strerror(errno), errno));
            }
        }
        m_num_edges = static_cast<idx_t>(m_mapped_size / sizeof(PersistentEdge));
        m_flags &= ~MODIFIED; // 清零脏标志位
        close(fd);  // free file descriptor
        return Status::OK();
    }

    Status Flush() override {
        if (m_mapped_edges != nullptr && m_mapped_edges != MAP_FAILED) {
            if (m_flags & MODIFIED) {
                int iRet = msync(m_mapped_edges, m_mapped_size, MS_SYNC);
                if (iRet != 0) {
                    GFS_LOG_ERROR("error msync, {}", strerror(errno));
                    return Status::IOError(fmt::format("error msync, {}", strerror(errno)));
                }
                m_flags &= ~MODIFIED; // 清零脏标志位
            }
        }
        return Status::OK();
    }

    void Close() override {
        int iRet = 0;
        if (m_mapped_edges != nullptr && m_mapped_edges != MAP_FAILED) {
            Flush();
            iRet = munmap(m_mapped_edges, m_mapped_size);
            if (iRet != 0) {
                GFS_LOG_ERROR("error munmap, {}", strerror(errno));
            }
            m_mapped_size = 0;
            m_mapped_edges = nullptr;
            m_flags &= ~MODIFIED; // 清零脏标志位
            m_num_edges = 0;
        }
    }

    const PersistentEdge &GetImmutableEdge(const idx_t idx, char * /* buf */) const {
        return m_mapped_edges[idx];
    }


    PersistentEdge *GetMutableEdge(const idx_t idx, char * /* buf */) {
        m_flags |= MODIFIED;
        return &m_mapped_edges[idx];
    }

    Status Set(const idx_t idx, const PersistentEdge *const pEdge) {
        assert(pEdge >= m_mapped_edges && pEdge <= m_mapped_edges + m_mapped_size);
        m_flags |= MODIFIED;
        return Status::OK();
    }

    idx_t num_edges() const {
        return m_num_edges;
    }

    inline const std::string &filename() const {
        return m_filename;
    }
private:
    std::string m_filename;
    size_t m_mapped_size;
    PersistentEdge *m_mapped_edges;
    enum Flags {
        // 标记是否有修改
        MODIFIED = 0x01,
        // MAP_POPULATE 标志位
        MMAP_USE_POPULATE = 0x02,
        // MAP_LOCKED 标志位
        MMAP_USE_LOCKED = 0x04,
    };
    uint32_t m_flags;
    idx_t m_num_edges;
};
}

#endif //STARKNOWLEDGEGRAPHDATABASE_EDGELISTMMAPREADER_H
