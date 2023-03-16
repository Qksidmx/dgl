#ifndef GFS_EDGELISTRAWREADER_H
#define GFS_EDGELISTRAWREADER_H

#include <sys/mman.h>

#include "edge_list_reader.h"

namespace gfs {

class EdgeListRawReader: public EdgeListReader {

public:
    explicit
    EdgeListRawReader(
            const std::string &basefile,
            uint32_t shard_id, uint32_t partition_id,
            const interval_t &interval, const EdgeTag_t tag=0)
            : m_filename(FILENAME::sub_partition_edgelist(basefile, shard_id, partition_id, interval, tag)), m_fd(-1), m_num_edges(0) {
    }

    ~EdgeListRawReader() {
        this->Close();
    }

    Status Open() {
        size_t file_size = PathUtils::getsize(m_filename);
        // 特殊处理, 0大小的文件只是占位, 不需要打开
        if (file_size == 0) { return Status::OK(); }
        m_fd = open(m_filename.c_str(), O_RDWR);
        if (m_fd < 0) {
            return Status::IOError(fmt::format("edges-partition file: `{}`, error: {}({})",
                                               m_filename, strerror(errno), errno));
        }
        m_num_edges = static_cast<idx_t>(file_size / sizeof(PersistentEdge));
        return Status::OK();
    }

    Status Flush() {
        return Status::OK();
    }

    void Close() {
        Flush();
        m_num_edges = 0;
        if (m_fd >= 0) {
            int iRet = close(m_fd);
            if (iRet != 0) {
                SKG_LOG_ERROR("error close, {}", strerror(errno));
            }
            m_fd = -1;
        }
    }

    const PersistentEdge &GetImmutableEdge(const idx_t idx, char *buf) const {
        preada(m_fd, buf, sizeof(PersistentEdge), idx * sizeof(PersistentEdge));
        return *reinterpret_cast<const PersistentEdge *>(buf);
    }

    PersistentEdge *GetMutableEdge(const idx_t idx, char *buf) {
        preada(m_fd, buf, sizeof(PersistentEdge), idx * sizeof(PersistentEdge));
        return reinterpret_cast<PersistentEdge *>(buf);
    }

    Status Set(const idx_t idx, const PersistentEdge *const pEdge) {
        assert(idx < m_num_edges);
        Status s = pwritea(m_fd, pEdge, sizeof(PersistentEdge), idx * sizeof(PersistentEdge));
        return s;
    }

    idx_t num_edges() const {
        return m_num_edges;
    }

    inline const std::string &filename() const {
        return m_filename;
    }
private:
    std::string m_filename;
    int m_fd;
    idx_t m_num_edges;
};
}

#endif //STARKNOWLEDGEGRAPHDATABASE_EDGELISTRAWREADER_H
