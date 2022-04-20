#ifndef STARKNOWLEDGEGRAPHDATABASE_SRCIDXREADER_H
#define STARKNOWLEDGEGRAPHDATABASE_SRCIDXREADER_H

#include <string>
#include <sys/mman.h>

#include "util/status.h"

#include "env/env.h"
#include "util/pathutils.h"
#include "util/EliasGammaSeq.h"
#include "util/EliasGammaSeqSerialization.h"

namespace skg {
class IndexReader {
public:

    virtual ~IndexReader() {
    }

    virtual Status Open() = 0;

    virtual void Close() = 0;

    virtual std::pair<idx_t, idx_t> GetOutIdxRange(const vid_t src) const = 0;
    virtual idx_t GetFirstInIndex(const vid_t dst) const = 0;
    virtual const std::string& filename() const = 0;
};

struct ValueIndex {
    vid_t value;
    idx_t idx;

    bool operator<(const ValueIndex &rhs) const {
        return value < rhs.value;
    }
    bool operator<(const vid_t &id) const {
        return value < id;
    }
};

class IndexMmapReader : public IndexReader {
public:
    explicit
    IndexMmapReader(const std::string &filename)
            : m_filename(filename),
              m_mapped_size(0), m_mapped_indices(nullptr),
              m_num_indices(0) {
    }

    ~IndexMmapReader() override {
        this->Close();
    }

    Status Open() override {
        m_mapped_size = PathUtils::getsize(m_filename);
        // 特殊处理, 0大小的文件只是占位, 不需要mmap到内存中
        if (m_mapped_size == 0) { return Status::OK(); }
        int fd = open(m_filename.c_str(), O_RDWR);
        if (fd < 0) {
            return Status::IOError(fmt::format("index file: `{}`, error: {}({})",
                                               m_filename, strerror(errno), errno));
        }
        m_mapped_indices = static_cast<ValueIndex *>(
                mmap(nullptr, m_mapped_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
        close(fd);
        if (m_mapped_indices == MAP_FAILED) {
            return Status::IOError(fmt::format("Can NOT load {}, error: {}({})",
                                               m_filename, strerror(errno), errno));
        }
        m_num_indices = static_cast<idx_t>(m_mapped_size / sizeof(ValueIndex));

        return Status::OK();
    }

    void Close() override {
        if (m_mapped_indices != nullptr && m_mapped_indices != MAP_FAILED) {
            munmap(m_mapped_indices, m_mapped_size);
            m_mapped_size = 0;
            m_mapped_indices = nullptr;
            m_num_indices = 0;
        }
    }

    std::pair<idx_t, idx_t> GetOutIdxRange(const vid_t src) const {
        auto src_iter = std::lower_bound(m_mapped_indices, indices_end(), src);
        if (src_iter == indices_end()) {
            return std::make_pair(INDEX_NOT_EXIST, INDEX_NOT_EXIST);
        } else {
            auto next_iter = src_iter + 1;
            if (src_iter->value != src) {
                // not found
                return std::make_pair(INDEX_NOT_EXIST, INDEX_NOT_EXIST);
            } else {
                // found
                if (next_iter == indices_end()) {
                    return std::make_pair(src_iter->idx, INDEX_NOT_EXIST);
                } else {
                    return std::make_pair(src_iter->idx, next_iter->idx);
                }
            }
        }
    }

    idx_t GetFirstInIndex(const vid_t dst) const {
        auto dst_iter = std::lower_bound(m_mapped_indices, indices_end(), dst);
        if (dst_iter == indices_end()) {
            return INDEX_NOT_EXIST; // 不存在
        } else {
            if (dst_iter->value != dst) {
                // not found
                return INDEX_NOT_EXIST;
            } else {
                // found
                return dst_iter->idx;
            }
        }
    }

    inline const std::string& filename() const {
        return m_filename;
    }
private:
    inline
    ValueIndex * const indices_end() const {
        return m_mapped_indices + m_num_indices;
    }
private:
    std::string m_filename;
    size_t m_mapped_size;
    ValueIndex *m_mapped_indices;
    idx_t m_num_indices;
};

class IndexRawReader : public IndexReader {
public:
    explicit
    IndexRawReader(const std::string &filename)
            : m_filename(filename),
              m_fd(-1), m_num_indices(0) {
    }

    ~IndexRawReader() override {
        this->Close();
    }

    Status Open() override {
        size_t file_size = PathUtils::getsize(m_filename);
        // 特殊处理, 0大小的文件只是占位, 不需要mmap到内存中
        if (file_size == 0) { return Status::OK(); }
        m_fd = open(m_filename.c_str(), O_RDWR);
        if (m_fd < 0) {
            return Status::IOError(fmt::format("index file: `{}`, error: {}({})",
                                               m_filename, strerror(errno), errno));
        }
        m_num_indices = static_cast<idx_t>(file_size / sizeof(ValueIndex));
        return Status::OK();
    }

    void Close() override {
        m_num_indices = 0;
        if (m_fd >= 0) {
            close(m_fd);
            m_fd = -1;
        }
    }

    std::pair<idx_t, idx_t> GetOutIdxRange(const vid_t src) const {
        const idx_t src_idx = BinFindIdx(src);
        if (src_idx == INDEX_NOT_EXIST) {
            return std::make_pair(INDEX_NOT_EXIST, INDEX_NOT_EXIST);
        } else {
            Status s;
            ValueIndex src_iter, next_iter;
            s = Read(src_idx, &src_iter);
            if (src_iter.value != src) {
                // not found
                return std::make_pair(INDEX_NOT_EXIST, INDEX_NOT_EXIST);
            } else {
                // found
                if (src_idx + 1 == m_num_indices) {
                    return std::make_pair(src_iter.idx, INDEX_NOT_EXIST);
                } else {
                    s = Read(src_idx + 1, &next_iter);
                    return std::make_pair(src_iter.idx, next_iter.idx);
                }
            }
        }
    }

    idx_t GetFirstInIndex(const vid_t dst) const {
        const idx_t dst_idx = BinFindIdx(dst);
        if (dst_idx == INDEX_NOT_EXIST) {
            return INDEX_NOT_EXIST; // 不存在
        } else {
            Status s;
            ValueIndex dst_iter;
            s = Read(dst_idx, &dst_iter);
            if (dst_iter.value != dst) {
                // not found
                return INDEX_NOT_EXIST;
            } else {
                // found
                return dst_iter.idx;
            }
        }
    }

public:
    inline const std::string& filename() const {
        return m_filename;
    }

private:
    Status Read(idx_t idx, ValueIndex *index_st) const {
        assert(m_num_indices == 0 || idx < m_num_indices);
        Status s = preada(m_fd, index_st, sizeof(ValueIndex), idx * sizeof(ValueIndex));
        return s;
    }
    idx_t BinFindIdx(const vid_t val) const {
        if (m_num_indices == 0) { return INDEX_NOT_EXIST; }
        Status s;
        int32_t low = 0, high = m_num_indices - 1;
        int32_t mid = INDEX_NOT_EXIST;
        ValueIndex index_st;
        while (low <= high) {
            mid = (low + high) / 2;
            s = Read(mid, &index_st);
            if (val == index_st.value) {
                return mid;
            } else if (val > index_st.value) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return INDEX_NOT_EXIST;
    }
private:
    std::string m_filename;
    int m_fd;
    idx_t m_num_indices;
};

/*
class IndexEliasGammaReader : public IndexReader {
public:
    explicit
    IndexEliasGammaReader(const std::string &filename)
            : m_filename(filename) {
    }

    ~IndexEliasGammaReader() {
        this->Close();
    }

    Status Open() override {
        const size_t file_size = PathUtils::getsize(m_filename);
        // 特殊处理, 0大小的文件只是占位, 不需要加载
        if (file_size == 0) { return Status::OK(); }
        const std::string eg_v_filename = m_filename + ".eg.v";
        const std::string eg_off_filename = m_filename + ".eg.offs";
        {// try to load elias-gamma-compress-result
            Status load_status;
            bool parse_success;
            {
                std::string pb_eg_v;
                load_status = ReadFileToString(Env::Default(), eg_v_filename, &pb_eg_v);
                if (load_status.ok()) {
                    parse_success = EliasGammaSeqSerialization::Decode(pb_eg_v, &m_vertices_seq);
                    if (!parse_success) { load_status = Status::InvalidArgument(); }
                }// else load failed
            }
            if (load_status.ok()) {
                std::string pb_eg_off;
                load_status = ReadFileToString(Env::Default(), eg_off_filename, &pb_eg_off);
                if (load_status.ok()) {
                    parse_success = EliasGammaSeqSerialization::Decode(pb_eg_off, &m_offset_seq);
                    if (parse_success) {
                        SKG_LOG_DEBUG("Load index from `{}', `{}'", eg_v_filename, eg_off_filename);
                        return load_status;
                    }
                    // else load failed
                }// else load failed
            }
        }
        int fd = open(m_filename.c_str(), O_RDWR);
        if (fd < 0) {
            return Status::IOError(fmt::format("index file: `{}`, error: {}({})",
                                               m_filename, strerror(errno), errno));
        }
        const size_t num_indices = static_cast<idx_t>(file_size / sizeof(ValueIndex));
        std::vector<uint32_t> vertices_seq(num_indices);
        std::vector<uint32_t> offsets_seq(num_indices);
        Status s;
        do {
            ValueIndex idx;
            for (size_t i = 0; i < num_indices; ++i) {
                s = preada(fd, &idx, sizeof(ValueIndex), i * sizeof(ValueIndex));
                if (!s.ok()) { break; }
                vertices_seq[i] = idx.value;
                offsets_seq[i] = idx.idx;
            }
            s = m_vertices_seq.EncodeSeq(vertices_seq);
            if (!s.ok()) { break; }
            s = m_offset_seq.EncodeSeq(offsets_seq);
            if (!s.ok()) { break; }
            SKG_LOG_DEBUG("elias-gamma `{}', compress ratio v:{:.2f}, off:{:.2f}",
                          m_filename,
                          m_vertices_seq.CompressionRatio(), m_offset_seq.CompressionRatio());
            {// save elias-gamma-compress-result
                const std::string pb_vertices = EliasGammaSeqSerialization::Encode(m_vertices_seq);
                Status saved_status = WriteStringToFile(Env::Default(), pb_vertices, eg_v_filename);
                if (saved_status.ok()) {
                    const std::string pb_offsets = EliasGammaSeqSerialization::Encode(m_offset_seq);
                    saved_status = WriteStringToFile(Env::Default(), pb_offsets, m_filename + ".eg.offs");
                    if (saved_status.ok()) {
                        SKG_LOG_DEBUG("save elias-gamma-compressed to `{}', `{}'", eg_v_filename, eg_off_filename);
                    } else {
                        SKG_LOG_WARNING("save elias-gamma-compressed to `{}' error.", eg_off_filename);
                    }
                } else {
                    SKG_LOG_WARNING("save elias-gamma-compressed to `{}' error.", eg_v_filename);
                }
            }
        } while (false);
        close(fd);
        return s;
    }

    void Close() override {
    }

    std::pair<idx_t, idx_t> GetOutIdxRange(const vid_t src) const {
        vid_t pos = m_vertices_seq.GetIndex(src);
        if (pos == static_cast<vid_t>(-1)) {
            return std::make_pair(INDEX_NOT_EXIST, INDEX_NOT_EXIST);
        }
        std::pair<idx_t, idx_t> result = m_offset_seq.GetTwo(pos);
        if (result.first == static_cast<vid_t>(-1)) { result.first = INDEX_NOT_EXIST; }
        if (result.second == static_cast<vid_t>(-1)) { result.second = INDEX_NOT_EXIST; }
        return result;
    }

    idx_t GetFirstInIndex(const vid_t dst) const {
        vid_t pos = m_vertices_seq.GetIndex(dst);
        if (pos == static_cast<vid_t>(-1)) {
            return INDEX_NOT_EXIST;
        }
        vid_t index_value = m_offset_seq.Get(pos);
        if (index_value != static_cast<vid_t>(-1)) {
            return index_value;
        } else {
            return INDEX_NOT_EXIST;
        }
    }

    inline const std::string& filename() const {
        return m_filename;
    }
private:
    std::string m_filename;
    EliasGammaSeq m_vertices_seq;
    EliasGammaSeq m_offset_seq;
};*/

}

#endif //STARKNOWLEDGEGRAPHDATABASE_SRCIDXREADER_H
