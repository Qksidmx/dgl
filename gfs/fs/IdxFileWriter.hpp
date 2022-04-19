#ifndef STARKNOWLEDGEGRAPH_DSTIDXFILEMANAGER_H
#define STARKNOWLEDGEGRAPH_DSTIDXFILEMANAGER_H


#include <cstdio>
#include <string>

#include "util/skgfilenames.h"

namespace skg {

class IndexFileWriter{
public:
    explicit
    IndexFileWriter(const std::string &idxfilename)
            : m_filename(idxfilename), f(nullptr) {
    }

    Status Open() {
        this->f = fopen(this->m_filename.c_str(), "wb");
        if (this->f == nullptr) {
            return Status::IOError(fmt::format("Create dst index: {}, err: {}({})", this->m_filename, strerror(errno), errno));
        }
        return Status::OK();
    }

    ~IndexFileWriter() {
        if (f != nullptr) {
            fclose(f);
        }
    }

    void write(const vid_t dst, const idx_t idx) {
        fwrite(&dst, sizeof(vid_t), 1, f);
        fwrite(&idx, sizeof(idx_t), 1, f);
    }

private:
    // src-index文件名
    std::string m_filename;
    FILE *f;
};


}
#endif //STARKNOWLEDGEGRAPH_DSTIDXFILEMANAGER_H
