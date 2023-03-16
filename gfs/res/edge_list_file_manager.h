#ifndef STARKNOWLEDGEGRAPH_SHARDEDGELISTFILEMANAGER_HPP
#define STARKNOWLEDGEGRAPH_SHARDEDGELISTFILEMANAGER_HPP


#include <cstdint>
#include <cstring>
#include <sys/mman.h>

#include "fmt/format.h"

#include "status.h"
#include "filenames.h"

namespace gfs {

    class EdgeListFileWriter {
    public:
        explicit
        EdgeListFileWriter(
                const std::string &basefile,
                uint32_t shard_id, uint32_t partition_id,
                const interval_t &interval, const EdgeTag_t tag)
                : SHARD_BUFSIZE(64 * 1024UL * 1024UL), // 64MB constant
                  m_filename(FILENAME::sub_partition_edgelist(basefile, shard_id, partition_id, interval, tag)), f(nullptr) {
        }

        Status Open() {
            this->f = fopen(this->m_filename.c_str(), "wb");
            if (this->f == nullptr) {
                //return Status::IOError(fmt::format("Create edge-partition: {}, err: {}({})", this->m_filename, strerror(errno), errno));
                return Status::IOError("");
            }
            // 设置缓冲区大小(SHARDER_BUFSIZE)
            setvbuf(this->f, nullptr, _IOFBF, this->SHARD_BUFSIZE);
            return Status::OK();
        }

        ~EdgeListFileWriter() {
            if (f != nullptr) {
                fclose(f);
            }
        }

        void add_edge(const PersistentEdge &edge) {
            fwrite(&edge, sizeof(PersistentEdge), 1, f);
        }

    public:
        // buffsize
        const size_t SHARD_BUFSIZE;

        const std::string &filename() const {
            return m_filename;
        }

    private:
        std::string m_filename;
        FILE *f;
    };

}

#endif //STARKNOWLEDGEGRAPH_SHARDEDGELISTFILEMANAGER_HPP
