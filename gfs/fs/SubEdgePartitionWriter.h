#ifndef STARKNOWLEDGEGRAPHDATABASE_SUBEDGEPARTITIONWRITER_H
#define STARKNOWLEDGEGRAPHDATABASE_SUBEDGEPARTITIONWRITER_H

#include "util/status.h"

#include "util/internal_types.h"
#include "fs/MetaAttributes.h"

namespace skg {
    class SubEdgePartitionWriter {
    public:

        static
        Status FlushEdges(
                std::vector<MemoryEdge> &&buffered_edges,
                const std::string &storage_dir,
                uint32_t shard_id, uint32_t partition_id,
                const interval_t &interval,
                const MetaAttributes &attributes
        );

        static
        Status FlushEdgesSortTwice(
                std::vector<MemoryEdge> &&buffered_edges,
                const std::string &storage_dir,
                uint32_t shard_id, uint32_t partition_id,
                const interval_t &interval,
                const MetaAttributes &attributes
        );

        static
        std::vector<MemoryEdge> RemoveDuplicateEdges(std::vector<MemoryEdge> &&edges);

    };
}

#endif //STARKNOWLEDGEGRAPHDATABASE_SUBEDGEPARTITIONWRITER_H
