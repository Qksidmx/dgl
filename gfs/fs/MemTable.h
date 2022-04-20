#ifndef STARKNOWLEDGEGRAPHDATABASE_MEMTABLE_H
#define STARKNOWLEDGEGRAPHDATABASE_MEMTABLE_H

#include "EdgeRequest.h"
#include "VertexRequest.h"
#include "EdgesQueryResult.h"
#include "VertexQueryResult.h"
#include "ColumnDescriptor.h"
#include "SubEdgePartition.h"

#include "util/internal_types.h"

namespace skg {

    class SubEdgePartition;

    class MemTable {
    public:
        MemTable() = default;
        virtual ~MemTable() = default;

        virtual
        const interval_t &GetInterval() const = 0;

        virtual
        size_t GetNumEdges() const = 0;

        virtual
        const EdgeLabel GetLabel() const = 0;
    public:

        // 节点出发的增/删/查/改

        virtual
        Status DeleteVertex(const VertexRequest &request) = 0;
        virtual
        Status GetInEdges(const VertexRequest &request, EdgesQueryResult *result) const = 0;
        virtual
        Status GetOutEdges(const VertexRequest &request, EdgesQueryResult *result) const = 0;
        virtual
        Status GetBothEdges(const VertexRequest &request, EdgesQueryResult *result) const = 0;

        virtual
        Status GetInVertices(const VertexRequest &request, VertexQueryResult *result) const = 0;
        virtual
        Status GetOutVertices(const VertexRequest &request, VertexQueryResult *result) const = 0;
        virtual
        Status GetBothVertices(const VertexRequest &request, VertexQueryResult *result) const = 0;

        virtual
        Status GetInDegree(const vid_t &request, int *result) const = 0;
        virtual
        Status GetOutDegree(const vid_t &request, int *result) const = 0;

        // 边出发的增/删/查/改

        /**
         * 插入新的边到 Memory-Table.
         *
         * 插入的数据在 Memory-Table 中按照 attributes 的形式组织.
         */
        virtual
        Status AddEdge(const EdgeRequest &request) = 0;

        virtual
        Status DeleteEdge(const EdgeRequest &request) = 0;

        virtual
        Status GetEdgeAttributes(const EdgeRequest &request, EdgesQueryResult *result) = 0;

        virtual
        Status SetEdgeAttributes(const EdgeRequest &request) = 0;

        virtual
        Status ExtractEdges(std::vector<MemoryEdge> *edges, interval_t *interval) = 0;

        virtual
        Status CreateEdgeAttrCol(ColumnDescriptor descriptor) = 0;

        virtual
        size_t GetEstimateSize() const = 0;

        virtual
        bool IsNeedFlush() const = 0;

        virtual
        Status ExportData(const std::string &outDir,
                          std::shared_ptr<IDEncoder> encoder,
                          uint32_t shard_id, uint32_t partition_id) const = 0;
    public:
        // no copying allow
        MemTable(const MemTable &) = delete;
        MemTable& operator=(const MemTable &rhs) = delete;
    };
}

#endif //STARKNOWLEDGEGRAPHDATABASE_MEMTABLE_H
