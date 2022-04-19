#ifndef STARKNOWLEDGEGRAPHDATABASE_VECMEMTABLE_HPP
#define STARKNOWLEDGEGRAPHDATABASE_VECMEMTABLE_HPP

#include "util/status.h"
#include "util/options.h"
#include "EdgesQueryResult.h"

#include "EdgePartition.h"
#include "MemTable.h"



namespace skg {

    class VecMemTable: public MemTable {
    public:
        VecMemTable(const interval_t &interval,
                    const MetaAttributes &attributes,
                    const Options &options)
                : m_interval(interval),
                  m_buffered_edges(),
                  m_options(options),
                  m_attributes(attributes) {
        }

        const interval_t &GetInterval() const override {
            return m_interval;
        }

        size_t GetNumEdges() const override {
            return m_buffered_edges.size();
        }

        const EdgeLabel GetLabel() const override {
            return EdgeLabel(m_attributes.label, m_attributes.src_label, m_attributes.dst_label);
        }

    public:
        // 节点出发的增/删/查/改

        Status DeleteVertex(const VertexRequest &request) override;

        Status GetInEdges(const VertexRequest &request, EdgesQueryResult *result) const override ;
        Status GetOutEdges(const VertexRequest &request, EdgesQueryResult *result) const override ;
        Status GetBothEdges(const VertexRequest &request, EdgesQueryResult *result) const override ;

        Status GetInVertices(const VertexRequest &request, VertexQueryResult *result) const override ;
        Status GetOutVertices(const VertexRequest &request, VertexQueryResult *result) const override ;
        Status GetBothVertices(const VertexRequest &request, VertexQueryResult *result) const override ;

        Status GetInDegree(const vid_t &request, int *degree) const override ;
        Status GetOutDegree(const vid_t &request, int *degree) const override ;

        // 边出发的增/删/查/改

        Status AddEdge(const EdgeRequest &request) override ;
        Status DeleteEdge(const EdgeRequest &request) override ;
        Status GetEdgeAttributes(const EdgeRequest &request, EdgesQueryResult *result) override ;
        Status SetEdgeAttributes(const EdgeRequest &request) override ;

        Status ExtractEdges(std::vector<MemoryEdge> *edges, interval_t *interval) override {
            assert(edges != nullptr);
            assert(edges->empty());
            assert(interval != nullptr);
            *interval = m_interval;
            // buffer为空, 不做处理
            if (this->GetNumEdges() == 0) { return Status::OK(); }
            Status s;
            edges->swap(m_buffered_edges);
            m_buffered_edges.clear();
            return s;
        }

        Status CreateEdgeAttrCol(ColumnDescriptor descriptor) override ;

        size_t GetEstimateSize() const override {
            // 估算每条边占用空间大小
            size_t bytes_per_edge = sizeof(vid_t) + sizeof(vid_t) + sizeof(EdgeTag_t) + sizeof(EdgeWeight_t) + m_attributes.GetColumnsValueByteSize();
            // 估算 MemTable 大小
            size_t bytes_size = this->GetNumEdges() * bytes_per_edge;
            return bytes_size;
        }

        bool IsNeedFlush() const override {
            return GetEstimateSize() > m_options.mem_buffer_mb * MB_BYTES;
        }

        Status ExportData(const std::string &outDir,
                          std::shared_ptr<IDEncoder> encoder,
                          uint32_t shard_id, uint32_t partition_id) const override;
    private:
        /**
         * 按照 columns 中的属性列名字, 获取存储的边属性值, 存放到 buff 中
         *
         * columns: ColumnDescriptor 中, 设置了待获取的属性名 (note: 没有待获取的属性类型)
         */
        inline
        void CollectProperties(
                const MemoryEdge &edge,
                const std::vector<ColumnDescriptor> &columns,
                char *buff, PropertiesBitset_t *bitset) const;

        /**
         * 按照
         */
        inline
        void ReorderAttributesToEdge(const EdgeRequest &request, MemoryEdge *edge) const;
    private:
        // 管理的顶点区间
        interval_t m_interval;

        // buffer
        std::vector<MemoryEdge> m_buffered_edges;
        const Options m_options;

        MetaAttributes m_attributes;
    public:
        // no copying allow
        VecMemTable(const VecMemTable &) = delete;
        VecMemTable& operator=(const VecMemTable &) = delete;
    };

}
#endif //STARKNOWLEDGEGRAPHDATABASE_VECMEMTABLE_HPP
