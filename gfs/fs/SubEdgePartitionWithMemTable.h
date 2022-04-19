#ifndef STARKNOWLEDGEGRAPHDATABASE_SUBEDGEPARTITIONWITHMEMTABLE_H
#define STARKNOWLEDGEGRAPHDATABASE_SUBEDGEPARTITIONWITHMEMTABLE_H

#include "SubEdgePartition.h"

namespace skg {

    class SubEdgePartitionWithMemTable: public SubEdgePartition {
    public:
        friend class SubEdgePartition;
    public:
        SubEdgePartitionWithMemTable(
                const std::string &prefix,
                uint32_t shard_id, uint32_t partition_id,
                const interval_t &interval, const MetaAttributes &attributes,
                const Options &options)
                : SubEdgePartition(prefix, shard_id, partition_id, interval, attributes, options),
                  m_memTable(nullptr) {
        }

    public:
        // ========================= //
        // == Partition 的统计数据  == //
        // ========================= //

        std::unique_ptr<MemTable> &GetMemTable() {
            return m_memTable;
        }

        inline size_t GetNumEdges() const override {
            return m_edge_list_f->num_edges() + m_memTable->GetNumEdges();
        }

        size_t GetNumEdgesInMemory() const override {
            return m_memTable->GetNumEdges();
        }
        
        Status FlushCache(bool force) override;

    public:

        // 节点的增/删/查/改
        Status DeleteVertex(const VertexRequest &request) override;

        /**
         * 读取所有到dst的入边
         * @param dst
         * @param pQueryResult
         */
        Status GetInEdges(const VertexRequest &req, EdgesQueryResult *pQueryResult) const override;

        /**
         * 读取所有src出发的出边
         * @param src
         * @param pQueryResult
         */
        Status GetOutEdges(const VertexRequest &req, EdgesQueryResult *pQueryResult) const override;

        Status GetBothEdges(const VertexRequest &req, EdgesQueryResult *result) const override;

        /**
         * @brief 获取入边的节点
         * @param req
         * @param pQueryResult
         * @return
         */
        Status GetInVertices(const VertexRequest &req, VertexQueryResult *result) const override;

        /**
         * @brief 获取出边的节点
         * @param req
         * @param pQueryResult
         * @return
         */
        Status GetOutVertices(const VertexRequest &req, VertexQueryResult *result) const override;

        Status GetBothVertices(const VertexRequest &req, VertexQueryResult *result) const override;

        Status GetInDegree(const vid_t dst, int * ans) const override;

        Status GetOutDegree(const vid_t src, int * ans) const override;


        // 边的增/删/查/改

        Status AddEdge(const EdgeRequest &request) override;

        /**
         * 删除指定的边
         */
        Status DeleteEdge(const EdgeRequest &request) override;

        /**
         * 获取指定边的属性值
         */
        Status GetEdgeAttributes(const EdgeRequest &request, EdgesQueryResult *result) override;

        /**
         * 更新指定边的属性值
         */
        Status SetEdgeAttributes(const EdgeRequest &request) override;

        Status CreateEdgeAttrCol(ColumnDescriptor descriptor) override;

        Status DeleteEdgeAttrCol(const std::string &columnName) override;

        Status ExportData(const std::string &outDir, std::shared_ptr<IDEncoder> encoder) override;

        virtual
        bool IsNeedFlush() const {
            return m_memTable->IsNeedFlush();
        }

    private:
        std::unique_ptr<MemTable> m_memTable;
    };

}

#endif //STARKNOWLEDGEGRAPHDATABASE_SUBEDGEPARTITIONWITHMEMTABLE_H
