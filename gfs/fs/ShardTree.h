#ifndef STARKNOWLEDGEGRAPHDATABASE_SHARDTREE_H
#define STARKNOWLEDGEGRAPHDATABASE_SHARDTREE_H

#include <fstream>
#include <queue>

#include "util/status.h"
#include "util/options.h"
#include "EdgeRequest.h"
#include "VertexRequest.h"
#include "VertexQueryResult.h"
#include "fmt/format.h"

#include "util/internal_types.h"
#include "VecMemTable.h"
#include "HashMemTable.h"
#include "EdgePartition.h"
#include "Metadata.h"
//#include "SkgDBImpl_BulkUpdate.h"

namespace skg {

    class ShardTree;
    using ShardTreePtr = std::shared_ptr<ShardTree>;

    class ShardTree {
    public:
        static
        Status Create(const std::string &dirname, uint32_t shard_id, const MetaPartition &partition);
        static
        Status Create(const std::string &dirname, uint32_t shard_id, const MetaPartition &partition,
                      const MetaHeterogeneousAttributes &hetProp);
        static
        Status Ingest(const std::string &dirname, const MetaPartition &partition, 
                      const MetaHeterogeneousAttributes &hetAttributes);
        static
        Status Open(const std::string &dirname,
                    uint32_t shard_id, const interval_t interval,
                    const Options &options,
                    ShardTreePtr *pTree);

        ShardTree(const std::string &dirname,
                  uint32_t shard_id, const interval_t &interval,
                  const Options &options)
                : m_dirname(dirname), m_shard_id(shard_id), m_interval(interval), m_options(options),
                  m_closed(true)  {
        }

    public:
        virtual ~ShardTree() {
            if (m_closed) { return ; }
            this->Flush();
            for (size_t i = 0; i < m_partitions.size(); ++i) {
                // buff 合并到 partition 中
                if (m_partitions[i]->IsNeedFlush()) {
                    m_flush_queue.emplace_back(m_partitions[i]);
                }
            }
            this->DoFlush();
            m_closed = true;
        }
    public:
        inline uint32_t id() const {
            return m_shard_id;
        }

        std::string GetStorageDir() const {
            return m_dirname;
        }

        std::string GetTreeDir() const {
            return DIRNAME::shardtree(m_dirname, m_shard_id);
        }

        const Options options() const {
            return m_options;
        }

        interval_t GetInterval() const {
            return m_interval;
        }

        size_t GetNumEdges() const;

        struct NumEdgesDetail {
            size_t memory_num_edges;
            size_t disk_num_edges;
            size_t num_edges() const {
                return memory_num_edges + disk_num_edges;
            }

            NumEdgesDetail() {
                memory_num_edges = 0;
                disk_num_edges = 0;
            }
        };

        NumEdgesDetail GetNumEdgesDetail() const;

    public:

        Status Flush();

        // 边的增/删/查/改

        /**
         * @brief
         * request.IsCheckExist()
         *      -- true  -> 先检查边是否在图中, 如果存在, 则更新边的值.
         *                  相当于调用 SetEdgeAttr, 且设置了 `CreateIfNotExist`
         *      -- false -> 如果确认插入的边不存在于原来的图中, 直接插入到 MemTable 缓存中.
         */
        Status AddEdge(/*const*/ EdgeRequest &request);

        Status DeleteEdge(const EdgeRequest &request);

        /**
         * @brief
         */
        Status GetEdgeAttributes(const EdgeRequest &request, EdgesQueryResult *result);

        /**
         * @brief
         * request.IsCreateIfNotExist()
         *      -- true  -> 如果边不存在, 则尝试插入新的边并设置属性
         *      -- false -> 如果边不存在, 返回 NotExist
         */
        Status SetEdgeAttributes(/*const*/ EdgeRequest &request);

        // 节点的增/删/查/改
        Status DeleteVertex(const VertexRequest &request) const;
        Status GetInEdges(const VertexRequest &request, EdgesQueryResult *result) const;
        Status GetOutEdges(const VertexRequest &request, EdgesQueryResult *result) const;
        Status GetBothEdges(const VertexRequest &request, EdgesQueryResult *result) const;
        // 为多线程增加接口
        static
        Status MtiGetOutE(const ShardTreePtr& ptr, const VertexRequest *request, EdgesQueryResult *result) {
            return ptr->GetOutEdges(*request, result);
        }

        /**
         * @brief 获取入边的节点
         * @param req
         * @param pQueryResult
         * @return
         */
        Status GetInVertices(const VertexRequest &request, VertexQueryResult *result) const;

        /**
         * @brief 获取出边的节点
         * @param req
         * @param pQueryResult
         * @return
         */
        Status GetOutVertices(const VertexRequest &request, VertexQueryResult *result) const;
        Status GetBothVertices(const VertexRequest &request, VertexQueryResult *result) const;

        // 为多线程增加接口
        static
        Status MtiGetOutV(const ShardTreePtr& ptr, const VertexRequest *request, VertexQueryResult *result) {
            return ptr->GetOutVertices(*request, result);
        }

        Status GetInDegree(const VertexRequest &request, VertexQueryResult *result) const;
        Status GetOutDegree(const VertexRequest &request, VertexQueryResult *result) const;

        Status CreateNewEdgeLabel(const EdgeLabel &label, EdgeTag_t tag, EdgeTag_t src_tag, EdgeTag_t dst_tag);
        Status CreateEdgeAttrCol(const EdgeLabel &label, const ColumnDescriptor &config);
        Status DeleteEdgeAttrCol(const EdgeLabel &label, const std::string &columnName);
        Status ExportData(const std::string &outDir, std::shared_ptr<IDEncoder> encoder);

        /*Status BulkUpdate(const BulkUpdateOptions &bulk_options,
                          ReqShardRange &req_shard_range);
        // 为多线程增加的接口
        static
        Status MtiBulkUpdate(const ShardTreePtr &ptr,
                            const BulkUpdateOptions &bulk_options, ReqShardRange &req_shard_range) {
            return ptr->BulkUpdate(bulk_options, req_shard_range);
        }
	*/

    private:
        Status Load();

        Status LoadTree(const MetaPartition &curRoot, std::vector<EdgePartitionPtr> *flatPartitions, uint32_t *idx);

        Status CollectMetaPartition(size_t curRootIdx, MetaPartition *metaPartition);

        Status DoFlush();
        Status DoCompaction();

        /**
         * @brief ShardTree 估计大小(字节数)
         */
        size_t GetEstimateSize() const {
            size_t size = 0;
            for (const auto &p : m_partitions) {
                size += p->GetEstimateSize();
            }
            return size;
        }

    private:
        Status AddEdgeNotCheckExist(/*const*/ EdgeRequest &request);

        std::string m_dirname;
        uint32_t m_shard_id;
        interval_t m_interval;
        const Options m_options;
        std::vector<EdgePartitionPtr> m_partitions;
        std::deque<EdgePartitionPtr> m_flush_queue;
        std::deque<EdgePartitionPtr> m_compaction_queue;
        std::atomic<bool> m_closed;
    public:
        // No copying allowed
        ShardTree(const ShardTree&) = delete;
        ShardTree& operator=(const ShardTree&) = delete;
    };
}

#endif //STARKNOWLEDGEGRAPHDATABASE_SHARDTREE_H
