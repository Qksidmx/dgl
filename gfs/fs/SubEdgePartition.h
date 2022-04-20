#ifndef STARKNOWLEDGEGRAPHDATABASE_SUBEDGEPARTITION_H
#define STARKNOWLEDGEGRAPHDATABASE_SUBEDGEPARTITION_H

#include <string>
#include <stack>
#include <sys/mman.h>
#include "VertexRequest.h"
#include "EdgesQueryResult.h"
#include "ColumnDescriptor.h"


#include "util/types.h"
#include "util/options.h"
#include "fs/EdgesQueryResult.h"
#include "fs/VertexRequest.h"
#include "fs/EdgeRequest.h"
#include "preprocessing/types.h"
#include "metrics/metrics.hpp"
//#include "fs/EdgeListFileManager.hpp"
#include "fs/IdxFileWriter.h"
#include "fs/EdgeListReader.h"
#include "fs/EdgeListMMapReader.h"
#include "fs/EdgeListRawReader.h"
#include "fs/IdxReader.h"
#include "fs/IEdgeColumnWriter.h"
#include "fs/IEdgeColumnPartition.h"
#include "fs/MetaAttributes.h"
#include "fs/BlocksCacheManager.h"
//#include "util/chifilenames.h"
#include "util/pathutils.h"
#include "fs/MemTable.h"
//#include "fs/SkgDBImpl_BulkUpdate.h"
#include "util/dense_bitset.hpp"

#include "fs/VertexRequest.h"
#include "fs/ColumnDescriptorUtils.h"

namespace skg {
    class MemTable;

    class SubEdgePartition;
    using SubEdgePartitionPtr = std::shared_ptr<SubEdgePartition>;

    class SubEdgePartition {
    public:
        enum class FlushCompactState {
            FLUSH_NOT_REQUEST,
            // 需要把 MemTable 的数据合并到 SubEdgePartition 中
            FLUSH_COMPACT_MEMORY_TABLE,
            // 需要把 SubEdgePartition 的数据合并到树的下一层
            FLUSH_COMPACT_LEVEL,
        };
    public:
        static
        bool IsPartitionExist(
                const std::string &dirname,
                uint32_t shard_id, uint32_t partition_id,
                const interval_t &interval, const EdgeTag_t tag);

        static
        Status DropPartition(
                const std::string &prefix,
                uint32_t shard_id, uint32_t partition_id,
                const interval_t &interval, const EdgeTag_t tag);

        static
        Status Create(
                const std::string &prefix,
                uint32_t shard_id, uint32_t partition_id,
                const interval_t &interval, const MetaAttributes &attributes);

        static
        Status Open(
                const std::string &prefix,
                uint32_t shard_id, uint32_t partition_id,
                const interval_t &interval, const MetaAttributes &attributes,
                const Options &options,
                std::shared_ptr<SubEdgePartition> *pShard);

    public:
        SubEdgePartition(
                const std::string &prefix,
                uint32_t shard_id, uint32_t partition_id,
                const interval_t &interval, const MetaAttributes &attributes,
                const Options &options);

        virtual ~SubEdgePartition();

    public:

        /**
         * 删除此 Partition
         */
        Status Drop();

        // ========================= //
        // == Partition 的统计数据  == //
        // ========================= //

        const std::string GetStorageDir() const {
            return m_storage_dir;
        }

        uint32_t shard_id() const {
            return m_shard_id;
        }

        uint32_t id() const {
            return m_partition_id;
        }

        const MetaAttributes &attributes() const {
            return m_attributes;
        }

        const interval_t &GetInterval() const {
            return m_interval;
        }

        virtual
        inline size_t GetNumEdges() const {
            return m_edge_list_f->num_edges();
        }


        virtual
        size_t GetNumEdgesInMemory() const {
            return 0;
        }

        size_t GetNumEdgesInDisk() const {
            return m_edge_list_f->num_edges();
        }

        inline const EdgeLabel label() const {
            return m_attributes.GetEdgeLabel();
        }

        /**
         * 持久化到磁盘
         */
        virtual
        Status FlushCache(bool force);

        Status MergeEdgesAndFlush(std::vector<MemoryEdge> &&buffered_edges, const interval_t &interval);

        // 节点的增/删/查/改
        virtual
        Status DeleteVertex(const VertexRequest &request);

        /**
         * 读取所有到dst的入边
         */
        virtual
        Status GetInEdges(const VertexRequest &req, EdgesQueryResult *pQueryResult) const;

        /**
         * 读取所有src出发的出边
         */
        virtual
        Status GetOutEdges(const VertexRequest &req, EdgesQueryResult *pQueryResult) const;

        /**
         * @brief
         * @param req
         * @param result
         * @return
         */
        virtual
        Status GetBothEdges(const VertexRequest &req, EdgesQueryResult *result) const;

        /**
         * @brief 获取入边的节点
         * @param req
         * @param pQueryResult
         * @return
         */
        virtual
        Status GetInVertices(const VertexRequest &req, VertexQueryResult *result) const;

        /**
         * @brief 获取出边的节点
         * @param req
         * @param pQueryResult
         * @return
         */
        virtual
        Status GetOutVertices(const VertexRequest &req, VertexQueryResult *result) const;

        virtual
        Status GetBothVertices(const VertexRequest &req, VertexQueryResult *result) const;

        virtual
        Status GetInDegree(const vid_t dst, int * ans) const;

        virtual
        Status GetOutDegree(const vid_t src, int * ans) const;


        // 边的增/删/查/改

        virtual
        Status AddEdge(const EdgeRequest &request);

        /**
         * 删除指定的边
         */
        virtual
        Status DeleteEdge(const EdgeRequest &request);

        /**
         * 获取指定边的属性值
         */
        virtual
        Status GetEdgeAttributes(const EdgeRequest &request, EdgesQueryResult *result);

        /**
         * 更新指定边的属性值
         */
        virtual
        Status SetEdgeAttributes(const EdgeRequest &request);

        /*virtual
        Status BulkUpdate(
                const BulkUpdateOptions &bulk_options,
                ReqShardRange &req_shard_range,
                dense_bitset *is_updated);
		*/

        virtual
        Status CreateEdgeAttrCol(ColumnDescriptor config);

        virtual
        Status DeleteEdgeAttrCol(const std::string &columnName);
        
        virtual
        Status ExportData(const std::string &outDir, std::shared_ptr<IDEncoder> encoder);
    private:
        void ReferByOptions(const Options &options);

        /**
         * 根据 src, dst 定位某条边在此 partition 中是第几条边. 用于操作边的属性
         */
        idx_t GetEdgeIdx(const vid_t src, const vid_t dst) const;

        Status OpenHandlers();

        Status CloseHandlers();

        Status CollectProperties(
                const PersistentEdge &edge,
                const idx_t idx,
                const std::vector<ColumnDescriptor> &columns,
                char *buff, PropertiesBitset_t *bitset) const;

        Status GetPropertiesColumnHandler(const std::string &colname, IEdgeColumnPartitionPtr *ptr) const ;
    public:
        virtual
        size_t GetEstimateSize() const;

        // ==== FIXME 待独立出去 ==== //
        Status TruncatePartition();

        virtual
        bool IsNeedFlush() const {
            return false;
        }

        virtual
        bool IsNeedCompact() const {
            // FIXME 按照 split_factor == 4, 1+4+16 -> 21
            // FIXME 暂时按照两层计算, 1 + 4 == 5
            return this->GetEstimateSize() > m_options.shard_size_mb * MB_BYTES / (1 + 4);
        }

        /**
         * 从磁盘中读取所有的边 (忽略被打上删除标志的边)
         * @return
         */
        std::vector<MemoryEdge> LoadAllEdges();
    protected:
        const std::string m_storage_dir;
        uint32_t m_shard_id;
        uint32_t m_partition_id;
        MetaAttributes m_attributes;
        std::unique_ptr<EdgeListReader> m_edge_list_f;
        std::unique_ptr<IndexReader> m_src_index_f;
        std::unique_ptr<IndexReader> m_dst_index_f;
        interval_t m_interval;
        const Options m_options;
        size_t m_num_max_shard_edges;
        // 属于该partition的边属性列
        std::vector<IEdgeColumnPartitionPtr> m_columns;

    public:
        // no copying allow
        SubEdgePartition(const SubEdgePartition&) = delete;
        SubEdgePartition& operator=(const SubEdgePartition&) = delete;
    };

}
#endif //STARKNOWLEDGEGRAPHDATABASE_SUBEDGEPARTITION_H
