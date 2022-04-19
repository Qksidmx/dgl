#ifndef STARKNOWLEDGEGRAPH_PERSISTENTSHARD_HPP
#define STARKNOWLEDGEGRAPH_PERSISTENTSHARD_HPP

#include <string>
#include <stack>
#include <sys/mman.h>

#include "util/types.h"
#include "util/options.h"
#include "fs/EdgesQueryResult.h"
#include "preprocessing/types.h"
#include "metrics/metrics.hpp"
#include "fs/Metadata.h"
#include "fs/EdgeListFileManager.h"
#include "fs/IdxFileWriter.h"
#include "fs/SubEdgePartition.h"
#include "fs/IEdgeColumnWriter.h"
#include "fs/IEdgeColumnPartition.h"
#include "fs/BlocksCacheManager.h"
//#include "fs/SkgDBImpl_BulkUpdate.h"
#include "util/dense_bitset.hpp"

namespace skg {

    class EdgePartition;

    using EdgePartitionPtr = std::shared_ptr<EdgePartition>;

    class EdgePartition {
    public:
        static
        Status Move(const std::string &dirname,
                    uint32_t to_shard_id, uint32_t to_partition_id,
                    const interval_t &interval, const MetaHeterogeneousAttributes &hetAttributes) {
            Status s;
            for (const auto &attr: hetAttributes) {
//                SKG_LOG_DEBUG("Moving sub partition of interval {}, label {}", interval, attr.label, attr.label_tag);
                s = PathUtils::RenameFile(
                        DIRNAME::sub_partition(dirname, 0, 0, interval, attr.label_tag),
                        DIRNAME::sub_partition(dirname, to_shard_id, to_partition_id, interval, attr.label_tag)
                );
                if (!s.ok()) { return s; }
            }
            return s;
        }

        static
        Status Open(const std::string &dirname,
                    uint32_t shard_id, uint32_t partition_id,
                    const interval_t &interval,
                    const Options &options,
                    EdgePartitionPtr *pPartition) {
            assert(*pPartition == nullptr);
            Status s;
            MetaHeterogeneousAttributes hetAttributes;
            // 到db的目录获取全局的边属性信息
            s = MetadataFileHandler::ReadEdgeAttrConf(dirname, &hetAttributes);
            if (!s.ok()) { return s; }
            EdgePartitionPtr impl = std::make_shared<EdgePartition>(dirname, shard_id, partition_id, interval, options);
            // 加载 sub-edge-partition
            std::shared_ptr<SubEdgePartition> subPartition;
            for (const auto &attributes : hetAttributes) {
                s = SubEdgePartition::Open(
                        dirname,
                        shard_id, partition_id, interval,
                        attributes, options,
                        &subPartition);
                if (!s.ok()) { return s; }
                impl->m_subpartitions.emplace_back(std::move(subPartition));
            }
            *pPartition = std::move(impl);
            return s;
        }

    public:
        EdgePartition(const std::string &dirname,
                      uint32_t shard_id, uint32_t partition_id,
                      const interval_t &interval,
                      const Options &options)
            : m_dirname(dirname),
              m_interval(interval),
              m_options(options),
              m_shard_id(shard_id),
              m_partition_id(partition_id),
              m_subpartitions(),
              m_leaves_id() {
        }

        uint32_t id() {
            return m_partition_id;
        }

        const interval_t &GetInterval() const {
            return m_interval;
        }

        std::vector<uint32_t> GetChildrenIds() const {
            return m_leaves_id;
        }

        Status SetChildrenIds(const std::vector<uint32_t> &indices) {
            m_leaves_id = indices;
            return Status::OK();
        }

        size_t GetNumEdges() const {
            size_t numEdges = 0;
            for (const auto &subpartition : m_subpartitions) {
                numEdges += subpartition->GetNumEdges();
            }
            return numEdges;
        }

        size_t GetNumEdgesInMemory() const {
            size_t numEdges = 0;
            for (const auto &subpartition : m_subpartitions) {
                numEdges += subpartition->GetNumEdgesInMemory();
            }
            return numEdges;
        }

        size_t GetNumEdgesInDisk() const {
            size_t numEdges = 0;
            for (const auto &subpartition : m_subpartitions) {
                numEdges += subpartition->GetNumEdgesInDisk();
            }
            return numEdges;
        }

        Status DeleteVertex(const VertexRequest &req) {
            Status s;
            for (auto &subpartition : m_subpartitions) {
                s = subpartition->DeleteVertex(req);
                if (!s.ok()) { return s; }
            }
            return s;
        }

        /**
         * 读取所有到dst的入边
         * @param dst
         * @param pQueryResult
         */
        Status GetInEdges(const VertexRequest &req, EdgesQueryResult *pQueryResult) const {
            Status s;
            for (const auto &subpartition : m_subpartitions) {
                s = subpartition->GetInEdges(req, pQueryResult);
                if (!s.ok()) { break; }
            }
            return s;
        }

        /**
         * 读取所有src出发的出边
         * @param src
         * @param pQueryResult
         */
        Status GetOutEdges(const VertexRequest &req, EdgesQueryResult *pQueryResult) const {
            Status s;
            for (const auto &subpartition : m_subpartitions) {
                s = subpartition->GetOutEdges(req, pQueryResult);
                if (!s.ok()) { return s; }
                if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 SubPartition 中获取数据
            }
            return s;
        }

        Status GetBothEdges(const VertexRequest &req, EdgesQueryResult *result) const {
            Status s;
            for (const auto &subpartition: m_subpartitions) {
                s = subpartition->GetBothEdges(req, result);
                if (!s.ok()) { break; }
                if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 SubPartition 中获取数据
            }
            return s;
        }

        Status GetInVertices(const VertexRequest &req, VertexQueryResult *result) const {
            Status s;
            for (const auto &subpartition: m_subpartitions) {
                s = subpartition->GetInVertices(req, result);
                if (!s.ok()) { break; }
                if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 SubPartition 中获取数据
            }
            return s;
        }

        Status GetOutVertices(const VertexRequest &req, VertexQueryResult *result) const {
            Status s;
            for (const auto &subpartition: m_subpartitions) {
                s = subpartition->GetOutVertices(req, result);
                if (!s.ok()) { break; }
                if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 SubPartition 中获取数据
            }
            return s;
        }

        Status GetBothVertices(const VertexRequest &req, VertexQueryResult *result) const {
            Status s;
            for (const auto &subpartition: m_subpartitions) {
                s = subpartition->GetBothVertices(req, result);
                if (!s.ok()) { break; }
                if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 SubPartition 中获取数据
            }
            return s;
        }

        Status GetInDegree(const vid_t dst, int * indegree) const {
            Status s;
            for (const auto &subpartition : m_subpartitions) {
                s = subpartition->GetInDegree(dst, indegree);
                if (!s.ok()) { return s; }
            }
            return s;
        }

        Status GetOutDegree(const vid_t src, int * outdegree) const {
            Status s;
            for (const auto &subpartition : m_subpartitions) {
                s = subpartition->GetOutDegree(src, outdegree);
                if (!s.ok()) { return s; }
            }
            return s;
        }

        Status AddEdge(const EdgeRequest &req) {
            Status s;
            m_interval.ExtendTo(req.m_dstVid);
            for (size_t i = 0; i < m_subpartitions.size(); ++i) {
                if (req.GetLabel() == m_subpartitions[i]->label()) {
                    s = m_subpartitions[i]->AddEdge(req);
                    m_interval.ExtendTo(m_subpartitions[i]->GetInterval().second);
                    return s;
                }
            }
            // 找不到 tag 的 subpartition
            return Status::InvalidArgument(fmt::format(
                    "edge label: `{}' not exist in shard[{}/{}].",
                    req.GetLabel().ToString(), m_shard_id, id()// FIXME shard-id
            ));
        }

        /**
         * 删除 src->dst 的边
         * (打标志, 待merge时再删除)
         * @param src
         * @param dst
         * @return
         */
        Status DeleteEdge(const EdgeRequest &req) {
            Status s;
            for (size_t i = 0; i < m_subpartitions.size(); ++i) {
                if (m_subpartitions[i]->label() == req.GetLabel()) {
                    s = m_subpartitions[i]->DeleteEdge(req);
                    return s;
                }
            }
            // 找不到相应的边
            return Status::NotExist();
        }

        Status GetEdgeAttributes(const EdgeRequest &request, EdgesQueryResult *result) {
            Status s;
            for (size_t i = 0; i < m_subpartitions.size(); ++i) {
                if (m_subpartitions[i]->label() == request.GetLabel()) {
                    s = m_subpartitions[i]->GetEdgeAttributes(request, result);
                    return s;
                }
            }
            // 找不到相应的边
            return Status::NotExist();
        }

        Status SetEdgeAttributes(const EdgeRequest &req) {
            Status s;
            for (size_t i = 0; i < m_subpartitions.size(); ++i) {
                if (req.GetLabel() == m_subpartitions[i]->label()) {
                    s = m_subpartitions[i]->SetEdgeAttributes(req);
                    return s;
                }
            }
            // 找不到 tag 的 subpartition
            return Status::NotExist();
        }

	/*
        Status BulkUpdate(
                const BulkUpdateOptions &bulk_options,
                ReqShardRange &req_shard_range, dense_bitset *is_updated) {
            Status s;
            for (size_t i = 0; i < m_subpartitions.size(); ++i) {
                m_subpartitions[i]->BulkUpdate(bulk_options, req_shard_range, is_updated);
            }
            return s;
        }
	*/

        Status FlushCache(bool force) {
            Status s;
            for (size_t i = 0; i < m_subpartitions.size(); ++i) {
                s = m_subpartitions[i]->FlushCache(force);
                if (!s.ok()) { return s; }
            }
            return s;
        }

        Status CreateNewEdgeLabel(
                const EdgeLabel &label,
                EdgeTag_t tag, EdgeTag_t src_tag, EdgeTag_t dst_tag) {
            for (size_t i = 0; i < m_subpartitions.size(); ++i) {
                if (label == m_subpartitions[i]->label()) {
                    return Status::InvalidArgument(fmt::format("edge label:`{}' already exist!", label.ToString()));
                }
            }
            Status s;
            MetaAttributes properties(label);
            properties.label_tag = tag;
            properties.src_tag = src_tag;
            properties.dst_tag = dst_tag;
            s = SubEdgePartition::Create(m_dirname, m_shard_id, m_partition_id, m_interval, properties);
            if (!s.ok()) { return s; }

            SubEdgePartitionPtr partition;
            s = SubEdgePartition::Open(m_dirname, m_shard_id, m_partition_id, m_interval, properties, m_options, &partition);
            if (!s.ok()) { return s; }
            m_subpartitions.emplace_back(std::move(partition));

            return s;
        }

        Status CreateEdgeAttrCol(const EdgeLabel &label, const ColumnDescriptor &config) {
            Status s = Status::NotExist(fmt::format("edge label:`{}' is NOT exist in edge-partition:{}~{}",
                    label.ToString(), m_interval.first, m_interval.second));
            for (size_t i = 0; i < m_subpartitions.size(); ++i) {
                if (label == m_subpartitions[i]->label()) {
                    s = m_subpartitions[i]->CreateEdgeAttrCol(config);
                    if (!s.ok()) { return s; }
                }
            }
            // 不存在 label 的 SubEdgePartition
            return s;
        }

        Status DeleteEdgeAttrCol(const EdgeLabel &label, const std::string &columnName) {
            Status s;
            for (size_t i = 0; i < m_subpartitions.size(); ++i) {
                if (label == m_subpartitions[i]->label()) {
                    s = m_subpartitions[i]->DeleteEdgeAttrCol(columnName);
                    if (!s.ok()) { return s; }
                }
            }
            return s;
        }

        Status ExportData(const std::string &outDir, std::shared_ptr<IDEncoder> encoder) {
            Status s;
            for (const auto &iter: m_subpartitions) {
                s = iter->ExportData(outDir, encoder);
                if (!s.ok()) { return s; }
            }
            return s;
        }

        bool IsNeedFlush() const {
            for (const auto &subpartition : m_subpartitions) {
                if (subpartition->IsNeedFlush()) {
                    return true;
                }
            }
            return false;
        }

        size_t GetEstimateSize() const {
            size_t size = 0;
            for (const auto &subpartition : m_subpartitions) {
                size += subpartition->GetEstimateSize();
            }
            return size;
        }
 
        bool IsNeedCompact() const {
            for (const auto &subpartition : m_subpartitions) {
                if (subpartition->IsNeedCompact()) {
                    return true;
                }
            }
            return false;
        }

    public:
        /**
         *  ==== 迭代器 ====
         *  可通过 for (const auto &col: attributes) { ... } 来访问异构属性中不同 类型 的attributes
         */
        using iterator = std::vector<SubEdgePartitionPtr>::iterator;
        using const_iterator = std::vector<SubEdgePartitionPtr>::const_iterator;

        inline
        iterator begin() {
            return m_subpartitions.begin();
        }

        inline
        iterator end() {
            return m_subpartitions.end();
        }

        inline
        const_iterator begin() const {
            return m_subpartitions.begin();
        }

        inline
        const_iterator end() const {
            return m_subpartitions.end();
        }

    private:
        const std::string m_dirname;
        interval_t m_interval;
        const Options m_options;
        uint32_t m_shard_id;
        uint32_t m_partition_id;

        // sub partition
        std::vector<SubEdgePartitionPtr> m_subpartitions;
        // 子节点
        std::vector<uint32_t> m_leaves_id;
    };
}

#endif //STARKNOWLEDGEGRAPH_PERSISTENTSHARD_HPP
