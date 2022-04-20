#include "ShardTree.h"

#include <fstream>
#include <queue>

#include "util/status.h"
#include "util/options.h"
#include "fmt/format.h"

#include "util/internal_types.h"
#include "fs/VecMemTable.h"
#include "fs/HashMemTable.h"
#include "util/skglogger.h"
#include "fs/Metadata.h"
#include "fs/Compaction.h"
#include "util/dense_bitset.hpp"

namespace skg {

    Status ShardTree::Create(const std::string &dirname, uint32_t shard_id, const MetaPartition &partition) {
        Status s;
        const std::string shardTreeDir = DIRNAME::shardtree(dirname, shard_id, partition.interval);
        s = PathUtils::CreateDirIfMissing(shardTreeDir);//dbDir/shard1,dbDir/shard2...
        if (!s.ok()) { return s; }
        // shard-tree 的元数据
        s = PathUtils::CreateDirIfMissing(DIRNAME::meta(shardTreeDir));//dbDir/shard1/meta,dbDir/shard2/meta...
        if (!s.ok()) { return s; }
        s = MetadataFileHandler::WriteShardTreeIntervals(shardTreeDir, partition);//dbDir/shard1/intervals,dbDir/shard2/intervals...
        if (!s.ok()) { return s; }
        return s;
    }

    Status ShardTree::Create(const std::string &dirname, uint32_t shard_id, const MetaPartition &partition,
                             const MetaHeterogeneousAttributes &hetProp) {
        Status s = ShardTree::Create(dirname, shard_id, partition);
        if (!s.ok()) { return s; }
        // 创建属性
        for (const auto &prop: hetProp) {
            s = SubEdgePartition::Create(dirname, shard_id, 0, partition.interval, prop);
            if (!s.ok()) { return s; }
        }
        assert(partition.children.empty());
        return s;
    }

    Status ShardTree::Ingest(const std::string &dirname, const MetaPartition &partition,
                             const MetaHeterogeneousAttributes &hetAttributes) {
        Status s;
        uint32_t shard_id = partition.id;//partition is root in before, sequentially assigned id from 1.
        s = Create(dirname, shard_id, partition);//create shard dir with meta file and intervals file
        if (!s.ok()) { return s; }
        // 根节点
        s = EdgePartition::Move(dirname, shard_id, 0, partition.interval, hetAttributes);
        if (!s.ok()) { return s; }
        // BFS 处理所有要导入的 Partition
        std::queue<MetaPartition> q;
        q.push(partition);
        while (!q.empty()) {
            MetaPartition root = q.front();
            q.pop();
            for (auto &child : root.children) {
                s = EdgePartition::Move(dirname, shard_id, child.id, child.interval, hetAttributes);
                if (!s.ok()) { return s; }
                if (!child.children.empty()) {
                    q.push(std::move(child));
                }
            }
        }
        return s;
    }

    Status ShardTree::Open(const std::string &dirname,
                           uint32_t shard_id, const interval_t interval,
                           const Options &options, ShardTreePtr *pTree) {
        Status s;
        SKG_LOG_DEBUG("Loading shard-tree: {}-{}", interval.first, interval.second);
        ShardTree *impl = new ShardTree(dirname, shard_id, interval, options);
        s = impl->Load();
        impl->m_closed = false;
        if (s.ok()) {
            *pTree = ShardTreePtr(impl);
        }
        return s;
    }

    size_t ShardTree::GetNumEdges() const {
        size_t numEdges = 0;
        for (const auto &partition : m_partitions) {
            numEdges += partition->GetNumEdges();
        }
        return numEdges;
    }

    ShardTree::NumEdgesDetail ShardTree::GetNumEdgesDetail() const {
        NumEdgesDetail detail;
        for (const auto &partition : m_partitions) {
            detail.memory_num_edges += partition->GetNumEdgesInMemory();
            detail.disk_num_edges += partition->GetNumEdgesInDisk();
        }
        return detail;
    }

    Status ShardTree::Flush() {
        Status s;
        SKG_LOG_DEBUG("flushing shard-tree: {}", m_shard_id);
        // buffer 中新插入的边, 刷到磁盘
        s = m_partitions[0]->FlushCache(true);
        if (!s.ok()) { return s; }
        // buff 合并到 Partition 中
        m_partitions[0]->FlushCache(false);
        m_flush_queue.emplace_back(m_partitions[0]);
        // TODO 放后台线程进行处理
        s = DoFlush();
        if (!s.ok()) { return s; }
        // ==== update metadata ==== //
        // BFS 方式, 获取 shard-tree 的 intervals
        assert(!m_partitions.empty());
        MetaPartition metaPartition;
        s = CollectMetaPartition(0, &metaPartition);
        if (!s.ok()) { return s; }
        s = MetadataFileHandler::WriteShardTreeIntervals(GetTreeDir(), metaPartition);
        if (!s.ok()) { return s; }
        return s;
    }

    Status ShardTree::AddEdge(/*const*/ EdgeRequest &request) {
        request.SetCreateIfNotExist(true);// 如果边不存在, 则创建一条新的边
        if (request.IsCheckExist()) {// 检查边是否存在, 如果存在, 则转化为更新操作
            return this->SetEdgeAttributes(request);
        } else {
            // 不检查边是否存在, 直接插入到 shard-tree 顶部的 partition buff 中
            return this->AddEdgeNotCheckExist(request);
        }
    }

    Status ShardTree::AddEdgeNotCheckExist(/*const*/ EdgeRequest &request) {

        metrics::GetInstance()->start_time("ShardTree.AddEdgeNotCheckExist",metric_duration_type::MILLISECONDS);

        // 不检查边是否存在, 直接插入到 shard-tree 顶部的 partition buff 中
        m_interval.ExtendTo(request.m_dstVid);
        Status s = m_partitions[0]->AddEdge(request);
        if (!s.ok()) { return s; }

        m_partitions[0]->FlushCache(false);
        // TODO 检查 buff 是不是满了, 如果满了, 合并到 partition 中
        if (m_partitions[0]->IsNeedFlush()) {
            m_flush_queue.emplace_back(m_partitions[0]);
        }
        for (const auto &partition : m_partitions) {
            // TODO 检查 shard-tree 是不是满了, 如果满了, 通过 Status 返回外层, 通知需要分裂
            if (partition->IsNeedCompact()) {
                size_t sz = partition->GetEstimateSize();
                SKG_LOG_INFO("partition of {}:{} is going to do compaction, size: {:.1f}MB",
                             this->id(), partition->id(), 1.0 * sz / MB_BYTES);
                m_compaction_queue.emplace_back(partition);
            }
        }

        // TODO 放后台线程进行处理
        s = DoFlush();

        if (s.ok()) {
            s = DoCompaction();
        }
        metrics::GetInstance()->stop_time("ShardTree.AddEdgeNotCheckExist");

        return s;
    }

    Status ShardTree::SetEdgeAttributes(/*const*/ EdgeRequest &request) {
        Status s = Status::NotExist(); // 默认为找不到的错误状态
        // 尝试在 partition 中寻找边, 更新属性值
        metrics::GetInstance()->start_time("ShardTree.SetEdgeAttributes.find-and-update",metric_duration_type::MILLISECONDS);
        for (size_t p = 0; p < m_partitions.size(); ++p) {
            if (m_partitions[p]->GetInterval().Contain(request.m_dstVid)) {
                s = m_partitions[p]->SetEdgeAttributes(request);
                if (s.ok()) { // 已经找到并更新边
                    break;
                } else if (!s.IsNotExist()) {
                    // 出错
                    return s;
                } else {
                    // 找不到边, 继续在其他 partition 中查找
                }
            }
        }
        metrics::GetInstance()->stop_time("ShardTree.SetEdgeAttributes.find-and-update");
        // 所有 partition 都找不到需要修改的边
        if (request.IsCreateIfNotExist() && s.IsNotExist()) {
            // 如果用户指定了不存在需要插入一条新的边
            EdgeLabel lbl = request.GetLabel();
            SKG_LOG_TRACE("edge lbl:`{}', e[`{}:{}'->`{}:{}'], eid[`{}'->`{}'] not exist."
                          "going to create.",
                          lbl.ToString(),
                          request.m_srcVertexLabel, request.m_srcVertex,
                          request.m_dstVertexLabel, request.m_dstVertex,
                          request.m_srcVid, request.m_dstVid);
            s = this->AddEdgeNotCheckExist(request);
        }
        return s;
    }

    /*
    Status ShardTree::BulkUpdate(
            const BulkUpdateOptions &bulk_options,
            ReqShardRange &req_shard_range) {
        Status s;
        metrics::GetInstance()->start_time("ShardTree.BulkUpdate.find-update",metric_duration_type::MILLISECONDS);
        m_interval.ExtendTo(req_shard_range.interval.second); // 扩充 interval 范围
        dense_bitset is_updated(req_shard_range.num_edges());
        // 到 各个 partition 中批量更新存在的边
        for (size_t p = 0; p < m_partitions.size(); ++p) {
            m_partitions[p]->BulkUpdate(bulk_options, req_shard_range, &is_updated);
        }
        metrics::GetInstance()->stop_time("ShardTree.BulkUpdate.find-update");
        // 不存在的边, 插入到顶层的 partition 中
        metrics::GetInstance()->start_time("ShardTree.BulkUpdate.add",metric_duration_type::MILLISECONDS);
        for (ReqsIter iter = req_shard_range.edges_beg; iter != req_shard_range.edges_end; ++iter) {
            uint32_t req_index = iter - req_shard_range.edges_beg;
            if (!is_updated.get(req_index)) {
                s = this->AddEdgeNotCheckExist(*iter);
                // TODO 插入失败的边, 返回
            }
        }
        metrics::GetInstance()->stop_time("ShardTree.BulkUpdate.add");
        return s;
    }
    */

    Status ShardTree::DeleteEdge(const EdgeRequest &request) {
        Status s;
        // 尝试在 partition 中寻找边并删除
        for (size_t p = 0; p < m_partitions.size(); ++p) {
            if (m_partitions[p]->GetInterval().Contain(request.m_dstVid)) {
                s = m_partitions[p]->DeleteEdge(request);
                if (s.ok()) { // 已经找到边并删除
                    break;
                } else if (!s.IsNotExist()) {
                    // 出错
                    return s;
                } else {
                    // 找不到边, 继续在其他 partition 中查找
                }
            }
        }
        return s;
    }

    Status ShardTree::GetEdgeAttributes(const EdgeRequest &request, EdgesQueryResult *result) {
        Status s;
        // 尝试在 partition 中寻找边, 获取属性值
        for (size_t p = 0; p < m_partitions.size(); ++p) {
            // partition 的 interval 不包含 dst, 则 partition 一定不含其入边
            if (m_partitions[p]->GetInterval().Contain(request.m_dstVid)) {
                s = m_partitions[p]->GetEdgeAttributes(request, result);
                if (s.ok()) { // 已经找到边
                    return s;
                } else if (!s.IsNotExist()) {
                    // 出错
                    return s;
                } else {
                    // 如果在此 partition 中找不到, 继续到其他 partition 中查找
                }
            }
        }
        return Status::NotExist(fmt::format(
                "partitions of edge label:{} not exist.",
                request.GetLabel().ToString()));
    }

    Status ShardTree::DeleteVertex(const VertexRequest &request) const {
        Status s;
        // 遍历 partition, 删除 vertex 的所有出边 && 入边
        for (const auto &partition : m_partitions) {
            s = partition->DeleteVertex(request);
            if (!s.ok()) { return s; }
        }
        return s;
    }

    Status ShardTree::GetInEdges(const VertexRequest &request, EdgesQueryResult *pQueryResult) const {
        Status s;
        for (size_t p = 0; p < m_partitions.size(); ++p) {
            // partition 的 interval 不包含 dst, 则 partition 一定不含其入边
            if (m_partitions[p]->GetInterval().Contain(request.m_vid)) {
                s = m_partitions[p]->GetInEdges(request, pQueryResult);
                if (!s.ok()) { break; }
                if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 Partition 中获取数据
            }
        }
        return s;
    }

    Status ShardTree::GetOutEdges(const VertexRequest &request, EdgesQueryResult *pQueryResult) const {
        Status s;
        // out-edges 可能分布在所有的 partition 中
        for (size_t p = 0; p < m_partitions.size(); ++p) {
            s = m_partitions[p]->GetOutEdges(request, pQueryResult);
            if (!s.ok()) { return s; }
            if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 Partition 中获取数据
        }
        return s;
    }

    Status ShardTree::GetBothEdges(const VertexRequest &request, EdgesQueryResult *result) const {
        Status s;
        for (size_t p = 0; p < m_partitions.size(); ++p) {
            s = m_partitions[p]->GetBothEdges(request, result);
            if (!s.ok()) { break; }
            if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 Partition 中获取数据
        }
        return s;
    }


    Status ShardTree::GetInVertices(const VertexRequest &request, VertexQueryResult *result) const {
        Status s;
        for (size_t p = 0; p < m_partitions.size(); ++p) {
            // partition 的 interval 不包含 dst, 则 partition 一定不含其入边
            if (m_partitions[p]->GetInterval().Contain(request.m_vid)) {
                s = m_partitions[p]->GetInVertices(request, result);
                if (!s.ok()) { break; }
                if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 Partition 中获取数据
            }
        }
        return s;
    }

    Status ShardTree::GetOutVertices(const VertexRequest &request, VertexQueryResult *result) const {
        Status s;
        // out-edges 可能分布在所有 partition 中
        for (size_t p = 0; p < m_partitions.size(); ++p) {
            s = m_partitions[p]->GetOutVertices(request, result);
            if (!s.ok()) { return s; }
            if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 Partition 中获取数据
        }
        return s;
    }

    Status ShardTree::GetBothVertices(const VertexRequest &request, VertexQueryResult *result) const {
        Status s;
        for (size_t p = 0; p < m_partitions.size(); ++p) {
            s = m_partitions[p]->GetBothVertices(request, result);
            if (!s.ok()) { return s; }
            if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 Partition 中获取数据
        }
        return s;
    }

    Status ShardTree::GetInDegree(const VertexRequest &request, VertexQueryResult *pResult) const {
        pResult->Clear(); // 清空之前的数据

        Status s;
        int degree = 0;

        // 设置返回的 metadata
        MetaAttributes attributes(request.GetLabel());
        s = attributes.AddColumn(ColumnDescriptor("degree", ColumnType::INT32));
        if (!s.ok()) { return s; }
        MetaHeterogeneousAttributes hAttributes;
        s = hAttributes.AddAttributes(attributes);
        if (!s.ok()) { return s; }
        pResult->SetResultMetadata(hAttributes);

        do {
            for (size_t p = 0; p < m_partitions.size(); ++p) {
                if (m_partitions[p]->GetInterval().Contain(request.m_vid)) {
                    s = m_partitions[p]->GetInDegree(request.m_vid, &degree);
                    if (!s.ok()) { break; }
                }
            }
        } while (false);
        if (s.ok()) {
            // 设置 degree 到 Result 中
            ResultProperties properties(sizeof(int32_t));
            properties.put(degree, 0, 0);
            pResult->Receive(request.m_labelTag, request.m_vid, request.m_vertex, properties);
        }
        return s;
    }

    Status ShardTree::GetOutDegree(const VertexRequest &request, VertexQueryResult *pResult) const {
        pResult->Clear(); // 清空之前的数据

        Status s;
        int degree = 0;

        // 设置返回的 metadata
        MetaAttributes attributes(request.m_label);
        s = attributes.AddColumn(ColumnDescriptor("degree", ColumnType::INT32));
        if (!s.ok()) { return s; }
        MetaHeterogeneousAttributes hAttributes;
        s = hAttributes.AddAttributes(attributes);
        if (!s.ok()) { return s; }
        pResult->SetResultMetadata(hAttributes);

        do {
            for (size_t p = 0; p < m_partitions.size(); ++p) {
                s = m_partitions[p]->GetOutDegree(request.m_vid, &degree);
                if (!s.ok()) { return s; }
            }
        } while (false);
        if (s.ok()) {
            // 设置 degree 到 Result 中
            ResultProperties properties(sizeof(int32_t));
            properties.put(degree, 0, 0);
            pResult->Receive(request.m_labelTag, request.m_vid, request.m_vertex, properties);
        }
        return s;
    }

    Status ShardTree::Load() {
        m_partitions.clear();
        Status s;
        MetaPartition metaPartition;
        s = MetadataFileHandler::ReadShardTreeIntervals(GetTreeDir(), &metaPartition);
        if (!s.ok()) { return s; }
        // 加载 shard-tree 的树型结构
        uint32_t rootIdx = static_cast<uint32_t>(-1);
        s = LoadTree(metaPartition, &m_partitions, &rootIdx);
        if (!s.ok()) { return s; }
        assert(rootIdx == 0);
        return s;
    }

    Status ShardTree::LoadTree(
            const MetaPartition &curRoot,
            std::vector<EdgePartitionPtr> *flatPartitions,
            uint32_t *idx) {
        Status s;
        EdgePartitionPtr partition;
        *idx = flatPartitions->size(); // partition 加载的位置
        s = EdgePartition::Open(
                GetStorageDir(),
                m_shard_id, curRoot.id,
                curRoot.interval,
                m_options, &partition);
        if (!s.ok()) { return s; }
        flatPartitions->emplace_back(std::move(partition));
        partition = flatPartitions->back();
        if (!curRoot.children.empty()) {
            // 递归加载子树
            std::vector<uint32_t> childrenIds(curRoot.children.size());
            for (size_t i = 0; i < curRoot.children.size(); ++i) {
                s = LoadTree(curRoot.children[i], flatPartitions, &childrenIds[i]);
                if (!s.ok()) { return s; }
            }
            partition->SetChildrenIds(childrenIds);
        }
        return s;
    }

    Status ShardTree::CollectMetaPartition(size_t curRootIdx, MetaPartition *metaPartition) {
        Status s;
        metaPartition->id = m_partitions[curRootIdx]->id();
        metaPartition->interval = m_partitions[curRootIdx]->GetInterval();
        std::vector<uint32_t> childrenIds = m_partitions[curRootIdx]->GetChildrenIds();
        if (!childrenIds.empty()) {
            metaPartition->children.resize(childrenIds.size());
            for (size_t i = 0; i < childrenIds.size(); ++i) {
                // 递归收集 ShardTree 各层的 interval 信息
                s = CollectMetaPartition(childrenIds[i], &metaPartition->children[i]);
                if (!s.ok()) { return s; }
            }
        }
        return s;
    }

    Status ShardTree::CreateNewEdgeLabel(
            const EdgeLabel &label, EdgeTag_t tag, EdgeTag_t src_tag, EdgeTag_t dst_tag) {
        Status s;
        for (size_t i = 0; i < m_partitions.size(); ++i) {
            s = m_partitions[i]->CreateNewEdgeLabel(label, tag, src_tag, dst_tag);
            if (!s.ok()) { return s; }
        }
        return s;
    }

    Status ShardTree::CreateEdgeAttrCol(const EdgeLabel &label, const ColumnDescriptor &config) {
        Status s;
        for (size_t i = 0; i < m_partitions.size(); ++i) {
            s = m_partitions[i]->CreateEdgeAttrCol(label, config);
            if (!s.ok()) { return s; }
        }
        // TODO 创建属性列后 check 数据是否需要进行分裂
        return s;
    }

    Status ShardTree::DeleteEdgeAttrCol(const EdgeLabel &label, const std::string &columnName) {
        Status s;
        for (size_t i = 0; i < m_partitions.size(); ++i) {
            s = m_partitions[i]->DeleteEdgeAttrCol(label, columnName);
            if (!s.ok()) { return s; }
        }
        return s;
    }

    Status ShardTree::DoFlush() {
        Status s;
        // flush 任务
        while (!m_flush_queue.empty()) {
            // pop one partition that need to flush
            EdgePartitionPtr partition = m_flush_queue.front();
            m_flush_queue.pop_front();
            // do flush
            for (auto &sub: *partition.get()) {
                Compaction *compaction = new MemoryTableCompaction(std::static_pointer_cast<SubEdgePartitionWithMemTable>(sub));
                s = compaction->Run();
                delete compaction;
                if (!s.ok()) { break; }
            }
        }
        return s;
    }

    Status ShardTree::DoCompaction() {
        Status s;
        // compaction 任务
        while (!m_compaction_queue.empty()) {
            // pop one partition that need to compact
            EdgePartitionPtr partition = m_compaction_queue.front();
            m_compaction_queue.pop_front();
            std::vector<uint32_t> childrenIds = partition->GetChildrenIds();
            bool isSplit = false;
            if (childrenIds.empty()) {
                // split compaction
                isSplit = true;
                // TODO 分配 children id
                uint32_t curr_max_id = 0;
                for (size_t i = 0; i < m_partitions.size(); ++i) {
                    curr_max_id = std::max(m_partitions[i]->id(), curr_max_id);
                }
                for (size_t i = 0; i < m_options.shard_split_factor; ++i) {
                    childrenIds.emplace_back(++curr_max_id);
                }
            } else {
                // level compaction
                isSplit = false;
                // TODO 找出 children
            }
            Compaction *compaction = nullptr;
            // do compact
            for (auto &sub: *partition.get()) {
                if (isSplit) {
                    compaction = new SplitCompaction(m_options, sub, childrenIds);
                } else {
                    compaction = new LevelCompaction(m_options, sub, {nullptr, nullptr, nullptr, nullptr});
                }
                s = compaction->Run();
                delete compaction; compaction = nullptr;
                if (!s.ok()) { return s; }
            }
        }
        return s;
    }

    Status ShardTree::ExportData(const std::string &outDir, std::shared_ptr<IDEncoder> encoder) {
        Status s;
        for (const auto &partition : m_partitions) {
            s = partition->ExportData(outDir, encoder);
            if (!s.ok()) { return s; }
        }
        return s;
    }

}

