#ifndef STARKNOWLEDGEGRAPHDATABASE_LEVELCOMPACTIONBUILDER_H
#define STARKNOWLEDGEGRAPHDATABASE_LEVELCOMPACTIONBUILDER_H

#include <cstdint>

#include "util/types.h"
#include "util/options.h"

#include "env/env.h"
#include "util/internal_types.h"
#include "fs/MetaAttributes.h"
#include "fs/SubEdgePartition.h"
#include "fs/SubEdgePartitionWithMemTable.h"
#include "fs/SubEdgePartitionWriter.h"

namespace skg {

class Compaction {
public:
    Compaction() {
    }

    virtual
    ~Compaction() {
    }

    virtual Status Run() = 0;
public:
    Compaction(const Compaction &) = delete;
    Compaction& operator=(const Compaction &) = delete;
};

class MemoryTableCompaction : public Compaction {
public:
    MemoryTableCompaction(const std::shared_ptr<SubEdgePartitionWithMemTable> &partition) : m_partition(partition) {
    }

    Status Run() override {
        Status s;
        // FIXME Problem1. Flush 过程导致阻塞.
        // FIXME -> 通过其他线程进行 Flush 操作

        // FIXME Problem2. Flush 过程读操作. MergeEdgesAndFlush过程, 关闭文件句柄, 导致 SubEdgePartition 对外不可用.
        // FIXME -> ? Mark 为只读操作. 磁盘修改通过 SubEdgePartition::Flush 同步到磁盘后, 从磁盘中读取内容, 与 MemTable 合并, 刷盘

        // FIXME Problem3. Flush 过程写操作.
        // FIXME -> ? 通过维护LOG, 待Flush完成后把LOG中的修改MERGE到新的 SubEdgePartition 中

        assert(m_partition->label() == m_partition->GetMemTable()->GetLabel());
        // 空, 直接返回
        if (m_partition->GetMemTable()->GetNumEdges() == 0) {
            return s;
        }
        interval_t interval;  // MemTable 中的interval, 可能会扩展当前 SubEdgePartition 的 interval
        std::vector<MemoryEdge> edges;
        s = m_partition->GetMemTable()->ExtractEdges(&edges, &interval);
        if (!s.ok()) { return s; }

        s = m_partition->MergeEdgesAndFlush(std::move(edges), interval);
        if (!s.ok()) { return s; }

        return s;
    }

private:
    std::shared_ptr<SubEdgePartitionWithMemTable> m_partition;
};

class LevelCompaction : public Compaction {
public:

    LevelCompaction(
            const Options &options,
            const SubEdgePartitionPtr &partition,
            const std::vector<SubEdgePartitionPtr> &children)
            : m_options(options),
              m_partition(partition),
              m_children(children),
              m_is_need_ensure_uniq(false) {
        assert(!children.empty());
    }

    Status Run() override {
        Status s;
        if (m_partition->GetNumEdges() == 0) {
            return s;
        }
        // 刷磁盘
        s = m_partition->FlushCache(true);
        if (!s.ok()) { return s; }
        // TODO 获取 Partition 的写锁

        // 读入 Partition 的所有边 && 属性数据
        std::vector<MemoryEdge> edges = m_partition->LoadAllEdges();
        // 按dst排序
        std::sort(edges.begin(), edges.end(), MemoryEdgeDstLessFunc());
        // 去除重复边
        if (m_is_need_ensure_uniq) {
            edges = SubEdgePartitionWriter::RemoveDuplicateEdges(std::move(edges));
        }
        // 按照叶子节点的 interval, 把边划分
        std::vector<std::vector<MemoryEdge>> leave_edges_to_merge(m_children.size());
        std::vector<interval_t> leave_intervals(m_children.size());
        for (size_t i = 0; i < m_children.size(); ++i) {
            // interval
            leave_intervals[i] = m_children[i]->GetInterval();
            // 预分配 vector 空间
            leave_edges_to_merge.reserve(edges.size() / m_children.size());
        }
        for (ssize_t i = edges.size() - 1; i >= 0; --i) {
            const MemoryEdge &curEdge = edges[i];
            for (size_t j = 0; j < m_children.size(); ++j) {
                if (m_children[j]->GetInterval().Contain(curEdge.dst) || j == m_children.size() - 1) {
                    leave_intervals[j].ExtendTo(curEdge.dst);
                    leave_edges_to_merge[j].emplace_back(curEdge);
                    break;
                }
            }
        }
        edges.clear(); edges.shrink_to_fit();
        // 数据合并到子区间, 重新排序, 刷磁盘
        for (size_t i = 0; i < m_children.size(); ++i) {
            SKG_LOG_INFO("Flushing {} edges to child partition: id[{}-{}] interval[{}]",
                         leave_edges_to_merge[i].size(),
                         m_children[i]->shard_id(), m_children[i]->id(),
                         leave_intervals[i]);
            s = m_children[i]->MergeEdgesAndFlush(
                    std::move(leave_edges_to_merge[i]),
                    leave_intervals[i]);
            if (!s.ok()) { return s; }
        }
        if (s.ok()) {
            // 清空原来的 Partition 数据
            s = m_partition->TruncatePartition();
        }
        return s;
    }

private:
    Options m_options;
    SubEdgePartitionPtr m_partition;
    std::vector<SubEdgePartitionPtr> m_children;
    bool m_is_need_ensure_uniq;
};

class SplitCompaction : public Compaction {
public:
    SplitCompaction(
            const Options &options,
            const SubEdgePartitionPtr &partition,
            const std::vector<uint32_t> &childrenIds)
        : m_options(options),
          m_partition(partition),
          m_childrenIds(childrenIds),
          m_is_need_ensure_uniq(false) {
        assert(!m_childrenIds.empty());
    }

    Status Run() override {
        Status s;
        if (m_partition->GetNumEdges() == 0) {
            return s;
        }
        s = m_partition->FlushCache(true);
        if (!s.ok()) { return s;}
        // TODO 获取 Partition 的写锁

        // 读入 Partition 的所有边 && 属性数据
        std::vector<MemoryEdge> edges = m_partition->LoadAllEdges();
        // 按dst排序
        std::sort(edges.begin(), edges.end(), MemoryEdgeDstLessFunc());
        // 去除重复边
        if (m_is_need_ensure_uniq) {
            edges = SubEdgePartitionWriter::RemoveDuplicateEdges(std::move(edges));
        }
        SKG_LOG_INFO("Splitting partition[{}], {} edges, -> {} parts",
                     m_partition->GetInterval(), edges.size(),
                     m_options.shard_split_factor);
        // 统计 dst-in-degree 计算拆分点
        // 生成叶子节点, 并把数据刷磁盘
        s = SplitShard(std::move(edges));
        if (!s.ok()) { return s; }
        // 清空原来的 Partition 数据
        s = m_partition->TruncatePartition();
        return s;
    }

    Status SplitShard(std::vector<MemoryEdge> &&edges) {
        Status s;
        const size_t num_children_to_split = m_options.shard_split_factor;
        size_t num_created_child = 0;
        // 每个子区间至少的边数
        size_t num_avg_edges = static_cast<size_t>(1.0 * edges.size() / num_children_to_split);
        size_t num_cur_split_edges = 0;  // 当前子区间实际边数

        // 遍历边集，找到拆分点
        size_t splitStartEdgeIdx = 0;
        vid_t curDst = edges[0].dst;
        vid_t split_interval_beg = m_partition->GetInterval().first;
        for (size_t i = 0; i < edges.size(); ++i) {
            if (i % 10000000 == 0) {
                SKG_LOG_DEBUG("Getting: {}, {}/{}", i, num_cur_split_edges, num_avg_edges);
            }
            num_cur_split_edges++;
            if (curDst != edges[i].dst || i == edges.size() - 1) {
                if (num_cur_split_edges >= num_avg_edges || i == edges.size() - 1) {
                    // 分裂为一个shard
                    // 刷到磁盘 TODO 使用deque拆分?或者排序时按照降序排，然后pop
                    std::vector<MemoryEdge> flush_edges(
                            edges.begin() + splitStartEdgeIdx,
                            edges.begin() + ((i!=edges.size()-1)?i:i+1));
                    s = this->CreateChild(
                            m_childrenIds[num_created_child++],
                            {split_interval_beg,
                             ((i!=edges.size()-1)?curDst:m_partition->GetInterval().second)},
                            std::move(flush_edges));
                    if (i != edges.size() - 1) {
                        split_interval_beg = curDst + 1;
                        // adjust num_avg_edges in next children's partition
                        const size_t num_non_split_edges = edges.size() - i; // 未分到子shard的edges数量
                        num_avg_edges = static_cast<size_t>(
                                (1.0 * num_non_split_edges) /
                                (num_children_to_split - num_created_child));
                        SKG_LOG_DEBUG("{} edges left. adjust avg-edges {} for {} partition",
                                      num_non_split_edges, num_avg_edges,
                                      num_children_to_split - num_created_child);
                        num_cur_split_edges = 0;
                        splitStartEdgeIdx = i;
                    }
                }
                // update cur dst
                curDst = edges[i].dst;
            }
        }
        if (num_children_to_split != num_created_child) {
            s = Status::NotImplement(fmt::format(
                    "can not split into {} children. only {}.",
                    num_children_to_split, num_created_child));
        }
        if (s.ok()) {
            // truncate partition 文件
            s = m_partition->TruncatePartition();
        }
        if (!s.ok()) { return s; }
        SKG_LOG_INFO("Split partition[{}] done.", m_partition->GetInterval());
        return s;
    }

    Status CreateChild(uint32_t partition_id, interval_t interval, std::vector<MemoryEdge> &&edges) {
        Status s;
        SKG_LOG_DEBUG("Creating shard-partition: id[{}]-[{}], interval[{}] with {} edges.",
                      m_partition->shard_id(), partition_id,
                      interval, edges.size());
        s = SubEdgePartition::Create(
                m_partition->GetStorageDir(),
                m_partition->shard_id(), partition_id, interval, m_partition->attributes());
        s = SubEdgePartitionWriter::FlushEdges(
                std::move(edges), m_partition->GetStorageDir(),
                m_partition->shard_id(), partition_id, interval,
                m_partition->attributes()
        );
        assert(s.ok());
        return s;
    }

private:
    Options m_options;
    SubEdgePartitionPtr m_partition;
    std::vector<uint32_t> m_childrenIds;
    bool m_is_need_ensure_uniq;
};

};

#endif //STARKNOWLEDGEGRAPHDATABASE_LEVELCOMPACTIONBUILDER_H
