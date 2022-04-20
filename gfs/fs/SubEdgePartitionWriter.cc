#include <iostream>
#include <numeric>
#include "preprocessing/types.h"
#include "fs/ColumnDescriptorUtils.h"
#include "fs/SubEdgePartitionWriter.h"

#include "fs/IdxFileWriter.h"

namespace skg {

    Status SubEdgePartitionWriter::FlushEdges(
            std::vector<MemoryEdge> &&buffered_edges,
            const std::string &storage_dir,
            uint32_t shard_id, uint32_t partition_id,
            const interval_t &interval,
            const MetaAttributes &attributes) {
        // sort
//        metrics::GetInstance()->start_time("SubEdgePartitionWriter.FlushEdges.sort");
        std::sort(buffered_edges.begin(), buffered_edges.end(), MemoryEdgeSortedFunc());
        //SKG_LOG_DEBUG("Sort done.", "");
//        metrics::GetInstance()->stop_time("SubEdgePartitionWriter.FlushEdges.sort");
        // 去除重复边
//        metrics::GetInstance()->start_time("SubEdgePartitionWriter.FlushEdges.uniq");
        //size_t size_with_dup = buffered_edges.size();
        buffered_edges = RemoveDuplicateEdges(std::move(buffered_edges));
        //SKG_LOG_DEBUG("Removed duplicated edges. size {}/{}, dup: {}",
        //              size_with_dup, buffered_edges.size(), size_with_dup - buffered_edges.size());
//        metrics::GetInstance()->stop_time("SubEdgePartitionWriter.FlushEdges.uniq");

        // persist to disk
        Status s;
//        metrics::GetInstance()->start_time("SubEdgePartitionWriter.FlushEdges.create");
        SKG_LOG_DEBUG("Creating shard with stack method...", "");
        EdgeListFileWriter edges_list_f(storage_dir, shard_id, partition_id, interval, attributes.label_tag);
        s = edges_list_f.Open();
        if (!s.ok()) { return s; }
//        SKG_LOG_DEBUG("building next link.", "");
        // Note: 使用 std::vector 作为std::stack的容器; 因若使用默认的std::deque, 创建3000W个stack, 会申请约16G空间
        std::vector<std::stack<idx_t, std::vector<idx_t> > > next_indices(interval.GetNumVertices());
//        SKG_LOG_DEBUG("stack array allocated", "");
        for (idx_t i = static_cast<idx_t>(buffered_edges.size()); i > 0; --i) {
            const MemoryEdge &edge = buffered_edges[i - 1];
            next_indices[interval.GetIndex(edge.dst)].push(i - 1);
        }
//        SKG_LOG_DEBUG("next link built.", "");

        {// 写入dst-index
//            SKG_LOG_DEBUG("writing first in offset.", "");
            IndexFileWriter dst_idx_f(FILENAME::sub_partition_dst_idx(edges_list_f.filename()));
            s = dst_idx_f.Open();
            if (!s.ok()) {
                return s;
            }

            // write first dst offset index
            for (vid_t dst = interval.first; dst <= interval.second; ++dst) {
                const vid_t dstIndex = interval.GetIndex(dst);
                if (next_indices[dstIndex].empty()) { continue; }
                const idx_t first_in_idx = next_indices[dstIndex].top();
                next_indices[dstIndex].pop();
                dst_idx_f.write(dst, first_in_idx);
            }
//            SKG_LOG_DEBUG("first in offset done.", "");
        }

        IndexFileWriter src_idx_f(FILENAME::sub_partition_src_idx(edges_list_f.filename()));
        // 边属性列文件
        std::vector<std::unique_ptr<IEdgeColumnPartitionWriter>> edata_cols_f;
        edata_cols_f.reserve(attributes.GetColumnsSize());
        for (const auto &col: attributes) {
            // tag, 权重与边整合存储, 不需要另外写磁盘
            if (col.columnType() == ColumnType::TAG || col.columnType() == ColumnType::WEIGHT) {
                continue;
            }
            std::unique_ptr<IEdgeColumnPartitionWriter> writer;
            s = ColumnDescriptorUtils::CreateWriter(
                    col, edges_list_f.filename(), &writer);
            // create edge column files error.
            if (!s.ok()) { return s; }
            edata_cols_f.emplace_back(std::move(writer));
        }
        // src索引文件
        s = src_idx_f.Open();
        if (!s.ok()) { return s; }
        vid_t curvid = 0;
        idx_t istart = 0;
        for (idx_t i = 0; i <= buffered_edges.size(); ++i) {
            if ((i != 0 && i % 50000000 == 0) || i == buffered_edges.size()) {
                SKG_LOG_DEBUG("flushing {}/{} ({:.2f}%)",
                              i, buffered_edges.size(),
                              100.0 * i / buffered_edges.size());
            }
            const MemoryEdge &edge = (
                    i < buffered_edges.size()?
                    buffered_edges[i]:
                    MemoryEdge::GetStopper());

            // write edge data
            if (!edge.IsStopper()) {
                size_t offset = 0;
                for (const auto &col: edata_cols_f) {
                    s = col->Write(edge.GetColsData().data() + offset);
                    if (!s.ok()) { return s; }
                    offset += col->value_size();
                }
            }

            if ((edge.src != curvid) || edge.IsStopper()) {
                // New vertex
                // write source offset index
                const idx_t cur_vid_num_edges = i - istart;

                if (cur_vid_num_edges > 0) {
                    src_idx_f.write(curvid, istart);
                }

                // Write edge-array file
                for (idx_t j = istart; j < i; ++j) {
                    const MemoryEdge &buffered_edge = buffered_edges[j];

                    // get next offset
                    idx_t next_dst_idx = INDEX_NOT_EXIST;
                    const vid_t dstIndex = interval.GetIndex(buffered_edge.dst);
                    if (!next_indices[dstIndex].empty()) {
                        next_dst_idx = next_indices[dstIndex].top();
                        next_indices[dstIndex].pop();
                    }

                    PersistentEdge store_edge(
                            buffered_edge.src, buffered_edge.dst,
                            buffered_edge.weight, attributes.label_tag,
                            next_dst_idx);
                    store_edge.CopyFrom(buffered_edge);
                    edges_list_f.add_edge(store_edge);
                }

                // move curvid into new src
                curvid = edge.src;
                istart = i;  // mark curvid start index
            }
        }
        // write edge data
        for (const auto &col: edata_cols_f) {
            s = col->Flush();
            if (!s.ok()) { return s; }
            s = col->CreateSizeRecord();
            if (!s.ok()) { return s; }
        }

//        metrics::GetInstance()->stop_time("SubEdgePartitionWriter.FlushEdges.create");
        return Status::OK();
    }

    Status SubEdgePartitionWriter::FlushEdgesSortTwice(
            std::vector<MemoryEdge> &&buffered_edges,
            const std::string &storage_dir,
            uint32_t shard_id, uint32_t partition_id,
            const interval_t &interval,
            const MetaAttributes &attributes) {
        // sort
//        metrics::GetInstance()->start_time("SubEdgePartitionWriter.FlushEdges.sort");
        std::sort(buffered_edges.begin(), buffered_edges.end(), MemoryEdgeSortedFunc());
        //SKG_LOG_DEBUG("Sort done.", "");
//        metrics::GetInstance()->stop_time("SubEdgePartitionWriter.FlushEdges.sort");
        // 去除重复边
//        metrics::GetInstance()->start_time("SubEdgePartitionWriter.FlushEdges.uniq");
        //size_t size_with_dup = buffered_edges.size();
        buffered_edges = RemoveDuplicateEdges(std::move(buffered_edges));
        //SKG_LOG_DEBUG("Removed duplicated edges. size {}/{}, dup: {}",
        //              size_with_dup, buffered_edges.size(), size_with_dup - buffered_edges.size());
//        metrics::GetInstance()->stop_time("SubEdgePartitionWriter.FlushEdges.uniq");

        SKG_LOG_DEBUG("Creating shard with sort-twice method...", "");
        // 创建0~size-1的位置数组
        std::vector<idx_t> indices(buffered_edges.size());
        std::iota(indices.begin(), indices.end(), 0);
        // 获取edges根据dst排序时的位置
        skg::preprocess::sorted_indexes(
                buffered_edges.data(),
                MemoryEdgeDstLessFunc(),
                &indices
        );

        // persist to disk
//        metrics::GetInstance()->start_time("SubEdgePartitionWriter.FlushEdges.create");
        EdgeListFileWriter edge_list_f(storage_dir, shard_id, partition_id, interval, attributes.label_tag);
        Status s = edge_list_f.Open();
        if (!s.ok()) { return s; }
        IndexFileWriter dst_idx_f(FILENAME::sub_partition_dst_idx(edge_list_f.filename()));
        s = dst_idx_f.Open();
        if (!s.ok()) { return s; }
        SKG_LOG_DEBUG("Writing dst indices.", "");
        std::vector<std::pair<const MemoryEdge*, idx_t>> edges_with_next_offset(buffered_edges.size());
        for (size_t i = 0; i < indices.size() - 1; ++i) {
            const MemoryEdge &cur_edge = buffered_edges[indices[i]];
            const MemoryEdge &next_edge = buffered_edges[indices[i + 1]];
            // 写第一个in-edges的offset到文件中
            if (i == 0) { // 第一条边, 写入
                dst_idx_f.write(cur_edge.dst, indices[i]);
            }
            if (cur_edge.dst != next_edge.dst) { // 到dst的分界点, 写入
                dst_idx_f.write(next_edge.dst, indices[i + 1]);
            }
            // 构建到下一个dst的offset
            if (cur_edge.dst == next_edge.dst) {
                edges_with_next_offset[indices[i]] = std::make_pair(&cur_edge, indices[i + 1]);
            } else {
                edges_with_next_offset[indices[i]] = std::make_pair(&cur_edge, INDEX_NOT_EXIST);
            }
        }
        idx_t last_index = indices[indices.size() - 1];
        edges_with_next_offset[last_index] = std::make_pair(&buffered_edges[last_index], INDEX_NOT_EXIST);

        SKG_LOG_DEBUG("Writing src indices.","");
        IndexFileWriter src_idx_f(FILENAME::sub_partition_src_idx(edge_list_f.filename()));
        s = src_idx_f.Open();
        if (!s.ok()) { return s; }
        // 边属性列文件
        std::vector<std::unique_ptr<IEdgeColumnPartitionWriter>> edata_cols_f;
        edata_cols_f.reserve(attributes.GetColumnsSize());
        for (const auto &col: attributes) {
            // tag, 权重与边整合存储, 不需要另外写磁盘
            if (col.columnType() == ColumnType::TAG || col.columnType() == ColumnType::WEIGHT) {
                continue;
            }
            std::unique_ptr<IEdgeColumnPartitionWriter> writer;
            s = ColumnDescriptorUtils::CreateWriter(
                    col, edge_list_f.filename(), &writer);
            // create edge column files error.
            if (!s.ok()) { return s; }
            edata_cols_f.emplace_back(std::move(writer));
        }
        vid_t cur_src = 0;
        for (idx_t i = 0; i < edges_with_next_offset.size(); ++i) {
            const MemoryEdge &cur_edge = *(edges_with_next_offset[i].first);
            // 写out-edges的index
            if (i == 0 || cur_src != cur_edge.src) {
                cur_src = cur_edge.src;
                src_idx_f.write(cur_src, i);
            }
            // 写adj-file, 下一个dst的offset
            PersistentEdge edge_to_write(cur_edge.src, cur_edge.dst, cur_edge.weight, cur_edge.tag, edges_with_next_offset[i].second);
            edge_to_write.CopyFrom(cur_edge);
            edge_list_f.add_edge(edge_to_write);
            // write edge data
            if (!cur_edge.IsStopper()) {
                size_t offset = 0;
                for (const auto &col: edata_cols_f) {
                    s = col->Write(cur_edge.GetColsData().data() + offset);
                    if (!s.ok()) { return s; }
                    offset += col->value_size();
                }
            }
        }
        // write edge data
        for (const auto &col: edata_cols_f) {
            s = col->Flush();
            if (!s.ok()) { return s; }
            s = col->CreateSizeRecord();
            if (!s.ok()) { return s; }
        }
//        metrics::GetInstance()->stop_time("SubEdgePartitionWriter.FlushEdges.create");
        return s;
    }


    std::vector<MemoryEdge> SubEdgePartitionWriter::RemoveDuplicateEdges(std::vector<MemoryEdge> &&edges) {
        if (edges.empty()) {// trivial case
            return edges;
        }
        std::vector<MemoryEdge> uniqEdges;
        uniqEdges.reserve(edges.size());
        uniqEdges.emplace_back(edges[0]);
        for (idx_t i = 1; i < edges.size(); ++i) {
            const MemoryEdge &preEdge = uniqEdges.back();
            MemoryEdge &curEdge = edges[i];
            // src, dst, tag 相同, 才看做是同一条边
            if (preEdge.src == curEdge.src && preEdge.dst == curEdge.dst
                && preEdge.tag == curEdge.tag) {
                SKG_LOG_TRACE("Detected duplicate edge: {} -> {}, tag: {}",
                                curEdge.src, curEdge.dst, curEdge.tag);
                // TODO if cur edge is newer than pre edge, replace pre edge
                // TODO 暂时不存在边属性, 不需要做替换
            } else {
                uniqEdges.emplace_back(std::move(curEdge));
            }
        }
        return uniqEdges;
    }

}
