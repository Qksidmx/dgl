#include <sstream>
#include <ctime>
#include "fmt/time.h"
#include "env/env.h"
#include "util/types.h"
#include "util/internal_types.h"

#include "VecMemTable.h"
#include "HashMemTable.h"
#include "SubEdgePartition.h"
#include "SubEdgePartitionWriter.h"
#include "SubEdgePartitionWithMemTable.h"

namespace skg {

    bool SubEdgePartition::IsPartitionExist(
            const std::string &dirname,
            uint32_t shard_id, uint32_t partition_id,
            const interval_t &interval, const EdgeTag_t tag) {
        const std::string partition_dir = DIRNAME::sub_partition(dirname, shard_id, partition_id, interval, tag);
        if (!PathUtils::FileExists(partition_dir)) {
            return false;
        }
        const std::string elist = FILENAME::sub_partition_edgelist(dirname, shard_id, partition_id, interval, tag);
        if (!PathUtils::FileExists(elist)) {
            return false;
        }
        std::string filename = FILENAME::sub_partition_dst_idx(elist);
        if (!PathUtils::FileExists(filename)) {
            return false;
        }
        filename = FILENAME::sub_partition_src_idx(elist);
        if (!PathUtils::FileExists(filename)) {
            return false;
        }
        return true;
    }

    Status SubEdgePartition::DropPartition(
            const std::string &prefix,
            uint32_t shard_id, uint32_t partition_id,
            const interval_t &interval, const EdgeTag_t tag) {
        Status s;
        const std::string elist = FILENAME::sub_partition_edgelist(prefix, shard_id, partition_id, interval, tag);
        s = PathUtils::RemoveFile(elist);
        if (!s.ok()) { return s; }
        s = PathUtils::RemoveFile(FILENAME::sub_partition_src_idx(elist));
        if (!s.ok()) { return s; }
        s = PathUtils::RemoveFile(FILENAME::sub_partition_dst_idx(elist));
        if (!s.ok()) { return s; }
        // 删除列属性文件
        s = PathUtils::RemoveFile(DIRNAME::sub_partition_edge_columns(prefix, shard_id, partition_id, interval, tag));
        if (!s.ok()) { return s; }
        // 删除 sub-partition 存储的文件夹
        s = PathUtils::RemoveFile(DIRNAME::sub_partition(prefix, shard_id, partition_id, interval, tag));
        return s;
    }

    Status SubEdgePartition::Create(
            const std::string &prefix,
            uint32_t shard_id, uint32_t partition_id,
            const interval_t &interval, const MetaAttributes &attributes) {
        if (SubEdgePartition::IsPartitionExist(prefix, shard_id, partition_id, interval, attributes.label_tag)) {
            const std::string partition_dir = DIRNAME::sub_partition(prefix, shard_id, partition_id, interval, attributes.label_tag);
            return Status::InvalidArgument(
                    fmt::format("SubPartition: `{}' already exist.", partition_dir));
        }
        Status s;
        // create dir for edge-partition
        const std::string partition_dir = DIRNAME::sub_partition(
                prefix, shard_id, partition_id,
                interval, attributes.label_tag);
        SKG_LOG_DEBUG("Creating sub partition, "
                      "shard-id: {}, partition-id: {}, interval: {}, label: {}-{}-{}",
                      shard_id, partition_id, interval,
                      attributes.src_label, attributes.label, attributes.dst_label);
        s = PathUtils::CreateDirIfMissing(partition_dir);
        if (!s.ok()) { return s; }
        // create empty files
        EdgeListFileWriter edges_list_f(prefix, shard_id, partition_id, interval, attributes.label_tag);
        s = edges_list_f.Open();
        if (!s.ok()) { return s; }
        IndexFileWriter dst_idx_f(FILENAME::sub_partition_dst_idx(edges_list_f.filename()));
        s = dst_idx_f.Open();
        if (!s.ok()) { return s; }
        IndexFileWriter src_idx_f(FILENAME::sub_partition_src_idx(edges_list_f.filename()));
        s = src_idx_f.Open();
        if (!s.ok()) { return s; }
        PathUtils::CreateDirIfMissing(
                DIRNAME::sub_partition_edge_columns(
                        prefix, shard_id, partition_id, interval, attributes.label_tag));
        // create empty edge columns
        for (const auto &col: attributes) {
            if (col.columnType() == ColumnType::TAG || col.columnType() == ColumnType::WEIGHT) {
                continue;
            }
            std::unique_ptr <IEdgeColumnPartitionWriter> writer;
            s = ColumnDescriptorUtils::CreateWriter(col, edges_list_f.filename(), &writer);
            if (!s.ok()) { return s; }
            s = writer->Flush();
            if (!s.ok()) { return s; }
            s = writer->CreateSizeRecord();
            if (!s.ok()) { return s; }
        }
        return s;
    }

    Status SubEdgePartition::Open(
            const std::string &prefix,
            uint32_t shard_id, uint32_t partition_id,
            const interval_t &interval,
            const MetaAttributes &attributes,
            const Options &options,
            std::shared_ptr <SubEdgePartition> *pShard) {
        assert(*pShard == nullptr);
        if (!SubEdgePartition::IsPartitionExist(prefix, shard_id, partition_id, interval, attributes.label_tag)) {
            return Status::FileNotFound(fmt::format(
                    "SubEdgePartition: {}--{}-{}-{}",
                    interval,
                    attributes.src_label, attributes.label, attributes.dst_label));
        }
        std::shared_ptr <SubEdgePartition> partition;
        if (partition_id == 0 && shard_id != 0) {
            partition = std::make_shared<SubEdgePartitionWithMemTable>(
                    prefix,
                    shard_id, partition_id,
                    interval, attributes, options);
            if (options.mem_table_type == Options::MemTableType::Vec) {
                std::static_pointer_cast<SubEdgePartitionWithMemTable>(partition)->m_memTable = \
                std::unique_ptr<MemTable>(new VecMemTable(interval, attributes, options));
            } else {
                std::static_pointer_cast<SubEdgePartitionWithMemTable>(partition)->m_memTable = \
                std::unique_ptr<MemTable>(new HashMemTable(interval, attributes, options));
            }
        } else {
            partition = std::make_shared<SubEdgePartition>(
                    prefix,
                    shard_id, partition_id,
                    interval, attributes, options);
        }
        Status s = partition->OpenHandlers();
        if (!s.ok()) { return s; }
        // open column handlers
        IEdgeColumnPartitionPtr fragment;
        for (const auto &column : attributes) {
            if (column.columnType() == ColumnType::TAG || column.columnType() == ColumnType::WEIGHT) {
                continue;
            }
            s = ColumnDescriptorUtils::CreatePartition(
                    column,
                    prefix, shard_id, partition_id,
                    partition->GetInterval(), attributes.label_tag,
                    &fragment);
            if (!s.ok()) { return s; }
            partition->m_columns.emplace_back(std::move(fragment));
        }
        partition->ReferByOptions(options);
        if (s.ok()) {
            *pShard = std::move(partition);
        } else {
            partition.reset();
        }
        return s;
    }

    SubEdgePartition::~SubEdgePartition() {
        // 数据刷到磁盘
        (void) this->FlushCache(true);
    }

    Status SubEdgePartition::Drop() {
        Status s;
        s = this->CloseHandlers();
        if (!s.ok()) { return s; }
        s = DropPartition(m_storage_dir, m_shard_id, m_partition_id, GetInterval(), m_attributes.label_tag);
        if (!s.ok()) { return s; }
        m_edge_list_f.reset();
        m_src_index_f.reset();
        m_dst_index_f.reset();
        m_columns.clear();
        return s;
    }

    size_t SubEdgePartition::GetEstimateSize() const {
        size_t bytes_per_edge = sizeof(PersistentEdge) + m_attributes.GetColumnsValueByteSize();
        return GetNumEdges() * bytes_per_edge;
    }

    Status SubEdgePartition::DeleteVertex(const VertexRequest &request) {
        Status s;
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};
        // delete vertex's out-edges
        idx_t idx = 0;
        auto idx_window = m_src_index_f->GetOutIdxRange(request.m_vid);
        if (idx_window.first != INDEX_NOT_EXIST) {
            for (idx = idx_window.first; idx < idx_window.second && idx < m_edge_list_f->num_edges(); ++idx) {
                PersistentEdge *edge = m_edge_list_f->GetMutableEdge(idx, edgeBuf);
                edge->SetDelete();
                s = m_edge_list_f->Set(idx, edge);
            }
        }
        // delete vertex's in-edges
        idx = m_dst_index_f->GetFirstInIndex(request.m_vid);
        if (idx != INDEX_NOT_EXIST) {
            do {
                PersistentEdge *edge = m_edge_list_f->GetMutableEdge(idx, edgeBuf);
                edge->SetDelete();
                s = m_edge_list_f->Set(idx, edge);
                idx = edge->next();
            } while (idx != INDEX_NOT_EXIST);
        }
        return s;
    }

    /**
     * 读取所有到dst的入边
     * @param dst
     * @param pQueryResult
     */
    Status SubEdgePartition::GetInEdges(const VertexRequest &req, EdgesQueryResult *pQueryResult) const {
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};
        char colData[SKG_MAX_EDGE_PROPERTIES_BYTES];
        PropertiesBitset_t bitset;
//        metrics::GetInstance()->start_time("SubEdgePartition.GetInEdges", metric_duration_type::MILLISECONDS);
//        metrics::GetInstance()->start_time("SubEdgePartition.GetInEdges.GetDstIdx", metric_duration_type::MILLISECONDS);
        // TODO: LRU 对经常查询的边做缓存
        idx_t idx = m_dst_index_f->GetFirstInIndex(req.m_vid);
//        metrics::GetInstance()->stop_time("SubEdgePartition.GetInEdges.GetDstIdx");
        Status s;
        while (idx != INDEX_NOT_EXIST) {
            memset(colData, 0, SKG_MAX_EDGE_PROPERTIES_BYTES);
//            metrics::GetInstance()->start_time("SubEdgePartition.GetInEdges.GetEdge", metric_duration_type::MILLISECONDS);
            const PersistentEdge &edge = m_edge_list_f->GetImmutableEdge(idx, edgeBuf);
//            metrics::GetInstance()->stop_time("SubEdgePartition.GetInEdges.GetEdge");
            if (!edge.deleted()) {  // 忽略被删除的边
                // get edge data
//                metrics::GetInstance()->start_time("SubEdgePartition.GetInEdges.GetEdgeProp", metric_duration_type::MILLISECONDS);
                s = CollectProperties(edge, idx, req.m_columns, colData, &bitset);
//                metrics::GetInstance()->stop_time("SubEdgePartition.GetInEdges.GetEdgeProp");
                if (!s.ok()) { return s; }
                // put to result
                assert(edge.tag == m_attributes.label_tag);
//                metrics::GetInstance()->start_time("SubEdgePartition.GetInEdges.ReceiveEdge", metric_duration_type::MILLISECONDS);
                s = pQueryResult->ReceiveEdge(
                        edge.src, edge.dst,
                        edge.weight, edge.tag,
                        colData, m_attributes.GetColumnsValueByteSize(),
                        bitset
                );
//                metrics::GetInstance()->stop_time("SubEdgePartition.GetInEdges.ReceiveEdge");
            }
            idx = edge.next();
        }
//        metrics::GetInstance()->stop_time("SubEdgePartition.GetInEdges");
        return s;
    }

    /**
     * 读取所有src出发的出边
     * @param src
     * @param pQueryResult
     */
    Status SubEdgePartition::GetOutEdges(const VertexRequest &req, EdgesQueryResult *pQueryResult) const {
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};
        char colData[SKG_MAX_EDGE_PROPERTIES_BYTES];
        PropertiesBitset_t bitset;
        // TODO: LRU 对经常查询的边做缓存
//        metrics::GetInstance()->start_time("SubEdgePartition.GetOutEdges", metric_duration_type::MILLISECONDS);
//        metrics::GetInstance()->start_time("SubEdgePartition.GetOutEdges.GetSrcIdx",metric_duration_type::MILLISECONDS);
        auto idx_window = m_src_index_f->GetOutIdxRange(req.m_vid);
//        metrics::GetInstance()->stop_time("SubEdgePartition.GetOutEdges.GetSrcIdx");
        Status s;
        if (idx_window.first != INDEX_NOT_EXIST) {  // 索引中找到src范围
            for (idx_t idx = idx_window.first;
                 idx < idx_window.second && idx < m_edge_list_f->num_edges();
                 ++idx) {
                memset(colData, 0, SKG_MAX_EDGE_PROPERTIES_BYTES);
                bitset.Clear();
//                metrics::GetInstance()->start_time("SubEdgePartition.GetOutEdges.GetEdge",metric_duration_type::MILLISECONDS);
                const PersistentEdge &edge = m_edge_list_f->GetImmutableEdge(idx, edgeBuf);
//                metrics::GetInstance()->stop_time("SubEdgePartition.GetOutEdges.GetEdge");
                if (!edge.deleted()) {  // 忽略被删除的边
                    // get edge data
//                    metrics::GetInstance()->start_time("SubEdgePartition.GetOutEdges.GetEdgeProp",metric_duration_type::MILLISECONDS);
                    s = CollectProperties(edge, idx, req.m_columns, colData, &bitset);
                    if (!s.ok()) { return s; }
//                    metrics::GetInstance()->stop_time("SubEdgePartition.GetOutEdges.GetEdgeProp");
                    // put to result
                    assert(edge.tag == m_attributes.label_tag);
//                    metrics::GetInstance()->start_time("SubEdgePartition.GetOutEdges.ReceiveEdge", metric_duration_type::MILLISECONDS);
                    s = pQueryResult->ReceiveEdge(
                            edge.src, edge.dst,
                            edge.weight, edge.tag,
                            colData, m_attributes.GetColumnsValueByteSize(),
                            bitset
                    );
//                    metrics::GetInstance()->stop_time("SubEdgePartition.GetOutEdges.ReceiveEdge");
                }
            }
        }
//        metrics::GetInstance()->stop_time("SubEdgePartition.GetOutEdges");
        return s;
    }

    Status SubEdgePartition::GetBothEdges(const VertexRequest &req, EdgesQueryResult *result) const {
        Status s;
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};
        char colData[SKG_MAX_EDGE_PROPERTIES_BYTES];
        PropertiesBitset_t bitset;
        {// out-edges
            auto idx_window = m_src_index_f->GetOutIdxRange(req.m_vid);
            if (idx_window.first != INDEX_NOT_EXIST) {  // 索引中找到src范围
                for (idx_t idx = idx_window.first;
                     idx < idx_window.second && idx < m_edge_list_f->num_edges();
                     ++idx) {
                    memset(colData, 0, SKG_MAX_EDGE_PROPERTIES_BYTES);
                    bitset.Clear();
                    const PersistentEdge &edge = m_edge_list_f->GetImmutableEdge(idx, edgeBuf);
                    if (!edge.deleted()) {  // 忽略被删除的边
                        // get edge data
                        s = CollectProperties(edge, idx, req.m_columns, colData, &bitset);
                        if (!s.ok()) { return s; }
                        // put to result
                        assert(edge.tag == m_attributes.label_tag);
                        s = result->ReceiveEdge(
                                edge.src, edge.dst,
                                edge.weight, edge.tag,
                                colData, m_attributes.GetColumnsValueByteSize(),
                                bitset
                        );
                    }
                }
            }
        }
        {// in-edges
            idx_t idx = m_dst_index_f->GetFirstInIndex(req.m_vid);
            while (idx != INDEX_NOT_EXIST) {
                memset(colData, 0, SKG_MAX_EDGE_PROPERTIES_BYTES);
                const PersistentEdge &edge = m_edge_list_f->GetImmutableEdge(idx, edgeBuf);
                if (!edge.deleted()) {  // 忽略被删除的边
                    // get edge data
                    s = CollectProperties(edge, idx, req.m_columns, colData, &bitset);
                    if (!s.ok()) { return s; }
                    // put to result
                    assert(edge.tag == m_attributes.label_tag);
                    s = result->ReceiveEdge(
                            edge.src, edge.dst,
                            edge.weight, edge.tag,
                            colData, m_attributes.GetColumnsValueByteSize(),
                            bitset
                    );
                }
                idx = edge.next();
            }
        }
        return s;
    }

    Status SubEdgePartition::GetInVertices(const VertexRequest &req, VertexQueryResult *result) const {
        idx_t idx = m_dst_index_f->GetFirstInIndex(req.m_vid);
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};
        while (idx != INDEX_NOT_EXIST) {
            const PersistentEdge &edge = m_edge_list_f->GetImmutableEdge(idx, edgeBuf);
            if (!edge.deleted()) {  // 忽略被删除的边
                // put to result
                assert(req.m_vid == edge.dst);
                assert(edge.tag == m_attributes.label_tag);
                // label-tag-of-src, src-vid
                result->Receive(m_attributes.src_tag, edge.src);
            }
            idx = edge.next();
        }
        if (result->IsOverLimit()) {
            return Status::ResultSizeOverLimit(fmt::format("{}", result->m_nlimit));
        } else {
            return Status::OK();
        }
    }

    Status SubEdgePartition::GetOutVertices(const VertexRequest &req, VertexQueryResult *result) const {
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};
        // 索引中找到src范围
        auto idx_window = m_src_index_f->GetOutIdxRange(req.m_vid);
        for (idx_t idx = idx_window.first;
             idx < idx_window.second && idx < m_edge_list_f->num_edges();
             ++idx) {
            const PersistentEdge &edge = m_edge_list_f->GetImmutableEdge(idx, edgeBuf);
            if (!edge.deleted()) {  // 忽略被删除的边
                // put to result
                assert(req.m_vid == edge.src);
                assert(edge.tag == m_attributes.label_tag);
                // label-tag-of-dst, dst-vid
                result->Receive(m_attributes.dst_tag, edge.dst);
            }
        }
        if (result->IsOverLimit()) {
            return Status::ResultSizeOverLimit(fmt::format("{}", result->m_nlimit));
        } else {
            return Status::OK();
        }
    }

    Status SubEdgePartition::GetBothVertices(const VertexRequest &req, VertexQueryResult *result) const {
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};
        {// in-vertices
            idx_t idx = m_dst_index_f->GetFirstInIndex(req.m_vid);
            while (idx != INDEX_NOT_EXIST) {
                const PersistentEdge &edge = m_edge_list_f->GetImmutableEdge(idx, edgeBuf);
                if (!edge.deleted()) {  // 忽略被删除的边
                    // put to result
                    assert(req.m_vid == edge.dst);
                    assert(edge.tag == m_attributes.label_tag);
                    // label-tag-of-src, src-vid
                    result->Receive(m_attributes.src_tag, edge.src);
                }
                idx = edge.next();
            }
        }
        {// out-vertices
            auto idx_window = m_src_index_f->GetOutIdxRange(req.m_vid);
            // 索引中找到src范围
            for (idx_t idx = idx_window.first;
                 idx < idx_window.second && idx < m_edge_list_f->num_edges();
                 ++idx) {
                const PersistentEdge &edge = m_edge_list_f->GetImmutableEdge(idx, edgeBuf);
                if (!edge.deleted()) {  // 忽略被删除的边
                    // put to result
                    assert(req.m_vid == edge.src);
                    assert(edge.tag == m_attributes.label_tag);
                    // label-tag-of-dst, dst-vid
                    result->Receive(m_attributes.dst_tag, edge.dst);
                }
            }
        }
        // 比如有两条边 1->2, 2->1, 查询 2 的 both vertices, vQueryResult 中有两个 1
        if (result->IsOverLimit()) {
            return Status::ResultSizeOverLimit(fmt::format("{}", result->m_nlimit));
        } else {
            return Status::OK();
        }
    }

    Status SubEdgePartition::GetInDegree(const vid_t dst, int *ans) const {
        Status s;
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};
        idx_t idx = m_dst_index_f->GetFirstInIndex(dst);
        while (idx != INDEX_NOT_EXIST) {
            const PersistentEdge &edge = m_edge_list_f->GetImmutableEdge(idx, edgeBuf);
            if (!edge.deleted()) { ++(*ans); }
            idx = edge.next();
        }
        return s;
    }

    Status SubEdgePartition::GetOutDegree(const vid_t src, int *ans) const {
        Status s;
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};
        auto idx_window = m_src_index_f->GetOutIdxRange(src);
        if (idx_window.first != INDEX_NOT_EXIST) {
            for (idx_t idx = idx_window.first; idx < idx_window.second && idx < m_edge_list_f->num_edges(); idx++) {
                const PersistentEdge &edge = m_edge_list_f->GetImmutableEdge(idx, edgeBuf);
                // 忽略被删除的边
                if (!edge.deleted()) { ++(*ans); }
            }
        }
        return s;
    }

    Status SubEdgePartition::AddEdge(const EdgeRequest &request) {
        assert(false);
        return Status::InvalidArgument("Trying to insert edges to partition without memtable.");
    }

    Status SubEdgePartition::DeleteEdge(const EdgeRequest &req) {
        // check label 一致
        assert(req.GetLabel() == m_attributes.GetEdgeLabel());

        Status s;
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};
        // Edge is not exist in MemTable. Trying to delete edge in disk
        auto idx_window = m_src_index_f->GetOutIdxRange(req.m_srcVid);
        if (idx_window.first != INDEX_NOT_EXIST) { // src 索引中无此vid
            for (idx_t idx = idx_window.first;
                 idx < idx_window.second && idx < m_edge_list_f->num_edges();
                 ++idx) {
                PersistentEdge *pEdge = m_edge_list_f->GetMutableEdge(idx, edgeBuf);
                if (pEdge->dst == req.m_dstVid) {
                    assert(pEdge->src == req.m_srcVid); // 由于是从src索引范围中找到的, 正常情况下肯定一致。
                    pEdge->SetDelete();
                    s = m_edge_list_f->Set(idx, pEdge);
                    return s;
                }
            }
        }
        // 找不到相应的边
        return Status::NotExist(fmt::format("edge: {}->{}.", req.m_srcVid, req.m_dstVid));
    }

    Status SubEdgePartition::GetEdgeAttributes(const EdgeRequest &req, EdgesQueryResult *result) {
        // check label 一致
        assert(req.GetLabel() == m_attributes.GetEdgeLabel());

        // check 边是否存在
        const idx_t idx = GetEdgeIdx(req.m_srcVid, req.m_dstVid);
        if (idx == INDEX_NOT_EXIST) {
            return Status::NotExist(fmt::format("edge: {}->{}", req.m_srcVid, req.m_dstVid));
        }
        Status s;
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};
        const PersistentEdge &edge = m_edge_list_f->GetImmutableEdge(idx, edgeBuf);
        // check 边属性列是否存在
        char buff[SKG_MAX_EDGE_PROPERTIES_BYTES] = {'\0'};
        PropertiesBitset_t bitset;
        s = CollectProperties(edge, idx, req.m_columns, buff, &bitset);
        if (!s.ok()) { return s; }
        assert(edge.tag == m_attributes.label_tag);
        s = result->ReceiveEdge(
                edge.src, edge.dst, 
                edge.weight, edge.tag,
                buff, m_attributes.GetColumnsValueByteSize(),
                bitset
        );
        if (!s.ok()) { return s; }
        return s;
    }

    Status SubEdgePartition::SetEdgeAttributes(const EdgeRequest &req) {
        // check label 一致
        assert(req.GetLabel() == m_attributes.GetEdgeLabel());

        // 磁盘数据为空的 shard, 提前退出
        if (this->m_edge_list_f->num_edges() == 0) { return Status::NotExist(fmt::format("edge: {}->{}", req.m_srcVid, req.m_dstVid)); }
        
        metrics::GetInstance()->start_time("SubEdgePartition.SetEdgeAttributes.disk",metric_duration_type::MILLISECONDS);
        // check 边是否存在
        metrics::GetInstance()->start_time("SubEdgePartition.SetEdgeAttributes.disk.idx",metric_duration_type::MILLISECONDS);
        const idx_t idx = GetEdgeIdx(req.m_srcVid, req.m_dstVid);
        metrics::GetInstance()->stop_time("SubEdgePartition.SetEdgeAttributes.disk.idx");
        if (idx == INDEX_NOT_EXIST) {
            metrics::GetInstance()->stop_time("SubEdgePartition.SetEdgeAttributes.disk");
            return Status::NotExist(fmt::format("edge: {}->{}", req.m_srcVid, req.m_dstVid));
        }
        Status s;
        metrics::GetInstance()->start_time("SubEdgePartition.SetEdgeAttributes.disk.prop",metric_duration_type::MILLISECONDS);
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};
        PersistentEdge *edge = m_edge_list_f->GetMutableEdge(idx, edgeBuf);
        assert(edge != nullptr);
        for (size_t i = 0; i < req.m_columns.size(); ++i) {
            if (req.m_columns[i].columnType() == ColumnType::WEIGHT) {
                // 修改边的权重
#ifdef SKG_REQ_VAR_PROP
                edge->weight = req.m_prop.get<EdgeWeight_t>(req.m_columns[i].offset());
#else
                edge->weight = *reinterpret_cast<const EdgeWeight_t *>(req.m_coldata + req.m_columns[i].offset());
#endif
                s = m_edge_list_f->Set(idx, edge);
            } else if (req.m_columns[i].columnType() == ColumnType::TAG) {
                // invalid. 不可修改边的类型
                s = Status::InvalidArgument("Can NOT change edge's label");
                continue;
            } else {
                // TODO 存在某些属性名字符合, 但属性类型不符合?
                IEdgeColumnPartitionPtr col;
                s = GetPropertiesColumnHandler(req.m_columns[i].colname(), &col);
                if (s.IsNotExist()) { continue; }
#ifdef SKG_REQ_VAR_PROP
                Slice bytes_to_put;
                if (req.m_columns[i].columnType() == ColumnType::FIXED_BYTES || req.m_columns[i].columnType() == ColumnType::VARCHAR) {
                    bytes_to_put = req.m_prop.getVar(req.m_columns[i].offset());
                } else {
                    bytes_to_put = req.m_prop.get(
                            req.m_columns[i].offset(),
                            req.m_columns[i].offset() + req.m_columns[i].value_size()
                    );
                }
                s = col->Set(idx, bytes_to_put.data(), bytes_to_put.size());
#else
                s = col->Set(idx, req.m_coldata + req.m_columns[i].offset(), req.m_columns[i].value_size());
#endif
                if (!s.ok()) {
                    // TODO 设置属性值出错
                }
                // 设置属性 bitset
                edge->SetProperty(col->id());
            }
        }
        metrics::GetInstance()->stop_time("SubEdgePartition.SetEdgeAttributes.disk.prop");
        metrics::GetInstance()->stop_time("SubEdgePartition.SetEdgeAttributes.disk");
        return s;
    }

    /*
    Status SubEdgePartition::BulkUpdate(
            const BulkUpdateOptions &bulk_options,
            ReqShardRange &req_shard_range,
            dense_bitset *is_updated) {

        // 磁盘数据为空的 shard, 提前退出
        if (this->m_edge_list_f->num_edges() == 0) { return Status::NotExist(); }

        Status s;
        for (ReqsIter iter = req_shard_range.edges_beg; iter != req_shard_range.edges_end; ++iter) {
            // 过滤 label 不匹配的
            if (this->label() != iter->GetLabel()) { continue; }
            // 过滤 interval 不匹配的
            if (!this->GetInterval().Contain(iter->DstVid())) { continue; }
            s = this->SetEdgeAttributes(*iter);
            if (s.ok()) {
                // 已经找到并更新成功
                is_updated->set_bit(iter - req_shard_range.edges_beg);
            }
        }
        // 通过 bitset 返回不存在的边
        return s;
    }
    */

    Status SubEdgePartition::FlushCache(bool force) {
        // 不是强制 flush, 且没有 flush 的需求, 则不做处理
        if (!force) {
            return Status::OK();
        }
        Status s;

        // unload origin file readers.
        // (topology data, edge-properties data) (mmap的文件修改的部分, 刷入到磁盘)
        if (m_edge_list_f != nullptr) {
            s = m_edge_list_f->Flush();
            if (!s.ok()) { return s; }
        }

        // 边属性的缓存 block 刷到磁盘
        if (BlocksCacheManager2::GetInstance() != nullptr) {
            s = BlocksCacheManager2::GetInstance()->Flush(GetInterval());
            if (!s.ok()) { return s; }
        }

        for (auto &column : m_columns) {
            s = column->Flush();
            if (!s.ok()) { return s; }
        }
        if (!s.ok()) { return s; }

        return s;
    }
//sort the edges in buffered_edges to all edges from disk, including columns
    Status SubEdgePartition::MergeEdgesAndFlush(
            std::vector<MemoryEdge> &&buffered_edges,//&&for moved rvalue
            const interval_t &interval) {//interval is given
        // assume exist edges on disk, read them
//        metrics::GetInstance()->start_time("EdgePartition.MergeEdgesAndFlush.load");
        std::vector<MemoryEdge> mergedEdges = this->LoadAllEdges();
        //SKG_LOG_DEBUG("load edges from disk done. size: {}", mergedEdges.size());
//        metrics::GetInstance()->stop_time("EdgePartition.MergeEdgesAndFlush.load");

        // unload origin file readers
        this->CloseHandlers();

        // merge buffered_edges and edges in disk partition
        // C++11, 使用 `move` 迭代器合并vector, 避免元素复制的开销
        // [Concatenating two std::vectors](https://stackoverflow.com/a/21972296)
//        metrics::GetInstance()->start_time("EdgePartition.MergeEdgesAndFlush.merge");
        mergedEdges.insert(
                mergedEdges.end(),
                std::make_move_iterator(buffered_edges.begin()),
                std::make_move_iterator(buffered_edges.end()));
        buffered_edges.clear(); buffered_edges.shrink_to_fit();  // 释放空间
        //SKG_LOG_DEBUG("merge done. size: {}", mergedEdges.size());
//        metrics::GetInstance()->stop_time("EdgePartition.MergeEdgesAndFlush.merge");

        Status s;
        // 更新 interval 的区间
        m_interval.ExtendTo(interval.second);

//        metrics::GetInstance()->start_time("EdgePartition.MergeEdgesAndFlush.flush");
        s = SubEdgePartitionWriter::FlushEdges(std::move(mergedEdges), m_storage_dir, m_shard_id, m_partition_id, GetInterval(), m_attributes);
        // TODO handle error
        if (!s.ok()) { return s; }
//        metrics::GetInstance()->stop_time("EdgePartition.MergeEdgesAndFlush.flush");

        // reload file readers
        s = OpenHandlers();
        if (!s.ok()) { return s; }
        return s;
    }

    void SubEdgePartition::ReferByOptions(const Options &options) {
        size_t bytes_per_edge = sizeof(PersistentEdge);
        for (const auto &m_column : m_columns) {
            bytes_per_edge += m_column->value_size();
        }
        m_num_max_shard_edges = options.shard_size_mb * 1024 * 1024 / bytes_per_edge;
//            SKG_LOG_INFO("Partition {}, set shard_size_mb={}, bytes_per_edge={}, num_max_edges={}",
//                         GetInterval(), options.shard_size_mb,
//                         bytes_per_edge, m_num_max_shard_edges);
    }

    idx_t SubEdgePartition::GetEdgeIdx(const vid_t src, const vid_t dst) const {
        auto idx_window = m_src_index_f->GetOutIdxRange(src);
        Status s;
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};
        if (idx_window.first != INDEX_NOT_EXIST) {  // 索引中找到src范围
            // TODO 若 idx_window 比较大, 改为二分查找
            for (idx_t idx = idx_window.first;
                 idx < idx_window.second && idx < m_edge_list_f->num_edges();
                 ++idx) {
                const PersistentEdge &edge = m_edge_list_f->GetImmutableEdge(idx, edgeBuf);
                if (!edge.deleted()) {  // 忽略被删除的边
                    if (edge.dst == dst) {
                        return idx;
                    }
                }
            }
        }
        return INDEX_NOT_EXIST;
    }

    Status SubEdgePartition::OpenHandlers() {
        Status s;
        s = m_edge_list_f->Open();
        if (!s.ok()) { return s; }
//            SKG_LOG_DEBUG("Got {} edges for {}", m_edge_list_f->num_edges(), GetInterval());
        s = m_src_index_f->Open();
        if (!s.ok()) { return s; }
        s = m_dst_index_f->Open();
        if (!s.ok()) { return s; }
        for (size_t i = 0; i < m_columns.size(); i++) {
            s = m_columns[i]->Open();
            if (!s.ok()) { return s; }
        }
        return s;
    }

    Status SubEdgePartition::CloseHandlers() {
        Status s;
        m_edge_list_f->Close();
        m_src_index_f->Close();
        m_dst_index_f->Close();
        for (size_t i = 0; i < m_columns.size(); i++) {
            m_columns[i]->Close();
        }
        return Status::OK();
    }

    /**
     * 从磁盘中读取所有的边 (忽略被打上删除标志的边)
     * @return
     */
    std::vector<MemoryEdge> SubEdgePartition::LoadAllEdges() {
        const size_t total_column_bytes = m_attributes.GetColumnsValueByteSize();
        std::vector<MemoryEdge> persistentEdges(m_edge_list_f->num_edges(), total_column_bytes);//vector size is num_edges(). what is total_column_bytes for?
        Bytes colData(total_column_bytes, 0);
        PropertiesBitset_t bitset;
        idx_t actual_size = 0;
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};//where did this init properly?
        for (idx_t i = 0; i < m_edge_list_f->num_edges(); ++i) {
            const PersistentEdge &edge = m_edge_list_f->GetImmutableEdge(i, edgeBuf);//reture record in this->m_mapped_edges, in fact edgeBuf is not used here
            Status s = CollectProperties(edge, i, m_attributes.GetColumns(), reinterpret_cast<char *>(colData.data()), &bitset);//return data by colData.data()
            if (!s.ok()) {
                // TODO FIXME
                SKG_LOG_ERROR("GetEdgeData failed! {}", s.ToString());
                return persistentEdges;
            }
            if (!edge.deleted()) {  // 忽略被删除的边
                // copy 拓扑数据 && 权重 && 类型 && 属性是否有值的 bitset
                persistentEdges[actual_size++].CopyFrom(edge, colData);
            }
        }
        persistentEdges.resize(actual_size);
        return persistentEdges;
    }

    Status SubEdgePartition::CollectProperties(
            const PersistentEdge &edge,//specify the edge in query
            const idx_t idx,//the i-th edge 
            const std::vector<ColumnDescriptor> &queryCols,//from this->m_attributes
            char *buff/*the buffer for output*/, PropertiesBitset_t *bitset) const {
        Status s;
        if (queryCols.empty()) { return s; }
        char *buffPtr = buff;
        if (queryCols.size() == 1 && queryCols[0].colname() == IRequest::QUERY_ALL_COLUMNS[0]) {
            for (size_t i = 0; i < m_columns.size(); ++i) {//for all columns
                const auto &col = m_columns[i];
                if (edge.IsPropertySet(col->id())) {
                    bitset->SetProperty(i);//what does this for
                    s = col->Get(idx, buffPtr);
                } else {
                    // left empty 该属性为 null
                }
                buffPtr += col->value_size();
            }
        } else {
            IEdgeColumnPartitionPtr col;
            for (size_t i = 0; i < queryCols.size(); ++i) {//for columns in query
                const auto &queryCol = queryCols[i];
                s = GetPropertiesColumnHandler(queryCol.colname(), &col);//for col->id()
                if (s.IsNotExist()) { continue; }
                if (edge.IsPropertySet(col->id())) {
                    bitset->SetProperty(i);
                    s = col->Get(idx, buffPtr);
                } else {
                    // left empty 该属性为 null
                }
                buffPtr += col->value_size();
            }
        }
        return Status::OK();
    }

    Status SubEdgePartition::GetPropertiesColumnHandler(const std::string &colname, IEdgeColumnPartitionPtr *ptr) const {
        assert(ptr != nullptr);
        for (auto &column : m_columns) {
            if (column->isColumnGroup()) {
                auto colGroup = std::static_pointer_cast<EdgeColumnGroupMMappedFilePartition>(column);
                for (size_t j = 0; j < colGroup->GetNumCols(); ++j) {
                    if (colname == colGroup->GetCols(j)->name()) {
                        *ptr = colGroup->GetCols(j);
                        return Status::OK();
                    }
                }
            } else {
                if (colname == column->name()) {
                    *ptr = column;
                    return Status::OK();
                }
            }
        }
        return Status::NotExist();
    }

    Status SubEdgePartition::TruncatePartition() {
        Status s;
        s = this->CloseHandlers();
        if (!s.ok()) { return s; }

        s = PathUtils::TruncateFile(m_edge_list_f->filename(), 0);
        if (!s.ok()) {return s;}
        s = PathUtils::TruncateFile(m_src_index_f->filename(), 0);
        if (!s.ok()) {return s;}
        s = PathUtils::TruncateFile(m_dst_index_f->filename(), 0);
        if (!s.ok()) {return s;}

        return this->OpenHandlers();
    }

    Status SubEdgePartition::CreateEdgeAttrCol(ColumnDescriptor descriptor) {
        Status s;
        // 嵌入存储的tag,weight，不需要创建
        if (descriptor.columnType() == ColumnType::TAG || descriptor.columnType() == ColumnType::WEIGHT) {
            return s;
        }
        // TODO check 不存在同名列
        s = m_attributes.AddColumn(descriptor);
        if (!s.ok()) { return s; }

        // 获取插入到配置项中的desc
        const ColumnDescriptor * const d = m_attributes.GetColumn(descriptor, true);
        assert(d != nullptr);

        // 当shard中含有边的时候, 创建对应存储的空间
        std::unique_ptr<IEdgeColumnPartitionWriter> writer;
        s = ColumnDescriptorUtils::CreateWriter(*d, m_edge_list_f->filename(), &writer);
        if (!s.ok()) { return s; }
        s = writer->CreateInitialBlocks(m_edge_list_f->num_edges());
        if (!s.ok()) { return s;}
        // TODO 调整 m_num_max_shard_edges
        IEdgeColumnPartitionPtr fragment;
        s = ColumnDescriptorUtils::CreatePartition(*d,
                GetStorageDir(), shard_id(), id(), GetInterval(), m_attributes.label_tag,
                                                   &fragment);
        if (!s.ok()) { return s; }
        m_columns.emplace_back(std::move(fragment));
        return s;
    }

    Status SubEdgePartition::DeleteEdgeAttrCol(const std::string &columnName) {
        // TODO 删除属性列
        return Status::NotImplement("can NOT delete edge-property col in edge-partition");
    }

    Status SubEdgePartition::ExportData(const std::string &outDir, std::shared_ptr<IDEncoder> encoder) {
        Status s;
        EdgeLabel elabel = this->label();
        const std::string exported_filename = fmt::format(
                "{}/edges/part--{}-{}-{}--{:04d}-{:04d}",
                outDir,
                elabel.src_label, elabel.edge_label, elabel.dst_label,
                m_shard_id, m_partition_id);
        s = Env::Default()->CreateDirIfMissing(PathUtils::get_dirname(exported_filename), true);
        if (!s.ok()) { return s; }
        std::unique_ptr<WritableFile> f;
        EnvOptions options;
        s = Env::Default()->NewWritableFile(exported_filename, &f, options);
        if (!s.ok()) { return s; }

        std::string label, vertex;
        char buff[SKG_MAX_EDGE_PROPERTIES_BYTES] = {'\0'};
        char edgeBuf[sizeof(PersistentEdge)] = {'\0'};
        for (idx_t idx = 0; idx < m_edge_list_f->num_edges(); ++idx) {
            const PersistentEdge &edge = m_edge_list_f->GetImmutableEdge(idx, edgeBuf);
            // edge's vid -> label, vertex
            s = encoder->GetVertexByID(edge.src, &label, &vertex);
            if (!s.ok()) { continue; }
            s = f->Append(vertex);
            if (!s.ok()) { continue; }
            s = encoder->GetVertexByID(edge.dst, &label, &vertex);
            if (!s.ok()) { continue; }
            s = f->Append(fmt::format(",{}", vertex));
            if (!s.ok()) { continue; }

            // check 边属性列是否存在
            char *buffPtr = buff;
            memset(buff, 0, sizeof(buff));
            for (size_t i = 0; i < m_columns.size(); ++i) {
                const auto &col = m_columns[i];
                if (edge.IsPropertySet(col->id())) {
                    s = col->Get(idx, buffPtr);
                } else {
                    // left empty 该属性为 null
                }

                s = f->Append(",");
                // null 值
                if (!edge.IsPropertySet(col->id())) {
                    s = f->Append("\\NULL");
                    continue;
                }
                // 取出值 uhuan: TODO varchar multi-value
                switch (col->columnType()) {
                    case ColumnType::INT32:
			{
                            int32_t val=*reinterpret_cast<int32_t *>(buffPtr);
                            s = f->Append(fmt::format("{}", val));
			}
                        //s = f->Append(fmt::format("{}", *reinterpret_cast<int32_t *>(buffPtr)));
                        break;
                    case ColumnType::INT64:
			{
                            int64_t val=*reinterpret_cast<int64_t *>(buffPtr);
                            s = f->Append(fmt::format("{}", val));
			}
                        //s = f->Append(fmt::format("{}", *reinterpret_cast<int64_t *>(buffPtr)));
                        break;
                    case ColumnType::FLOAT:
			{
                            float val=*reinterpret_cast<float *>(buffPtr);
                            s = f->Append(fmt::format("{}", val));
			}
                        //s = f->Append(fmt::format("{}", *reinterpret_cast<float *>(buffPtr)));
                        break;
                    case ColumnType::DOUBLE:
			{
                            double val=*reinterpret_cast<double *>(buffPtr);
                            s = f->Append(fmt::format("{}", val));
			}
                        //s = f->Append(fmt::format("{}", *reinterpret_cast<double *>(buffPtr)));
                        break;
                    case ColumnType::TIME:
			{
			    //const time_t val= *std::localtime(reinterpret_cast<const time_t*>(buffPtr));
			    const time_t val= *(reinterpret_cast<const time_t*>(buffPtr));
			    std::tm time =*std::localtime(&val); 
			    std::string str = fmt::format("{:%Y-%m-%d %H:%M:%S}",time); 
                            s =  f->Append(str); 
			}
                        //s = f->Append(fmt::format("{:%Y-%m-%d %H:%M:%S}", *std::localtime(reinterpret_cast<const time_t*>(buffPtr))));
                        break;
                    case ColumnType::FIXED_BYTES: 
			{
                            size_t len = std::min(col->value_size(), strlen(buffPtr));
                            s = f->Append(Slice(buffPtr, len)); 
			    break;
                        }
                    case ColumnType::NONE:
                    case ColumnType::TAG:
                    case ColumnType::WEIGHT:
                    case ColumnType::GROUP:
//                            fmt::format("{}:Not impl", static_cast<uint32_t>(metadata->GetColumnType(i)));
                        assert(false);
                        break;
                }

                buffPtr += col->value_size();
            }
            s = f->Append("\n");
        }

        return s;
    }

SubEdgePartition::SubEdgePartition(
        const std::string &prefix, uint32_t shard_id, uint32_t partition_id,
        const interval_t &interval, const MetaAttributes &attributes, const Options &options)
        : m_storage_dir(prefix),
          m_shard_id(shard_id), m_partition_id(partition_id),
          m_attributes(attributes),
          m_edge_list_f(),
          m_src_index_f(),
          m_dst_index_f(),
          m_interval(interval),
          m_options(options),
          m_num_max_shard_edges(1),
          m_columns() {
    if (m_options.use_mmap_read) {
        m_edge_list_f.reset(new EdgeListMmapReader(
                prefix, shard_id, partition_id, interval, attributes.label_tag, m_options));
    } else {
        m_edge_list_f.reset(new EdgeListRawReader(
                prefix, shard_id, partition_id, interval, attributes.label_tag));
    }
    // src-indices
    if (m_options.use_elias_gamma_compress) {
        SKG_LOG_DEBUG("compressing indices of shard:{}-{}-{}", shard_id, partition_id, attributes.label_tag);
	SKG_LOG_ERROR("invalid use_elias_gamma_compress option: `{}'", m_options.use_elias_gamma_compress);
        //m_src_index_f.reset(new IndexEliasGammaReader( FILENAME::sub_partition_src_idx(m_edge_list_f->filename())));
    } else {
        if (m_options.use_mmap_read) {
            m_src_index_f.reset(new IndexMmapReader(
                    FILENAME::sub_partition_src_idx(m_edge_list_f->filename())));
        } else {
            m_src_index_f.reset(new IndexRawReader(
                    FILENAME::sub_partition_src_idx(m_edge_list_f->filename())));
        }
    }
    // dst-indices
    if (m_options.use_mmap_read) {
        m_dst_index_f.reset(new IndexMmapReader(
                FILENAME::sub_partition_dst_idx(m_edge_list_f->filename())));
    } else {
        m_dst_index_f.reset(new IndexRawReader(
                FILENAME::sub_partition_dst_idx(m_edge_list_f->filename())));
    }
}

}
