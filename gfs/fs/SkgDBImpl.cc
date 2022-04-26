#include "SkgDBImpl.h"
#include <set>
#include <string>
#include <env/env.h>

//#include "Temporal.h"
#include "TraverseAction.h"
#include "PathAction.h"
//#include "TimePathAction.h"
#include "util/ThreadPool.h"
//#include "hetnet_action.h"
//#include "RequestUtilities.h"
//#include "file_reader_writer.h"
#include "util/pathutils.h"
#include "StringToLongIdEncoder.h"

namespace skg {

Status SkgDBImpl::RecoverHandlers(const MetaShardInfo &meta_shard_info) {
    Status s;
    const std::string basedir = this->GetStorageDirname();

    // 节点 string -> int 转换
    if (!PathUtils::FileExists(DIRNAME::id_mapping(basedir))) {
        this->m_options.id_type = Options::VertexIdType::LONG;
    }

    switch (this->m_options.id_type) {
        case Options::VertexIdType::STRING:{
            SKG_LOG_ERROR("id-encoder: {} not available!", "RocksDBIdEncoder");
	    //Jiefeng 20220112
            //this->m_id_encoder = std::make_shared<RocksDBIdEncoder>( m_options.id_convert_cache_mb, m_options.id_convert_num_hot_key);
            break;
        }
        case Options::VertexIdType::LONG: {
            SKG_LOG_DEBUG("id-encoder: {}", "StringToLongIdEncoder");
            this->m_id_encoder = std::make_shared<StringToLongIdEncoder>();
            break;
        }
    }
    s = this->m_id_encoder->Open(basedir);
    if (!s.ok()) { return s; }

    // 节点相关的操作句柄
    s = VertexColumnList::Open(basedir, &this->m_vertex_columns);
    if (!s.ok()) { return s; }

    bool isMigrated = false; // 对旧的生成的边属性文件兼容处理, 调整边属性的src-tag,dst-tag
    for (auto &prop: this->m_edge_attr) {
        if (prop.src_tag == 0 && prop.dst_tag == 0) {
            isMigrated = true;
            SKG_LOG_INFO("migrating edge:{} vtag", prop.label);
            s = this->m_vertex_columns->GetLabelTag(prop.src_label, &prop.src_tag);
            if (!s.ok()) { break; }
            s = this->m_vertex_columns->GetLabelTag(prop.dst_label, &prop.dst_tag);
            if (!s.ok()) { break; }
            SKG_LOG_INFO("migrated edge: {} vtag -> [{}:{}], [{}:{}]",
                         prop.label,
                         prop.src_label, prop.src_tag,
                         prop.dst_label, prop.dst_tag);
        }
    }
    if (isMigrated) {
        // 更新旧的边属性文件
        SKG_LOG_INFO("updating edge-het-properties", "");
        MetadataFileHandler::WriteEdgeAttrConf(basedir, this->m_edge_attr);
    }

    // Shard-Tree 的操作句柄
    {// 多线程打开 ShardTree
        // ShardTree <= 4 个时, tree options, mmap 加上预读
        Options tree_options = m_options;
        if (meta_shard_info.roots.size() <= 4) { tree_options.use_mmap_populate = true; }
        // 先插入指针，然后利用线程池多线程并发打开 ShardTree
        for (size_t i = 0; i < meta_shard_info.roots.size(); ++i) {
            ShardTreePtr tree;
            this->m_trees.emplace_back(tree);
        }
        const uint32_t open_threads = get_option_uint("open_threads", 8);
        ThreadPool pool(open_threads);
        std::vector<std::future<Status>> open_status;
        for (size_t i = 0; i < meta_shard_info.roots.size(); ++i) {
            open_status.emplace_back(
                    pool.enqueue(ShardTree::Open,
                                 basedir,
                                 meta_shard_info.roots[i].id,
                                 meta_shard_info.roots[i].interval,
                                 tree_options, &this->m_trees[i])
            );
        }
        for (auto &&status : open_status) {
            s = status.get();
            if (!s.ok()) { break; }
        }
    }
    return s;
}

    Status SkgDBImpl::GetVertexAttr(/* const */VertexRequest &req, VertexQueryResult *pQueryResult) const {
        assert(pQueryResult != nullptr);
        pQueryResult->Clear(); // 清空结果集, 防止结果集中存着之前的数据

        Status s;
        s = PrepareRequest(&req, m_vertex_columns, GetIDEncoder()); // 请求包中的 string-id 转换为 long-id
        if (!s.ok()) { return s; }

        s = m_vertex_columns->GetVertexAttr(req, pQueryResult);
        if (!s.ok()) { return s; }
        return s;
    }

    Status SkgDBImpl::DeleteVertex(VertexRequest &req) {
        Status s;
        s = PrepareRequest(&req, m_vertex_columns, GetIDEncoder()); // 请求包中的 string-id 转换为 long-id
        if (!s.ok()) { return s; }
#if 0
        if (req.IsWALEnabled()) { // 记录 REDO 日志
            uint64_t last_sequence = m_version.LastSequence();
            s = m_log_writer->AddRecord(RequestUtilities::SerializeDeleteVertexRecord(req, last_sequence, 0, -1));
            if (s.ok() && req.IsSyncEnabled()) {
                s = m_log_writer->file()->Sync(true);
                if (!s.ok()) {
                    SKG_LOG_ERROR("wal log sync error: {}", s.ToString());
                }
            }
            last_sequence += 1;
            m_version.SetLastSequence(last_sequence);
        }
#endif

        return RedoDeleteVertex(req);
    }

    Status SkgDBImpl::RedoDeleteVertex(VertexRequest &req) {
        Status s;
        // 到所有shard中删除节点关联的边
        for (size_t i = 0; i < m_trees.size(); ++i) {
            s = m_trees[i]->DeleteVertex(req);
            if (!s.ok()) { return s; }
        }

        // 删除节点的属性
        s = m_vertex_columns->DeleteVertex(req);
        if (!s.ok()) { return s; }

        // 从 id-mapping 中删除节点的记录
        s = m_id_encoder->DeleteVertex(req.m_label, req.m_vertex, req.m_vid);
        if (!s.ok()) { return s; }

        return s;
    }

    Status SkgDBImpl::SetVertexAttr(/* const */VertexRequest &req) {
        Status s;
        s = PrepareRequest(&req, m_vertex_columns, GetIDEncoder()); // 请求包中的 string-id 转换为 long-id
        if (!s.ok()) { return s; }
#if 0
        if (req.IsWALEnabled()) {
            uint64_t last_sequence = m_version.LastSequence();
            s = m_log_writer->AddRecord(RequestUtilities::SerializeSetVertexPropRecord(req, last_sequence, 0, -1));
            if (s.ok() && req.IsSyncEnabled()) {
                s = m_log_writer->file()->Sync(true);
                if (!s.ok()) {
                    SKG_LOG_ERROR("wal log sync error: {}", s.ToString());
                }
            }
            last_sequence += 1;
            m_version.SetLastSequence(last_sequence);
        }
#endif

        return RedoSetVertexAttr(req);
    }

    Status SkgDBImpl::RedoSetVertexAttr(VertexRequest &req) {
        return m_vertex_columns->SetVertexAttr(req);
    }

#ifndef SKG_SRC_SPLIT_SHARD
    Status SkgDBImpl::GetInEdges(/* const */VertexRequest &req, EdgesQueryResult *pQueryResult) const {
#else
    Status SkgDBImpl::GetOutEdges(/* const */VertexRequest &req, EdgesQueryResult *pQueryResult) const {
#endif
        assert(pQueryResult != nullptr);
        pQueryResult->Clear(); // 清空结果集, 防止结果集中存着之前的数据

//        metrics::GetInstance()->start_time("SkgDBImpl.GetInEdges", metric_duration_type::MILLISECONDS);
        Status s;
        s = PrepareRequest(&req, m_vertex_columns, GetIDEncoder()); // 请求包中的 string-id 转换为 long-id
        if (!s.ok()) { return s; }

        // 结果集大小限制
        pQueryResult->m_nlimit = req.GetLimit();

//        metrics::GetInstance()->start_time("SkgDBImpl.GetInEdges.shards", metric_duration_type::MILLISECONDS);
        // in-edges, 仅存在于一个 ShardTree 中
        s = Status::NotExist(fmt::format("[{}:{}({})] not exist in shard tree", req.GetLabel(), req.GetVertex(), req.GetVid()));
        for (size_t i = 0; i < m_trees.size(); ++i) {
            if (m_trees[i]->GetInterval().Contain(req.m_vid)) { // 找到该区间的 ShardTree
                s = m_trees[i]->GetInEdges(req, pQueryResult);
                break;
            }
        }
        if (!s.ok()) {
            if (s.IsNotExist()) {// 不存在该节点的 in-edges
                return Status::OK();
            }
            return s;
        }
//        metrics::GetInstance()->stop_time("SkgDBImpl.GetInEdges.shards");

        // 组织回包数据. long-id 转换为 string-id
        s = pQueryResult->TranslateEdgeVertex(GetIDEncoder());
        if (!s.ok()) { return s; }
        // 结果集的 metadata
        MetaHeterogeneousAttributes hAttributes;
        s = m_edge_attr.MatchQueryMetadata(req.m_columns, &hAttributes);
        if (!s.ok()) { return s; }
        pQueryResult->SetResultMetadata(hAttributes);
//        metrics::GetInstance()->stop_time("SkgDBImpl.GetInEdges");
        return s;
    }

#ifndef SKG_SRC_SPLIT_SHARD
    Status SkgDBImpl::GetOutEdges(/* const */VertexRequest &req, EdgesQueryResult *pQueryResult) const {
#else
    Status SkgDBImpl::GetInEdges(/* const */VertexRequest &req, EdgesQueryResult *pQueryResult) const {
#endif
        assert(pQueryResult != nullptr);
        pQueryResult->Clear(); // 清空结果集, 防止结果集中存着之前的数据

//        metrics::GetInstance()->start_time("SkgDBImpl.GetOutEdges", metric_duration_type::MILLISECONDS);
        Status s;
        s = PrepareRequest(&req, m_vertex_columns, GetIDEncoder()); // 请求包中的 string-id 转换为 long-id
        if (!s.ok()) { return s; }

        // 结果集大小限制
        pQueryResult->m_nlimit = req.GetLimit();

//        metrics::GetInstance()->start_time("SkgDBImpl.GetOutEdges.shards", metric_duration_type::MILLISECONDS);
#ifndef SKG_QUERY_USE_MT
        // out-edges, 可能存在于所有 ShardTree 中
        for (size_t i = 0; i < m_trees.size(); ++i) {
            s = m_trees[i]->GetOutEdges(req, pQueryResult);
            if (!s.ok()) { return s; }
            if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 ShardTree 中获取数据
        }
        if (!s.ok()) { return s; }
#else
        std::vector<std::future<Status>> thread_status;
        for (size_t i = 0; i < m_trees.size(); ++i) {
            thread_status.emplace_back(
                    m_query_pool.enqueue(ShardTree::MtiGetOutE, m_trees[i], &req, pQueryResult)
            );
        }
        for (auto &&thread_statu : thread_status) {
            s = thread_statu.get();
            if (!s.ok()) { return s; }
        }
#endif
//        metrics::GetInstance()->stop_time("SkgDBImpl.GetOutEdges.shards");

        // 组织回包数据. long-id 转换为 string-id
        s = pQueryResult->TranslateEdgeVertex(GetIDEncoder());
        if (!s.ok()) { return s; }
        // 结果集的 metadata
        MetaHeterogeneousAttributes hAttributes;
        s = m_edge_attr.MatchQueryMetadata(req.m_columns, &hAttributes);
        if (!s.ok()) { return s; }
        pQueryResult->SetResultMetadata(hAttributes);
//        metrics::GetInstance()->stop_time("SkgDBImpl.GetOutEdges");
        return s;
    }

    Status SkgDBImpl::GetBothEdges(VertexRequest &req, EdgesQueryResult *pQueryResult) const {
        assert(pQueryResult != nullptr);
        pQueryResult->Clear(); // 清空结果集, 防止结果集中存着之前的数据

//        metrics::GetInstance()->start_time("SkgDBImpl.GetOutEdges", metric_duration_type::MILLISECONDS);
        Status s;
        s = PrepareRequest(&req, m_vertex_columns, GetIDEncoder()); // 请求包中的 string-id 转换为 long-id
        if (!s.ok()) { return s; }

        // 结果集大小限制
        pQueryResult->m_nlimit = req.GetLimit();

//        metrics::GetInstance()->start_time("SkgDBImpl.GetOutEdges.shards", metric_duration_type::MILLISECONDS);
        // both-edges, 可能存在于所有 ShardTree 中
        for (size_t i = 0; i < m_trees.size(); ++i) {
            s = m_trees[i]->GetBothEdges(req, pQueryResult);
            if (!s.ok()) { return s; }
            if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 ShardTree 中获取数据
        }
        if (!s.ok()) { return s; }
//        metrics::GetInstance()->stop_time("SkgDBImpl.GetOutEdges.shards");

        // 组织回包数据. long-id 转换为 string-id
        s = pQueryResult->TranslateEdgeVertex(GetIDEncoder());
        if (!s.ok()) { return s; }
        // 结果集的 metadata
        MetaHeterogeneousAttributes hAttributes;
        s = m_edge_attr.MatchQueryMetadata(req.m_columns, &hAttributes);
        if (!s.ok()) { return s; }
        pQueryResult->SetResultMetadata(hAttributes);
//        metrics::GetInstance()->stop_time("SkgDBImpl.GetOutEdges");
        return s;
    }

#ifndef SKG_SRC_SPLIT_SHARD
    Status SkgDBImpl::GetInVertices(VertexRequest &req, VertexQueryResult *pQueryResult) const {
#else
    Status SkgDBImpl::GetOutVertices(VertexRequest &req, VertexQueryResult *pQueryResult) const {
#endif
        assert(pQueryResult != nullptr);
        Status s;
        s = PrepareRequest(&req, m_vertex_columns, GetIDEncoder());
        if (!s.ok()) { return s; }

        VertexQueryResult inVertices;
        // 结果集大小限制
        inVertices.m_nlimit = req.GetLimit();
        s = Status::NotExist(fmt::format("[{}:{}({})] not exist in shard tree",
                req.GetLabel(), req.GetVertex(), req.GetVid()));
        // in-vertices, 仅存在于一个 ShardTree 中
        for (size_t i = 0; i < m_trees.size(); ++i) {
            if (m_trees[i]->GetInterval().Contain(req.GetVid())) {
                s = m_trees[i]->GetInVertices(req, &inVertices);
                break;
            }
        }
        if (!s.ok()) {
            if (s.IsNotExist()) {// 不存在该节点的 in-vertices
                return Status::OK();
            }
            return s;
        }

        // in-vertices 为空集, 不需要再查询 result 的 metadata
        if (inVertices.Size() == 0) {
            pQueryResult->Clear();
            return s;
        }

        // long-id 转换为 string-id
        s = inVertices.TranslateVertex(GetIDEncoder());
        if (!s.ok()) { return s; }

        VertexRequest inVreq;
        inVreq.m_columns = req.m_columns;
        bool isFirst = true;
        while (inVertices.MoveNext()) {
            if (isFirst) {
                inVreq.m_vertex = inVertices.m_vertices[inVertices.m_row_index - 1].m_s_vertex;
                inVreq.m_vid = inVertices.m_vertices[inVertices.m_row_index - 1].m_vertex;
                inVreq.m_labelTag = inVertices.m_vertices[inVertices.m_row_index - 1].tag;
                isFirst = false;
            } else {
                VertexRequest::TmpQueryV tmp;
                tmp.vid = inVertices.m_vertices[inVertices.m_row_index - 1].m_vertex;;
                tmp.vertex = inVertices.m_vertices[inVertices.m_row_index - 1].m_s_vertex;
                tmp.tag = inVertices.m_vertices[inVertices.m_row_index - 1].tag;
                inVreq.m_more.push_back(tmp);
            }
        }
        pQueryResult->Clear(); // 清空结果集, 防止结果集中存着之前的数据
        // 结果集大小限制
        pQueryResult->m_nlimit = req.GetLimit();
        // 获取节点属性, 节点 tag -> label 的映射
        s = m_vertex_columns->GetVertexAttr(inVreq, pQueryResult);
        if (!s.ok()) { return s; }
        return s;
    }

#ifndef SKG_SRC_SPLIT_SHARD
    Status SkgDBImpl::GetOutVertices(VertexRequest &req, VertexQueryResult *pQueryResult) const {
#else
    Status SkgDBImpl::GetInVertices(VertexRequest &req, VertexQueryResult *pQueryResult) const {
#endif
        assert(pQueryResult != nullptr);
        Status s;
        s = PrepareRequest(&req, m_vertex_columns, GetIDEncoder());
        if (!s.ok()) { return s; }

#ifndef SKG_QUERY_USE_MT
        VertexQueryResult outVertices;
        outVertices.m_nlimit = req.GetLimit();
        // out-vertices, 可能存在于所有 ShardTree 中
        for (size_t i = 0; i < m_trees.size(); ++i) {
            s = m_trees[i]->GetOutVertices(req, &outVertices);
            if (!s.ok()) { return s; }
            if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 ShardTree 中>获取数据
        }
#else
#if 0
        VertexQueryResult outVertices;
        outVertices.m_nlimit = req.GetLimit();
        {
            std::vector<VertexQueryResult> results(m_trees.size());
            // out-vertices, 可能存在于所有 ShardTree 中
            static ::ThreadPool pool(get_option_uint("query_threads", 8));
            std::vector<std::future<Status>> thread_status;
            for (size_t i = 0; i < m_trees.size(); ++i) {
                results[i].m_nlimit = req.GetLimit();
                thread_status.emplace_back(
                        pool.enqueue(ShardTree::GetOutV, m_trees[i], &req, &results[i])
                );
            }
            for (size_t i = 0; i < thread_status.size(); ++i) {
                s = thread_status[i].get();
                if (!s.ok()) { return s; }
                SKG_LOG_DEBUG("merging: {}",results[i].m_vertices.size());
                outVertices.m_vertices.insert(
                        outVertices.m_vertices.end(),
                        results[i].m_vertices.begin(),
                        results[i].m_vertices.end()
                );
            }
        }
#else
        VertexQueryResult outVertices;
        outVertices.m_nlimit = req.GetLimit();
        // out-vertices, 可能存在于所有 ShardTree 中
        std::vector<std::future<Status>> thread_status;
        for (const auto &tree : m_trees) {
            thread_status.emplace_back(
                    m_query_pool.enqueue(ShardTree::MtiGetOutV, tree, &req, &outVertices)
            );
        }
        for (auto &&thread_statu : thread_status) {
            s = thread_statu.get();
            if (!s.ok()) { return s; }
        }
#endif
#endif

        // out-vertices 为空集, 不需要再收集 result 的 metadata
        if (outVertices.Size() == 0) {
            pQueryResult->Clear();
            return s;
        }

        // long-id 转换为 string-id
        s = outVertices.TranslateVertex(GetIDEncoder());
        if (!s.ok()) { return s; }

        VertexRequest outVreq;
        outVreq.m_columns = req.m_columns;
        bool isFirst = true;
        while (outVertices.MoveNext()) {
            if (isFirst) {
                outVreq.m_vertex = outVertices.m_vertices[outVertices.m_row_index - 1].m_s_vertex;
                outVreq.m_vid = outVertices.m_vertices[outVertices.m_row_index - 1].m_vertex;
                outVreq.m_labelTag = outVertices.m_vertices[outVertices.m_row_index - 1].tag;
                isFirst = false;
            } else {
                VertexRequest::TmpQueryV tmp;
                tmp.vid = outVertices.m_vertices[outVertices.m_row_index - 1].m_vertex;;
                tmp.vertex = outVertices.m_vertices[outVertices.m_row_index - 1].m_s_vertex;
                tmp.tag = outVertices.m_vertices[outVertices.m_row_index - 1].tag;
                outVreq.m_more.push_back(tmp);
            }
        }
        pQueryResult->Clear(); // 清空结果集, 防止结果集中存着之前的数据
        // 结果集大小限制
        pQueryResult->m_nlimit = req.GetLimit();
        // 获取节点属性, 节点 tag -> label 的映射
        s = m_vertex_columns->GetVertexAttr(outVreq, pQueryResult);
        if (!s.ok()) { return s; }
        return s;
    }

    Status SkgDBImpl::GetBothVertices(VertexRequest &req, VertexQueryResult *pQueryResult) const {
        assert(pQueryResult != nullptr);
        Status s;
        s = PrepareRequest(&req, m_vertex_columns, GetIDEncoder());
        if (!s.ok()) { return s; }

        VertexQueryResult bothVertices;
        bothVertices.m_nlimit = req.GetLimit();
        // both-vertices, 可能存在于所有 ShardTree 中
        for (size_t i = 0; i < m_trees.size(); ++i) {
            s = m_trees[i]->GetBothVertices(req, &bothVertices);
            if (!s.ok()) { return s; }
            if (s.IsOverLimit()) { break; } // 如果结果集超过大小了, 不再到其他 ShardTree 中获取数据
        }

        // both-vertices 为空集, 不需要再收集 result 的 metadata
        if (bothVertices.Size() == 0) {
            pQueryResult->Clear();
            return s;
        }

        // long-id 转换为 string-id
        s = bothVertices.TranslateVertex(GetIDEncoder());
        if (!s.ok()) { return s; }

        VertexRequest bothVreq;
        bothVreq.m_columns = req.m_columns;
        bool isFirst = true;
        while (bothVertices.MoveNext()) {
            if (isFirst) {
                bothVreq.m_vertex = bothVertices.m_vertices[bothVertices.m_row_index - 1].m_s_vertex;
                bothVreq.m_vid = bothVertices.m_vertices[bothVertices.m_row_index - 1].m_vertex;
                bothVreq.m_labelTag = bothVertices.m_vertices[bothVertices.m_row_index - 1].tag;
                isFirst = false;
            } else {
                VertexRequest::TmpQueryV tmp;
                tmp.vid = bothVertices.m_vertices[bothVertices.m_row_index - 1].m_vertex;;
                tmp.vertex = bothVertices.m_vertices[bothVertices.m_row_index - 1].m_s_vertex;
                tmp.tag = bothVertices.m_vertices[bothVertices.m_row_index - 1].tag;
                bothVreq.m_more.push_back(tmp);
            }
        }
        pQueryResult->Clear(); // 清空结果集, 防止结果集中存着之前的数据
        // 结果集大小限制
        pQueryResult->m_nlimit = req.GetLimit();
        // 获取节点属性, 节点 tag -> label 的映射
        s = m_vertex_columns->GetVertexAttr(bothVreq, pQueryResult);
        if (!s.ok()) { return s; }
        return s;
    }

    Status SkgDBImpl::DeleteEdge(/* const */EdgeRequest &req) {
        Status s;
        s = PrepareRequest(&req, m_vertex_columns, GetIDEncoder()); // 请求包中的 string-id 转换为 long-id
        if (!s.ok()) { return s; }

#if 0
        if (req.IsWALEnabled()) { // 记录 REDO 日志
            uint64_t last_sequence = m_version.LastSequence();
            s = m_log_writer->AddRecord(RequestUtilities::SerializeDeleteEdgeRecord(req, last_sequence, 0, -1));
            if (s.ok() && req.IsSyncEnabled()) {
                s = m_log_writer->file()->Sync(true);
                if (!s.ok()) {
                    SKG_LOG_ERROR("wal log sync error: {}", s.ToString());
                }
            }
            last_sequence += 1;
            m_version.SetLastSequence(last_sequence);
        }
#endif

        return RedoDeleteEdge(req);
    }

    Status SkgDBImpl::RedoDeleteEdge(EdgeRequest &req) {
        for (size_t i = 0; i < m_trees.size(); ++i) {
            if (m_trees[i]->GetInterval().Contain(req.m_dstVid)) {
                return m_trees[i]->DeleteEdge(req);
            }
        }
        return Status::NotExist();
    }

    Status SkgDBImpl::GetEdgeAttr(/* const */EdgeRequest &req, EdgesQueryResult *pQueryResult) {
        assert(pQueryResult != nullptr);
        pQueryResult->Clear(); // 清空结果集, 防止结果集中存着之前的数据

        Status s;
        s = PrepareRequest(&req, m_vertex_columns, GetIDEncoder()); // 请求包中的 string-id 转换为 long-id
        if (!s.ok()) { return s; }

        for (size_t i = 0; i < m_trees.size(); ++i) {
            if (m_trees[i]->GetInterval().Contain(req.m_dstVid)) {
                s = m_trees[i]->GetEdgeAttributes(req, pQueryResult);
                if (!s.ok()) { return s; }

                // 组织回包数据. 结果集中, long-id 转换为 string-id
                s = pQueryResult->TranslateEdgeVertex(GetIDEncoder());
                if (!s.ok()) { return s; }
                // 结果集的 metadata
                MetaHeterogeneousAttributes hAttributes;
                s = m_edge_attr.MatchQueryMetadata(req.m_columns, &hAttributes);
                if (!s.ok()) { return s; }
                pQueryResult->SetResultMetadata(hAttributes);
                return s;
            }
        }
        // 所有 shard 中都找不到查询的边
        return Status::NotExist();
    }

    Status SkgDBImpl::AddEdge(/* const */EdgeRequest &req) {
        // 加锁,  禁止其它写操作. TODO 在其他修改操作的地方尝试获取锁 TODO: rethink 加锁延后？
        // 使用 std::lock_guard 获取锁, 在析构时自动释放锁. http://zh.cppreference.com/w/cpp/thread/lock_guard
        std::lock_guard<std::mutex> lock(m_write_lock);

        Status s;
        s = PrepareRequest(&req, m_vertex_columns, GetIDEncoder()); // 请求包中的 string-id 转换为 long-id
        if (!s.ok()) { return s; }

        // 暂时先不支持 self-loop
        if (req.m_srcVid == req.m_dstVid) {
            return Status::UnSupportSelfLoop("self-loop not support");
        }
        // TODO 插入无向边 -> 转化为插入两条边

#if 0
        if (req.IsWALEnabled()) { // 记录 REDO 日志
            uint64_t last_sequence = m_version.LastSequence();
            s = m_log_writer->AddRecord(RequestUtilities::SerializeAddEdgeRecord(req, last_sequence, 0, -1));
            if (s.ok() && req.IsSyncEnabled()) {
                s = m_log_writer->file()->Sync(true);
                if (!s.ok()) {
                    SKG_LOG_ERROR("wal log sync error: {}", s.ToString());
                }
            }
            last_sequence += 1;
            m_version.SetLastSequence(last_sequence);
        }
#endif

        return RedoAddEdge(req);
    }

    Status SkgDBImpl::RedoAddEdge(/* const */ EdgeRequest &req) {
        Status s;
        // 写操作, 需要保证写入的节点id有足够的存储空间
        s = m_vertex_columns->UpdateMaxVertexID(std::max(req.m_srcVid, req.m_dstVid));
        if (!s.ok()) { return s; }

        //LogShardInfos();
        for (size_t i = 0; i < m_trees.size(); ++i) {
            if (m_trees[i]->GetInterval().Contain(req.m_dstVid) || i == m_trees.size() - 1) {
                return m_trees[i]->AddEdge(req);  // TODO 添加边后, ShardTree产生分裂. 需要更新数据
            }
        }
        //LogShardInfos();
        assert(false);
        return Status();
    }

    Status SkgDBImpl::SetEdgeAttr(/* const */EdgeRequest &req) {
        // 加锁,  禁止其它写操作. TODO 在其他修改操作的地方尝试获取锁 TODO: rethink 加锁延后？
        // 使用 std::lock_guard 获取锁, 在析构时自动释放锁. http://zh.cppreference.com/w/cpp/thread/lock_guard
        std::lock_guard<std::mutex> lock(m_write_lock);

        // TODO 和 AddEdge 进行整合?
        Status s;
        s = PrepareRequest(&req, m_vertex_columns, GetIDEncoder()); // 请求包中的 string-id 转换为 long-id
        if (!s.ok()) { return s; }
        // 暂时先不支持 self-loop
        if (req.m_srcVid == req.m_dstVid) {
            return Status::UnSupportSelfLoop("self-loop not support");
        }
        // TODO 插入无向边 -> 转化为插入两条边
#if 0
        if (req.IsWALEnabled()) { // 记录 REDO 日志
            uint64_t last_sequence = m_version.LastSequence();
            s = m_log_writer->AddRecord(RequestUtilities::SerializeSetEdgePropRecord(req, last_sequence, 0, -1));
            if (s.ok() && req.IsSyncEnabled()) {
                s = m_log_writer->file()->Sync(true);
                if (!s.ok()) {
                    SKG_LOG_ERROR("wal log sync error: {}", s.ToString());
                }
            }
            last_sequence += 1;
            m_version.SetLastSequence(last_sequence);
        }
#endif

        return RedoSetEdgeAttr(req);
    }

    Status SkgDBImpl::RedoSetEdgeAttr(EdgeRequest &req) {
        Status s;
        // 写操作, 需要保证写入的节点id有足够的存储空间
        s = m_vertex_columns->UpdateMaxVertexID(std::max(req.m_srcVid, req.m_dstVid));
        if (!s.ok()) { return s; }

        //LogShardInfos();
        for (size_t i = 0; i < m_trees.size(); ++i) {
            if (m_trees[i]->GetInterval().Contain(req.m_dstVid) || i == m_trees.size() - 1) {
                return m_trees[i]->SetEdgeAttributes(req);
            }
        }
        //LogShardInfos();
        // 所有 shard 中都找不到更新的边
        return Status::NotExist();
    }

    std::string SkgDBImpl::GetName() const {
        return m_name;
    }

    std::string SkgDBImpl::GetStorageDirname() const {
        assert(!m_name.empty());
        return m_options.GetDBDir(m_name);
    }

    vid_t SkgDBImpl::GetNumVertices() const {
        return m_vertex_columns->GetNumVertices();
    }

    size_t SkgDBImpl::GetNumEdges() const {
        size_t num_edges = 0;
        for (const auto &tree : m_trees) {
            num_edges += tree->GetNumEdges();
        }
        return num_edges;
    }

    std::shared_ptr<IDEncoder> SkgDBImpl::GetIDEncoder() const 
    {
        return m_id_encoder;
    }

    Status SkgDBImpl::Flush() {
        // Flush 过程加锁, 禁止其他写操作
        // 使用 std::lock_guard 获取锁, 在析构时自动释放锁. http://zh.cppreference.com/w/cpp/thread/lock_guard
        std::lock_guard<std::mutex> lock(m_write_lock);
        return this->FlushUnlocked();
    }

    Status SkgDBImpl::FlushUnlocked() {
        Status s;
        LogShardInfos();
        // 边的数据
        metrics::GetInstance()->start_time("SkgDBImpl.FlushTree", metric_duration_type::MILLISECONDS);
        for (auto &tree : m_trees) {
            s = tree->Flush();
            if (!s.ok()) { return s; }
        }
        metrics::GetInstance()->stop_time("SkgDBImpl.FlushTree");

        // 节点个数, 节点属性
        if (m_vertex_columns != nullptr) {
            s = m_vertex_columns->Flush();
            if (!s.ok()) { return s; }
        }
        // shard tree 划分信息
        MetaShardInfo meta_shard_info;
        for (auto &tree : m_trees) {
            meta_shard_info.roots.emplace_back(tree->id(), tree->GetInterval());
        }
        if (!s.ok()) { return s; }
        s = MetadataFileHandler::WriteLSMIntervals(GetStorageDirname(), meta_shard_info);
        if (!s.ok()) { return s; }
        // 边属性列信息
        s = MetadataFileHandler::WriteEdgeAttrConf(GetStorageDirname(), m_edge_attr);
        if (!s.ok()) { return s; }
        // encoder
        if (m_id_encoder != nullptr) {
            s = m_id_encoder->Flush();
            if (!s.ok()) { return s; }
        }

        // 生成新的WAL日志
#if 0
        //uint64_t last_sequence = m_version.LastSequence();
        s = m_log_writer->AddRecord(RequestUtilities::SerializeCheckpoint(last_sequence));
        if (s.ok()) {
            s = m_log_writer->file()->Sync(true);
            if (!s.ok()) {
                SKG_LOG_ERROR("wal log sync error: {}", s.ToString());
            }
        }
        last_sequence += 1;
        m_version.SetLastSequence(last_sequence);

        MetaJournal meta_journal;
        //meta_journal.m_log_number = m_log_writer->get_log_number();
        //meta_journal.m_last_sequence = last_sequence;
        meta_journal.m_log_number = 1;
        meta_journal.m_last_sequence = 0;
        s = MetadataFileHandler::WriteMetaJournal(GetStorageDirname(), meta_journal);
        if (!s.ok()) { return s; }
#endif
        return s;
    }

    std::vector<EdgeLabel> SkgDBImpl::GetEdgeLabels() const {
        return m_edge_attr.GetEdgeLabels();
    }

    std::vector<std::string> SkgDBImpl::GetVertexLabels() const {
        return m_vertex_columns->GetVertexLabels();
    }

    Status SkgDBImpl::CreateNewVertexLabel(const std::string &label) {
        if (label.empty()) {
            return Status::InvalidArgument("vertex label can NOT be empty!");
        }
#if 0
        if (req.IsWALEnabled()) {
            uint64_t last_sequence = LastSequence();
            s = m_log_writer->AddRecord(RequestUtilities::SerializeSetVertexPropRecord(req, last_sequence, 0, -1));
            if (s.ok() && req.IsSyncEnabled()) {
                s = m_log_writer->file()->Sync(true);
                if (!s.ok()) {
                    SKG_LOG_ERROR("wal log sync error: {}", s.ToString());
                }
            }
            last_sequence += 1;
            SetLastSequence(last_sequence);
        }
#endif
        return m_vertex_columns->CreateNewVertexLabel(label);
    }

    Status SkgDBImpl::CreateVertexAttrCol(const std::string &label, ColumnDescriptor config) {
#if 0
        if (req.IsWALEnabled()) {
            uint64_t last_sequence = LastSequence();
            s = m_log_writer->AddRecord(RequestUtilities::SerializeSetVertexPropRecord(req, last_sequence, 0, -1));
            if (s.ok() && req.IsSyncEnabled()) {
                s = m_log_writer->file()->Sync(true);
                if (!s.ok()) {
                    SKG_LOG_ERROR("wal log sync error: {}", s.ToString());
                }
            }
            last_sequence += 1;
            SetLastSequence(last_sequence);
        }
#endif
        return m_vertex_columns->CreateVertexAttrCol(label, config);
    }

    Status SkgDBImpl::DeleteVertexAttrCol(const std::string &label, const std::string &columnName) {
#if 0
        if (req.IsWALEnabled()) {
            uint64_t last_sequence = LastSequence();
            s = m_log_writer->AddRecord(RequestUtilities::SerializeSetVertexPropRecord(req, last_sequence, 0, -1));
            if (s.ok() && req.IsSyncEnabled()) {
                s = m_log_writer->file()->Sync(true);
                if (!s.ok()) {
                    SKG_LOG_ERROR("wal log sync error: {}", s.ToString());
                }
            }
            last_sequence += 1;
            SetLastSequence(last_sequence);
        }
#endif
        return m_vertex_columns->DeleteVertexAttrCol(label, columnName);
    }

    Status SkgDBImpl::GetVertexAttrs(const std::string &label, std::vector<ColumnDescriptor> *configs) const {
        assert(configs != nullptr);
        return m_vertex_columns->GetVertexAttrs(label, configs);
    }

    Status SkgDBImpl::GetVertexAttrNames(const std::string &label, std::vector<std::string> *names) const {
        assert(names != nullptr);
        return m_vertex_columns->GetVertexAttrNames(label, names);
    }

    Status SkgDBImpl::CreateNewEdgeLabel(
            const EdgeLabel &label) {
        if (label.edge_label.empty()) {
            return Status::InvalidArgument("edge label can NOT be empty!");
        }
        if (label.src_label.empty() || label.dst_label.empty()) {
            return Status::InvalidArgument(fmt::format("src/dst label can NOT be empty! src:`{}', dst:`{}'", label.src_label, label.dst_label));
        }
#if 0
        if (req.IsWALEnabled()) {
            uint64_t last_sequence = LastSequence();
            s = m_log_writer->AddRecord(RequestUtilities::SerializeSetVertexPropRecord(req, last_sequence, 0, -1));
            if (s.ok() && req.IsSyncEnabled()) {
                s = m_log_writer->file()->Sync(true);
                if (!s.ok()) {
                    SKG_LOG_ERROR("wal log sync error: {}", s.ToString());
                }
            }
            last_sequence += 1;
            SetLastSequence(last_sequence);
        }
#endif
        Status s;
        {// 插入到配置中
            MetaAttributes prop(label);
            {// check 节点label已经定义, 解析出 src-tag/dst-tag
                s = m_vertex_columns->GetLabelTag(label.src_label, &prop.src_tag);
                if (s.IsNotExist()) {
                    return Status::InvalidArgument(fmt::format("edge's src label is NOT exist."));
                }
                s = m_vertex_columns->GetLabelTag(label.dst_label, &prop.dst_tag);
                if (s.IsNotExist()) {
                    return Status::InvalidArgument(fmt::format("edge's dst label is NOT exist."));
                }
            }
            s = m_edge_attr.AddAttributes(prop);
        }

        if (s.ok()) {
            // 更新各个 ShardTree 的边属性配置
            auto properties = m_edge_attr.GetAttributesByEdgeLabel(label);
            for (const auto &tree: m_trees) {
                s = tree->CreateNewEdgeLabel(
                        label,
                        properties->label_tag, properties->src_tag, properties->dst_tag);
                if (!s.ok()) { return s; }
            }
        }
        return s;
    }

    Status SkgDBImpl::CreateEdgeAttrCol(const EdgeLabel &label, ColumnDescriptor config) {
        Status s;
#if 0
        if (req.IsWALEnabled()) {
            uint64_t last_sequence = LastSequence();
            s = m_log_writer->AddRecord(RequestUtilities::SerializeSetVertexPropRecord(req, last_sequence, 0, -1));
            if (s.ok() && req.IsSyncEnabled()) {
                s = m_log_writer->file()->Sync(true);
                if (!s.ok()) {
                    SKG_LOG_ERROR("wal log sync error: {}", s.ToString());
                }
            }
            last_sequence += 1;
            SetLastSequence(last_sequence);
        }
#endif
        // 更新边属性配置
        auto attributes = m_edge_attr.GetAttributesByEdgeLabel(label);
        if (attributes == m_edge_attr.end()) {
            // 不存在类型为 `label` 的边类型. 创建该类型的边, 并插入新的一列属性列
            MetaAttributes newAttributes(label);
            s = newAttributes.AddColumn(config);
            if (!s.ok()) { return s; }
            s = m_edge_attr.AddAttributes(newAttributes);
            if (!s.ok()) { return s; }
        } else {
            // 类型为 `label` 的边类型已存在, 插入新的一列属性列
            s = attributes->AddColumn(config);
            if (!s.ok()) { return s; }
        }

        if (s.ok()) {
            // 到每个 ShardTree 中创建新的属性列
            for (size_t i = 0; i < m_trees.size(); ++i) {
                s = m_trees[i]->CreateEdgeAttrCol(label, config);
                if (!s.ok()) { return s; }
            }
        }

        s = MetadataFileHandler::WriteEdgeAttrConf(GetStorageDirname(), m_edge_attr);
        if (!s.ok()) { return s; }
        return s;
    }

    Status SkgDBImpl::DeleteEdgeAttrCol(const EdgeLabel &label, const std::string &columnName) {
        Status s;
#if 0
        if (req.IsWALEnabled()) {
            uint64_t last_sequence = LastSequence();
            s = m_log_writer->AddRecord(RequestUtilities::SerializeSetVertexPropRecord(req, last_sequence, 0, -1));
            if (s.ok() && req.IsSyncEnabled()) {
                s = m_log_writer->file()->Sync(true);
                if (!s.ok()) {
                    SKG_LOG_ERROR("wal log sync error: {}", s.ToString());
                }
            }
            last_sequence += 1;
            SetLastSequence(last_sequence);
        }
#endif
        // 到每个 ShardTree 中删除属性列
        for (size_t i = 0; i < m_trees.size(); ++i) {
            s = m_trees[i]->DeleteEdgeAttrCol(label, columnName);
            if (!s.ok()) { return s; }
        }

        // 更新边属性配置
        auto attributes = m_edge_attr.GetAttributesByEdgeLabel(label);
        if (attributes == m_edge_attr.end()) {
            // 不存在类型为 `label` 的边类型
            return Status::NotExist(fmt::format("edge type:{} not exist", label.ToString()));
        } else {
            s = attributes->DeleteColumn(columnName);
            if (!s.ok()) { return s; }
        }
        s = MetadataFileHandler::WriteEdgeAttrConf(GetStorageDirname(), m_edge_attr);
        if (!s.ok()) { return s; }

        return Status::NotImplement("can NOT delete edge-property in SkgDBImpl");
    }

    Status SkgDBImpl::GetEdgeAttrs(const EdgeLabel &label, std::vector<ColumnDescriptor> *configs) const {
        return m_edge_attr.GetAttributesDescriptors(label, configs);
    }

    Status SkgDBImpl::GetEdgeAttrNames(const EdgeLabel &label, std::vector<std::string> *names) const {
        return m_edge_attr.GetAttributesNames(label, names);
    }

    Status SkgDBImpl::GenDegreeFile() {
        return Status::NotImplement();
    }

    Status SkgDBImpl::PrepareRequest(VertexRequest *req, const std::shared_ptr<VertexColumnList> &lst, std::shared_ptr<IDEncoder> encoder) {
        assert(lst != nullptr);
        assert(encoder != nullptr);
        Status s;
        if (!req->m_vertex.empty()) {
            s = lst->GetLabelTag(req->m_label, &req->m_labelTag);
            if (!s.ok()) { return s; }
            s = encoder->GetIDByVertex(req->m_label, req->m_vertex, &req->m_vid);
            if (!s.ok()) {
                if (req->IsCreateIfNotExist() && s.IsNotExist()) {
                    // 给新插入的节点, 创建新id
                    req->m_vid = lst->AllocateNewVid();
                    req->SetInitLabel(); // mark as need to update vertex's tag
                    s = encoder->Put(req->m_label, req->GetVertex(), req->m_vid);
                    if (!s.ok()) { return s; }
                } else {
                    // 其他错误
                    return s;
                }
            }
        }
        return s;
    }

    Status SkgDBImpl::PrepareRequest(EdgeRequest *req, const std::shared_ptr<VertexColumnList> &lst, std::shared_ptr<IDEncoder> encoder) {
        assert(lst != nullptr);
        assert(encoder != nullptr);
        Status s;
        if (!req->m_srcVertex.empty()) {
            s = encoder->GetIDByVertex(req->SrcLabel(), req->SrcVertex(), &req->m_srcVid);
            if (!s.ok()) {
                if (req->IsCreateIfNotExist() && s.IsNotExist()) {
                    // 给新插入的节点, 创建新id
                    req->m_srcVid = lst->AllocateNewVid();
                    s = encoder->Put(req->SrcLabel(), req->SrcVertex(), req->m_srcVid);
                    if (!s.ok()) { return s; }
                } else {
                    // 其他错误
                }
            }
            s = encoder->GetIDByVertex(req->DstLabel(), req->DstVertex(), &req->m_dstVid);
            if (!s.ok()) {
                if (req->IsCreateIfNotExist() && s.IsNotExist()) {
                    // 给新插入的节点, 创建新id
                    req->m_dstVid = lst->AllocateNewVid();
                    s = encoder->Put(req->DstLabel(), req->DstVertex(), req->m_dstVid);
                    if (!s.ok()) { return s; }
                } else {
                    // 其他错误
                }
            }
        }
        return s;
    }

    Status SkgDBImpl::Drop(const std::set<std::string> &ignore) {
        Status s;
        if (m_closed) { return Status::InvalidArgument("db is already closed"); }
        s = this->Close();
        if (!s.ok()) { return s; }
        const std::string dir = GetStorageDirname();
        std::vector<std::string> subDirs;
        s = Env::Default()->GetChildren(dir, &subDirs);
        if (!s.ok()) { return s; }
        for (const auto &subDir: subDirs) {
            if (Slice(subDir).starts_with("shard")) {
                const std::string tmp = fmt::format("{}/{}", dir, subDir);
                s = Env::Default()->DeleteDir(tmp, true, true);
            } else {
                const std::string tmp = fmt::format("{}/{}", dir, subDir);
                if (subDir == "id_mapping" || subDir == "meta" || subDir == "log" || subDir == "vdata") {
                    const std::string tmp = fmt::format("{}/{}", dir, subDir);
                    s = Env::Default()->DeleteDir(tmp, true, true);
                }
            }
        }
        // 如果文件夹为空, 删除文件夹
        Env::Default()->DeleteDir(dir);
        return s;
    }

    Status SkgDBImpl::Close() {
        // 加锁,  禁止其它写操作. TODO 在其他修改操作的地方尝试获取锁 TODO: rethink 加锁延后？
        // 使用 std::lock_guard 获取锁, 在析构时自动释放锁. http://zh.cppreference.com/w/cpp/thread/lock_guard
        std::lock_guard<std::mutex> lock(m_write_lock);

        Status s;
        if (m_closed) { return s; }
        s = this->FlushUnlocked();
        if (!s.ok()) { return s; }
        SKG_LOG_INFO("Flush done. resetting handlers", "");
        m_trees.clear();
        m_vertex_columns.reset();
        s = m_id_encoder->Close();
        if (!s.ok()) { return s; }
        m_id_encoder.reset();
        m_closed = true;
        SKG_LOG_INFO("Closed", "");
        return s;
    }

    Status SkgDBImpl::ExportData(const std::string &out_dir) {
        Status s;
        s = Env::Default()->CreateDirIfMissing(out_dir);
        if (!s.ok()) { return s; }

        // 导出 Schema
        const std::string schema_file = fmt::format("{}/schema.json", out_dir);
        rapidjson::StringBuffer sb;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
        writer.StartObject();
        writer.Key("vertices");
        s = m_vertex_columns->GetVerticesProperties().SerializeToExportStr(writer);
        if (!s.ok()) { return s; }
        writer.Key("edges");
        s = m_edge_attr.SerializeToExportStr(writer);
        if (!s.ok()) { return s; }
        writer.EndObject();
        std::string json_schema = sb.GetString();
        s = WriteStringToFile(Env::Default(), json_schema, schema_file);
        if (!s.ok()) { return s; }

        // 导出节点信息
        s = m_vertex_columns->ExportData(out_dir, m_id_encoder);
        if (!s.ok()) { return s; }
        // 导出边信息
        for (size_t i = 0; i < m_trees.size(); ++i) {
            s = m_trees[i]->ExportData(out_dir, m_id_encoder);
            if (!s.ok()) { return s; }
        }
        return s;
    }

    std::string SkgDBImpl::ShortestPath(const PathRequest& path_req) const {
        PathAction pa(this);
        return pa.shortest_path(path_req);
    }

    std::string SkgDBImpl::AllPath(const PathRequest& path_req) const {
        PathAction pa(this);
        return pa.all_path(path_req);
    }
    
    /*
    std::string SkgDBImpl::TimeShortestPath(const PathRequest& path_req) const {
        TimePathAction tpa(this);
        return tpa.shortest_path(path_req);
    }*/

    /*
    std::string SkgDBImpl::TimeAllPath(const PathRequest& path_req) const {
        TimePathAction tpa(this);
        return tpa.all_path(path_req);
    }
    */

    Status SkgDBImpl::Kout(const TraverseRequest& traverse_req,
            std::vector<PVpair> *e_visited) const {
        TraverseAction ta(this);
        return ta.k_out(traverse_req, e_visited); 
    }

    Status SkgDBImpl::KoutSize(const TraverseRequest& traverse_req,
            size_t *v_size, size_t *e_size) const {
        TraverseAction ta(this);
        return ta.k_out_size(traverse_req, v_size, e_size); 
    }

    Status SkgDBImpl::Kneighbor(const TraverseRequest& traverse_req,
            std::vector<PathVertex> *visited) const {
        TraverseAction ta(this);
        std::set<PathVertex> pv_set;
        Status s = ta.k_neighbor(traverse_req, &pv_set);
        if (!s.ok()) 
            return s;
        visited->assign(pv_set.begin(), pv_set.end());
        return s;
    }

    /*
     * engine
     */
    /*Status SkgDBImpl::PageRank(const HetnetRequest& hn_req) const {
        HetnetAction ha;
        Status s;
        std::string dbpath = GetStorageDirname();
        std::string db_dir = PathUtils::os_path_dirname(dbpath);
        HetnetRequest hr;
        hr.set(hn_req);
        hr.db_dir = db_dir;
        //hr.db_dir = "/data/skg_db/db";
        hr.db_name = GetName();
	//fix this
        //s = ha.async_pagerank(hr);
        return s;
    }*/
        
    /*Status SkgDBImpl::LPA(const HetnetRequest& hn_req) const {
        HetnetAction ha;
        Status s;
        std::string dbpath = GetStorageDirname();
        std::string db_dir = PathUtils::os_path_dirname(dbpath);
        HetnetRequest hr;
        hr.set(hn_req);
        hr.db_dir = db_dir;
        hr.db_name = GetName();
	//fix this
        //s = ha.async_LPA(hr);
        return s;
    }
    */
        
    /*Status SkgDBImpl::FastUnfolding(const HetnetRequest& hn_req) const {
        HetnetAction ha;
        Status s;
        std::string dbpath = GetStorageDirname();
        std::string db_dir = PathUtils::os_path_dirname(dbpath);
        HetnetRequest hr;
        hr.set(hn_req);
        hr.db_dir = db_dir;
        hr.db_name = GetName();
	//fix this
        //s = ha.async_fastunfolding(hr);
        return s;
    }
    */


    void SkgDBImpl::LogShardInfos() const {
        for (auto &tree : m_trees) {
            const ShardTree::NumEdgesDetail detail = tree->GetNumEdgesDetail();
            SKG_LOG_INFO("shard-id:{}, interval:{}, #edges:{}, #edges-in-mem:{}, #edges-in-disk:{}",
                         tree->id(),
                         tree->GetInterval(),
                         detail.num_edges(),
                         detail.memory_num_edges, detail.disk_num_edges
            );
        }
    }

}
