#include "VecMemTable.h"

namespace skg {

    Status VecMemTable::DeleteVertex(const VertexRequest &request) {
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); /* left empty*/) {
            if (iter->src == request.m_vid || iter->dst == request.m_vid) {
                iter = m_buffered_edges.erase(iter); // 删除后迭代器重置
            } else {
                ++iter;
            }
        }
        return Status::OK();
    }

    Status VecMemTable::GetInEdges(const VertexRequest &request, EdgesQueryResult *pQueryResult) const {
        // get in edges from memory buffer
        Status s;
        char colData[SKG_MAX_EDGE_PROPERTIES_BYTES];
        PropertiesBitset_t bitset;
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); ++iter) {
            if (request.m_vid == iter->dst) {
                memset(colData, 0, SKG_MAX_EDGE_PROPERTIES_BYTES);
                bitset.Clear();
                CollectProperties(*iter, request.GetColumns(), colData, &bitset);
                s = pQueryResult->ReceiveEdge(
                        iter->src, iter->dst,
                        iter->weight, iter->tag,
                        colData, m_attributes.GetColumnsValueByteSize(),
                        bitset
                );
                if (!s.ok()) { return s; }
            }
        }
        return s;
    }

    Status VecMemTable::GetOutEdges(const VertexRequest &request, EdgesQueryResult *pQueryResult) const {
        // get out edges from memory buffer
        Status s;
        char colData[SKG_MAX_EDGE_PROPERTIES_BYTES];
        PropertiesBitset_t bitset;
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); ++iter) {
            if (request.m_vid == iter->src) {
                memset(colData, 0, SKG_MAX_EDGE_PROPERTIES_BYTES);
                bitset.Clear();
                CollectProperties(*iter, request.GetColumns(), colData, &bitset);
                s = pQueryResult->ReceiveEdge(
                        iter->src, iter->dst,
                        iter->weight, iter->tag,
                        colData, m_attributes.GetColumnsValueByteSize(),
                        bitset
                );
                if (!s.ok()) { return s; }
            }
        }
        return s;
    }

    Status VecMemTable::GetBothEdges(const VertexRequest &request, EdgesQueryResult *result) const {
        Status s;
        char colData[SKG_MAX_EDGE_PROPERTIES_BYTES];
        PropertiesBitset_t bitset;
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); ++iter) {
            if (request.m_vid == iter->dst || request.m_vid == iter->src) {
                memset(colData, 0, SKG_MAX_EDGE_PROPERTIES_BYTES);
                bitset.Clear();
                CollectProperties(*iter, request.GetColumns(), colData, &bitset);
                s = result->ReceiveEdge(
                        iter->src, iter->dst,
                        iter->weight, iter->tag,
                        colData, m_attributes.GetColumnsValueByteSize(),
                        bitset
                );
                if (!s.ok()) { return s; }
            }
        }
        return s;
    }

    Status VecMemTable::GetInVertices(const VertexRequest &request, VertexQueryResult *result) const {
        // get in edges from memory buffer
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); ++iter) {
            if (request.m_vid == iter->dst) {
                result->Receive(m_attributes.src_tag, iter->src);
            }
        }
        if (result->IsOverLimit()) {
            return Status::ResultSizeOverLimit(fmt::format("{}", result->m_nlimit));
        } else {
            return Status::OK();
        }
    }

    Status VecMemTable::GetOutVertices(const VertexRequest &request, VertexQueryResult *result) const {
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); ++iter) {
            if (request.m_vid == iter->src) {
                result->Receive(m_attributes.dst_tag, iter->dst);
            }
        }
        if (result->IsOverLimit()) {
            return Status::ResultSizeOverLimit(fmt::format("{}", result->m_nlimit));
        } else {
            return Status::OK();
        }
    }

    Status VecMemTable::GetBothVertices(const VertexRequest &request, VertexQueryResult *result) const {
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); ++iter) {
            if (request.m_vid == iter->src) {
                result->Receive(m_attributes.dst_tag, iter->dst);
            } else if (request.m_vid == iter->dst) {
                result->Receive(m_attributes.src_tag, iter->src);
            }
        }
        if (result->IsOverLimit()) {
            return Status::ResultSizeOverLimit(fmt::format("{}", result->m_nlimit));
        } else {
            return Status::OK();
        }
    }

    Status VecMemTable::GetInDegree(const vid_t &request, int *ans) const {
        for (const auto &edge : m_buffered_edges) {
            if (request == edge.dst) {
                ++(*ans);
            }
        }
        return Status::OK();
    }

    Status VecMemTable::GetOutDegree(const vid_t &request, int * ans) const{
        for (const auto &edge : m_buffered_edges) {
            if (request == edge.src) {
                ++(*ans);
            }
        }
        return Status::OK();
    }

    Status VecMemTable::AddEdge(const EdgeRequest &request) {
        if (request.GetLabel().edge_label != m_attributes.GetEdgeLabel().edge_label) {
            return Status::InvalidArgument(fmt::format(
                    "can NOT insert edge into mem-buff of label:`{}'",
                    m_attributes.label));
        }
        // 插入到buffer的属性, 大小, 顺序 严格按照 attributes 完全一致
        // 若dst超过原来interval范围, 调整interval
        if (!m_interval.Contain(request.m_dstVid)) {
            m_interval.ExtendTo(request.m_dstVid);
        }
        Status s;
        // 边的权重默认为 1, 如果有设置, 再修改. 根据 attribute 中的属性列, 创建属性 buff 长度.
        MemoryEdge edge(request.m_srcVid, request.m_dstVid, 1, m_attributes.label_tag,
                        m_attributes.GetColumnsValueByteSize());
        // 把待插入的属性数据, 组织为与 MemTable 中属性数据顺序一致.
        ReorderAttributesToEdge(request, &edge);
        m_buffered_edges.emplace_back(edge);
        return s;
    }

    Status VecMemTable::DeleteEdge(const EdgeRequest &request) {
        //assert(request.GetLabel() == m_attributes.GetEdgeLabel());
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); /* left empty*/) {
            if (iter->src == request.m_srcVid && iter->dst == request.m_dstVid) {
                iter = m_buffered_edges.erase(iter);
                return Status::OK();
            } else {
                ++iter;
            }
        }
        return Status::NotExist();
    }

    Status VecMemTable::GetEdgeAttributes(const EdgeRequest &request, EdgesQueryResult *result) {
        //assert(request.GetLabel() == m_attributes.GetEdgeLabel());
        Status s;
        char buff[SKG_MAX_EDGE_PROPERTIES_BYTES];
        PropertiesBitset_t bitset;
        for (size_t i = 0; i < m_buffered_edges.size(); ++i) {
            if (m_buffered_edges[i].src == request.m_srcVid && m_buffered_edges[i].dst == request.m_dstVid) {
                memset(buff, 0, SKG_MAX_EDGE_PROPERTIES_BYTES);
                CollectProperties(m_buffered_edges[i], request.GetColumns(), buff, &bitset);
                s = result->ReceiveEdge(
                        m_buffered_edges[i].src,
                        m_buffered_edges[i].dst,
                        m_buffered_edges[i].weight,
                        m_buffered_edges[i].tag,
                        buff, m_attributes.GetColumnsValueByteSize(),
                        bitset
                );
                return s;
            }
        }
        return Status::NotExist();
    }

    Status VecMemTable::SetEdgeAttributes(const EdgeRequest &request) {
        //assert(m_attributes.GetEdgeLabel() == request.GetLabel());
        for (size_t i = 0; i < m_buffered_edges.size(); ++i) {
            MemoryEdge &edge = m_buffered_edges[i];
            // note: src, dst, tag 都相同才看做是同一条边
            if (request.m_srcVid == edge.src && request.m_dstVid == edge.dst) {
                // 把待修改的属性数据, 按照 MemTable 中属性列顺序修改相应的偏移量.
                ReorderAttributesToEdge(request, &edge);
                return Status::OK();
            }
        }
        // 边不存在于 MemTable 中
        return Status::NotExist();
    }

    void VecMemTable::CollectProperties(
            const MemoryEdge &edge,
            const std::vector<ColumnDescriptor> &columns,
            char *buff, PropertiesBitset_t *bitset) const {
        if (columns.size() == 1 && columns[0].colname() == IRequest::QUERY_ALL_COLUMNS[0]) { // 查询所有属性
            // TODO 属性是否为 null
            for (size_t i = 0; i < SKG_MAX_EDGE_PROPERTIES_SIZE; ++i) {
                bitset->SetProperty(edge.IsPropertySet(i));
            }
            edge.GetData(0, m_attributes.GetColumnsValueByteSize(), buff);
        } else {
            size_t offset = 0;
            for (size_t i = 0; i < columns.size(); ++i) {
                const auto &col = columns[i];
                const ColumnDescriptor *const p = m_attributes.GetColumn(col, false);
                if (p == nullptr) { continue; }
                if (p->columnType() == ColumnType::TAG || p->columnType() == ColumnType::WEIGHT) {
                    continue;
                }
                // 获取属性值
                if (edge.IsPropertySet(p->id())) {
                    bitset->SetProperty(i);
                    edge.GetData(p->offset(), p->value_size(), buff + offset);
                } else {
                    // 属性为 null
                }
                offset += p->value_size();
            }
        }
    }

    void VecMemTable::ReorderAttributesToEdge(const EdgeRequest &request, MemoryEdge *edge) const {
        // 把待修改的属性数据, 按照 MemTable 中属性列顺序修改相应的偏移量.
        for (const auto &col : request.GetColumns()) {
            if (col.columnType() == ColumnType::WEIGHT) {
#ifdef SKG_REQ_VAR_PROP
                edge->weight = request.m_prop.get<EdgeWeight_t>(col.offset());
#else
                edge->weight = *reinterpret_cast<const EdgeWeight_t *>(request.m_coldata + col.offset());
#endif
            } else {
                const ColumnDescriptor *const p = m_attributes.GetColumn(col, true);
                if (p == nullptr) {
                    SKG_LOG_TRACE("col:`{}' not exist", col.colname());
                    continue;
                }
                SKG_LOG_TRACE("filling col:`{}' data into offset:{}, {}-bytes", p->colname(), p->offset(),
                              p->value_size());
                size_t bytes_length = std::min(col.value_size(), p->value_size());
                if (bytes_length < p->value_size()) {
                    // fixed-bytes 清空原来的属性值
                    memset(edge->GetColsData().data() + p->offset(),
                           0, p->value_size());
                }
#ifdef SKG_REQ_VAR_PROP
                Slice bytes_to_put;
                if (p->columnType() == ColumnType::FIXED_BYTES || p->columnType() == ColumnType::VARCHAR) {
                    bytes_to_put = request.m_prop.getVar(col.offset());
                    if (col.value_size() > p->value_size()) {// 过长时, 截断存储
                        const size_t storage_len = p->value_size();
                        bytes_to_put.remove_suffix(bytes_to_put.size() - storage_len);
                    }
                } else {
                    bytes_to_put = request.m_prop.get(
                            col.offset(),
                            col.offset() + std::min(col.value_size(), p->value_size())
                    );
                }
                memcpy(
                        edge->GetColsData().data() + p->offset(), // 存放属性值的地址
                        bytes_to_put.data(),
                        bytes_length
                );
#else
                memcpy(
                        edge->GetColsData().data() + p->offset(), // 存放属性值的地址
                        request.m_coldata + col.offset(),
                        bytes_length
                );
#endif
                // 设置属性非 null
                edge->SetProperty(p->id());
            }
        }
    }

Status VecMemTable::CreateEdgeAttrCol(ColumnDescriptor descriptor) {
    if (GetNumEdges() != 0) {
        // TODO 在 MemTable 不为空的情况下创建属性列, 需要给 MemTable 中的所有边属性分配新的空间
        return Status::NotImplement("can NOT create edge property col with memory-buffer");
    }
    Status s;
    s = m_attributes.AddColumn(descriptor);
    return s;
}

Status VecMemTable::ExportData(const std::string &outDir, std::shared_ptr<IDEncoder> encoder,
                               uint32_t shard_id, uint32_t partition_id) const {
    Status s;
    const EdgeLabel lbl = this->GetLabel();
    const std::string exported_filename = fmt::format(
            "{}/edges/part--{}-{}-{}--{:04d}-{:04d}.m{:04d}",
            outDir, lbl.src_label, lbl.edge_label, lbl.dst_label,
            shard_id, partition_id, 0);
    s = Env::Default()->CreateDirIfMissing(PathUtils::get_dirname(exported_filename), true);
    if (!s.ok()) { return s; }
    std::unique_ptr<WritableFile> f;
    EnvOptions options;
    s = Env::Default()->NewWritableFile(exported_filename, &f, options);
    if (!s.ok()) { return s; }

    std::vector<ColumnDescriptor> descriptions;
    descriptions.emplace_back(ColumnDescriptor("*", ColumnType::NONE));

    std::string label, vertex;
    for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); ++iter) {
        // edge's vid -> label, vertex
        s = encoder->GetVertexByID(iter->src, &label, &vertex);
        if (!s.ok()) { continue; }
        s = f->Append(vertex);
        if (!s.ok()) { continue; }
        s = encoder->GetVertexByID(iter->dst, &label, &vertex);
        if (!s.ok()) { continue; }
        s = f->Append(fmt::format(",{}", vertex));
        if (!s.ok()) { continue; }

        // 边属性列
        for (const auto &prop: m_attributes) {
            s = f->Append(",");
            if (!iter->IsPropertySet(prop.id())) {
                // null 值
                s = f->Append("\\NULL");
                continue;
            }
            // TODO 取出属性值
            switch (prop.columnType()) {
                case ColumnType::INT32:
//                    s = f->Append(fmt::format("{}", iter->second.m_properties.get<int32_t>(prop.offset())));
                    break;
                case ColumnType::INT64:
//                    s = f->Append(fmt::format("{}", iter->second.m_properties.get<int64_t>(prop.offset())));
                    break;
                case ColumnType::FLOAT:
//                    s = f->Append(fmt::format("{}", iter->second.m_properties.get<float>(prop.offset())));
                    break;
                case ColumnType::DOUBLE:
//                    s = f->Append(fmt::format("{}", iter->second.m_properties.get<double>(prop.offset())));
                    break;
                case ColumnType::TIME: {
//                    time_t tm = iter->second.m_properties.get<time_t>(prop.offset());
//                    s = f->Append(fmt::format("{:%Y-%m-%d %H:%M:%S}",
//                                              *std::localtime(&tm)));
                    break;
                }
                case ColumnType::FIXED_BYTES: {
//                    s = f->Append(iter->second.m_properties.get(prop.offset(), prop.offset() + prop.value_size(), true));
                    break;
                }
                case ColumnType::VARCHAR:
//                    s = f->Append(iter->second.m_properties.getVar(prop.offset()));
                    break;
                case ColumnType::NONE:
                case ColumnType::TAG:
                case ColumnType::WEIGHT:
                case ColumnType::GROUP:
//                            fmt::format("{}:Not impl", static_cast<uint32_t>(metadata->GetColumnType(i)));
                    assert(false);
                    break;

            }
        }
        s = f->Append("\n");
    }
    return s;
}

}
