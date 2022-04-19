#include "fmt/format.h"
#include "HashMemTable.h"

namespace skg {

    Status HashMemTable::DeleteVertex(const VertexRequest &request) {
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); /* left empty*/) {
            if (iter->first.src == request.m_vid || iter->first.dst == request.m_vid) {
                m_buffered_edges.erase(iter); // 删除后迭代器重置
            } else {
                ++iter;
            }
        }
        return Status::OK();
    }

    Status HashMemTable::GetInEdges(const VertexRequest &request, EdgesQueryResult *pQueryResult) const {
        // get in edges from memory buffer
        Status s;
        char colData[SKG_MAX_EDGE_PROPERTIES_BYTES];
        PropertiesBitset_t bitset;
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); ++iter) {
            if (request.m_vid == iter->first.dst) {
                memset(colData, 0, SKG_MAX_EDGE_PROPERTIES_BYTES);
                bitset.Clear();
                CollectProperties(iter->second, request.GetColumns(), colData, &bitset);
                s = pQueryResult->ReceiveEdge(
                        iter->first.src, iter->first.dst, 
                        iter->second.weight, m_attributes.label_tag,
                        colData, m_attributes.GetColumnsValueByteSize(),
                        bitset
                );
                if (!s.ok()) { return s; }
            }
        }
        return s;
    }

    Status HashMemTable::GetOutEdges(const VertexRequest &request, EdgesQueryResult *pQueryResult) const {
        // get out edges from memory buffer
        Status s;
        char colData[SKG_MAX_EDGE_PROPERTIES_BYTES];
        PropertiesBitset_t bitset;
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); ++iter) {
            if (request.m_vid == iter->first.src) {
                memset(colData, 0, SKG_MAX_EDGE_PROPERTIES_BYTES);
                bitset.Clear();
                CollectProperties(iter->second, request.GetColumns(), colData, &bitset);
                s = pQueryResult->ReceiveEdge(
                        iter->first.src, iter->first.dst, 
                        iter->second.weight, m_attributes.label_tag, 
                        colData, m_attributes.GetColumnsValueByteSize(),
                        bitset
                );
                if (!s.ok()) { return s; }
            }
        }
        return s;
    }

    Status HashMemTable::GetBothEdges(const VertexRequest &request, EdgesQueryResult *result) const {
        Status s;
        char colData[SKG_MAX_EDGE_PROPERTIES_BYTES];
        PropertiesBitset_t bitset;
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); ++iter) {
            if (request.m_vid == iter->first.dst || request.m_vid == iter->first.src) {
                memset(colData, 0, SKG_MAX_EDGE_PROPERTIES_BYTES);
                bitset.Clear();
                CollectProperties(iter->second, request.GetColumns(), colData, &bitset);
                s = result->ReceiveEdge(
                        iter->first.src, iter->first.dst,
                        iter->second.weight, m_attributes.label_tag,
                        colData, m_attributes.GetColumnsValueByteSize(),
                        bitset
                );
                if (!s.ok()) { return s; }
            }
        }
        return s;
    }

    Status HashMemTable::GetInVertices(const VertexRequest &request, VertexQueryResult *result) const {
        // get in edges from memory buffer
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); ++iter) {
            if (request.m_vid == iter->first.dst) {
                result->Receive(m_attributes.src_tag, iter->first.src);
            }
        }
        if (result->IsOverLimit()) {
            return Status::ResultSizeOverLimit(fmt::format("{}", result->m_nlimit));
        } else {
            return Status::OK();
        }
    }

    Status HashMemTable::GetOutVertices(const VertexRequest &request, VertexQueryResult *result) const {
        // get out edges from memory buffer
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); ++iter) {
            if (request.m_vid == iter->first.src) {
                result->Receive(m_attributes.dst_tag, iter->first.dst);
            }
        }
        if (result->IsOverLimit()) {
            return Status::ResultSizeOverLimit(fmt::format("{}", result->m_nlimit));
        } else {
            return Status::OK();
        }
    }

    Status HashMemTable::GetBothVertices(const VertexRequest &request, VertexQueryResult *result) const {
        // get out edges from memory buffer
        for (auto iter = m_buffered_edges.begin(); iter != m_buffered_edges.end(); ++iter) {
            if (request.m_vid == iter->first.src) {
                result->Receive(m_attributes.dst_tag, iter->first.dst);
            } else if (request.m_vid == iter->first.dst) {
                result->Receive(m_attributes.src_tag, iter->first.src);
            }
        }
        if (result->IsOverLimit()) {
            return Status::ResultSizeOverLimit(fmt::format("{}", result->m_nlimit));
        } else {
            return Status::OK();
        }
    }

    Status HashMemTable::GetInDegree(const vid_t &request, int * ans) const {
        for (auto itr = m_buffered_edges.begin(); itr != m_buffered_edges.end(); itr++) {
            const auto &key = itr->first;
            if (request == key.dst) {
                *ans = *ans + 1;
            }
        }
        return Status::OK();
    }

    Status HashMemTable::GetOutDegree(const vid_t &request, int *ans) const {
        for (auto itr = m_buffered_edges.begin(); itr != m_buffered_edges.end(); itr++) {
            const auto &key = itr->first;
            if (request == key.src) {
                *ans = *ans + 1;
            }
        }
        return Status::OK();
    }

    Status HashMemTable::AddEdge(const EdgeRequest &request) {
        if (request.GetLabel().edge_label != m_attributes.GetEdgeLabel().edge_label) {
            return Status::InvalidArgument(fmt::format(
                    "can NOT insert edge into mem-buff of label:`{}'",
                    m_attributes.label));
        }
        if (!m_interval.Contain(request.m_dstVid)) {
            m_interval.ExtendTo(request.m_dstVid);
        }
        Status s;
        // 边的权重默认为 1, 如果有设置, 再修改. 根据 attribute 中的属性列, 创建属性 buff 长度.
        HashKey key(request.m_srcVid, request.m_dstVid);
        HashEdgeData edgeData(1.0f, m_attributes.GetColumnsValueByteSize());
        // 把带插入的属性数据, 组织为与 MemTable 中属性列顺序一致.
        ReorderAttributesToEdge(request, &edgeData);
        m_buffered_edges.insert(std::make_pair(key, edgeData));
        return Status::OK();
    }

    Status HashMemTable::DeleteEdge(const EdgeRequest &request) {
        //assert(request.GetLabel() == m_attributes.GetEdgeLabel());
        HashKey key(request.m_srcVid, request.m_dstVid);
        const auto iter = m_buffered_edges.find(key);
        if (iter == m_buffered_edges.end()) {
            return Status::NotExist();
        } else {
            m_buffered_edges.erase(iter);
            return Status::OK();
        }
    }

    Status HashMemTable::GetEdgeAttributes(const EdgeRequest &request, EdgesQueryResult *result) {
        //assert(request.GetLabel() == m_attributes.GetEdgeLabel());
        Status s;
        char buff[SKG_MAX_EDGE_PROPERTIES_BYTES];
        PropertiesBitset_t bitset;
        HashKey key(request.m_srcVid, request.m_dstVid);
        const auto iter = m_buffered_edges.find(key);
        if (iter == m_buffered_edges.end()) {
            // 边不存在于 MemTable 中
            return Status::NotExist();
        }
        memset(buff, 0, SKG_MAX_EDGE_PROPERTIES_BYTES);
        bitset.Clear();
        CollectProperties(iter->second, request.GetColumns(), buff, &bitset);
        s = result->ReceiveEdge(
                iter->first.src,
                iter->first.dst,
                iter->second.weight,
                m_attributes.label_tag,
                buff, m_attributes.GetColumnsValueByteSize(),
                bitset);
        return s;
    }

    Status HashMemTable::SetEdgeAttributes(const EdgeRequest &request) {
        //assert(m_attributes.GetEdgeLabel() == request.GetLabel());
        HashKey key(request.m_srcVid, request.m_dstVid);
        auto iter = m_buffered_edges.find(key);
        if (iter == m_buffered_edges.end()) {
            // 边不存在于 MemTable 中
            return Status::NotExist();
        }
        // 把待修改的属性数据, 按照 MemTable 中属性列顺序修改相应的偏移量.
        ReorderAttributesToEdge(request, &iter->second);
        return Status::OK();
    }

    void HashMemTable::CollectProperties(
            const HashEdgeData &edge,
            const std::vector<ColumnDescriptor> &columns,
            char *buff, PropertiesBitset_t *bitset) const {
        if (columns.size() == 1 && columns[0].colname() == IRequest::QUERY_ALL_COLUMNS[0]) {
            // 属性是否为 null
            *bitset = edge.m_properties.bitset();
            Slice fix_bytes = edge.m_properties.fixed_bytes();
            memcpy(buff, fix_bytes.data(), fix_bytes.size());
        } else {
            size_t offset = 0;
            for (const auto &col: columns) {
                const ColumnDescriptor *const p = m_attributes.GetColumn(col, false);
                if (p == nullptr) { continue; }
                if (p->columnType() == ColumnType::TAG || p->columnType() == ColumnType::WEIGHT) {
                    continue;
                }
                // 获取属性值
                if (!edge.m_properties.is_null(p->id())) {
                    bitset->SetProperty(p->id());
                    Slice bytes = edge.m_properties.get(p->offset(), p->offset() + p->value_size());
                    memcpy(buff + offset, bytes.data(), bytes.size());
                } else {
                    // 属性值为 null
                }
                offset += p->value_size();
            }
        }
    }

    void HashMemTable::ReorderAttributesToEdge(const EdgeRequest &request, HashEdgeData *edge) const {
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
#else
                Slice bytes_to_put(request.m_coldata + col.offset(),
                                   std::min(col.value_size(), p->value_size())
                );
#endif
                if (bytes_to_put.size() < p->value_size()) {
                    // fixed-bytes 清空原来的属性
                    std::string clear_bytes(p->value_size(), '\0');
                    edge->m_properties.putBytes(clear_bytes, p->offset(), p->id());
                }
                edge->m_properties.putBytes(bytes_to_put, p->offset(), p->id());
            }
        }
    }

Status HashMemTable::CreateEdgeAttrCol(ColumnDescriptor descriptor) {
    if (GetNumEdges() != 0) {
        // TODO 在 MemTable 不为空的情况下创建属性列, 需要给 MemTable 中的所有边属性分配新的空间
        return Status::NotImplement("can NOT create edge property col with memory-buffer");
    }
    Status s;
    s = m_attributes.AddColumn(descriptor);
    return s;
}

Status HashMemTable::ExportData(const std::string &outDir,
                                std::shared_ptr<IDEncoder> encoder,
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
        s = encoder->GetVertexByID(iter->first.src, &label, &vertex);
        if (!s.ok()) { continue; }
        s = f->Append(vertex);
        if (!s.ok()) { continue; }
        s = encoder->GetVertexByID(iter->first.dst, &label, &vertex);
        if (!s.ok()) { continue; }
        s = f->Append(fmt::format(",{}", vertex));
        if (!s.ok()) { continue; }

        // 边属性列
        for (const auto &prop: m_attributes) {
            s = f->Append(",");
            if (iter->second.m_properties.is_null(prop.id())) {
                // null 值
                s = f->Append("\\NULL");
                continue;
            }
            // 取出值
            switch (prop.columnType()) {
                case ColumnType::INT32:
                    //s = f->Append(fmt::format("{}", iter->second.m_properties.get<int32_t>(prop.offset())));
                    break;
                case ColumnType::INT64:
                    //s = f->Append(fmt::format("{}", iter->second.m_properties.get<int64_t>(prop.offset())));
                    break;
                case ColumnType::FLOAT:
                    //s = f->Append(fmt::format("{}", iter->second.m_properties.get<float>(prop.offset())));
                    break;
                case ColumnType::DOUBLE:
                    //s = f->Append(fmt::format("{}", iter->second.m_properties.get<double>(prop.offset())));
                    break;
                case ColumnType::TIME: {
                    time_t tm = iter->second.m_properties.get<time_t>(prop.offset());
                    //s = f->Append(fmt::format("{:%Y-%m-%d %H:%M:%S}",
                     //                         *std::localtime(&tm)));
                    break;
                }
                case ColumnType::FIXED_BYTES: {
                    //s = f->Append(iter->second.m_properties.get(prop.offset(), prop.offset() + prop.value_size(), true));
                    break;
                }
                case ColumnType::VARCHAR:
                    //s = f->Append(iter->second.m_properties.getVar(prop.offset()));
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
