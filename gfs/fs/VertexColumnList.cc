#include "env/env.h"
#include "VertexColumnList.h"

#include "util/internal_types.h"
#include <util/types.h>
#include <sstream>
#include "fs/VecMemTable.h"
#include "fs/HashMemTable.h"
#include "fs/SubEdgePartition.h"
#include "fs/SubEdgePartitionWriter.h"
#include "fs/SubEdgePartitionWithMemTable.h"
namespace skg {

    Status VertexColumnList::Create(const std::string &dir, const MetaHeterogeneousAttributes &hetAttributes, vid_t max_vertex_id) {
        Status s;
        s = PathUtils::CreateDirIfMissing(dir);
        if (!s.ok()) { return s; }
        s = PathUtils::CreateDirIfMissing(DIRNAME::meta(dir));
        if (!s.ok()) { return s; }
        s = MetadataFileHandler::WriteVertexAttrConf(dir, hetAttributes);
        if (!s.ok()) { return s; }
        MetaNumVertices vertices_info;
        if (max_vertex_id == 0) {
            vertices_info.max_allocated_vid = 0;
            vertices_info.num_vertices = 0;
            vertices_info.storage_capacity_vid = GetNextStorageCapacity(0);
        } else {
            vertices_info.max_allocated_vid = max_vertex_id;
            vertices_info.num_vertices = max_vertex_id + 1;
            vertices_info.storage_capacity_vid = GetNextStorageCapacity(vertices_info.max_allocated_vid);
        }
        SKG_LOG_TRACE("Creating vertex columns of storage: {}", vertices_info.storage_capacity_vid);
        s = MetadataFileHandler::WriteNumVertices(dir, vertices_info);
        if (!s.ok()) { return s; }
        // ==== 创建节点属性存储文件 ==== //
        // 文件夹
        s = PathUtils::CreateDirIfMissing(DIRNAME::vertex_attr(dir));
        if (!s.ok()) { return s; }
        {// 节点的 label-tag 列
            ColumnDescriptor descriptor(SKG_VERTEX_COLUMN_NAME_TAG, ColumnType::TAG);
            descriptor.SetColumnID(ColumnDescriptor::ID_VERTICES_TAG);
            s = IVertexColumn::CreateWithCapacity(dir, SKG_GLOBAL_LABEL, descriptor, vertices_info.storage_capacity_vid);
            if (!s.ok()) { return s; }
        }
        {// 节点的属性 bitset 列
            ColumnDescriptor descriptor(SKG_VERTEX_COLUMN_NAME_BITSET, ColumnType::FIXED_BYTES);
            descriptor.SetColumnID(ColumnDescriptor::ID_VERTICES_BITSET)
                    .SetFixedLength(SKG_MAX_EDGE_PROPERTIES_SIZE / 8);
            s = IVertexColumn::CreateWithCapacity(dir, SKG_GLOBAL_LABEL, descriptor, vertices_info.storage_capacity_vid);
            if (!s.ok()) { return s; }
        }
        // 属性列文件
        for (const auto &attributes: hetAttributes) {
            for (const auto &col: attributes) {
                s = IVertexColumn::CreateWithCapacity(
                        dir, attributes.label, col, vertices_info.storage_capacity_vid);
                if (!s.ok()) { return s; }
            }
        }
        return s;
    }

    Status VertexColumnList::Open(const std::string &dir, std::shared_ptr<VertexColumnList> *pLst) {
        assert(pLst != nullptr);
        assert(*pLst == nullptr);
        Status s;
        VertexColumnList *lst = new VertexColumnList();//
        do {
            lst->m_storage_dir = dir;
            // 节点数量
            MetaNumVertices vertices_info;
            s = MetadataFileHandler::ReadNumVertices(
                    lst->GetStorageDir(),
                    &vertices_info
            );
            // 已经分配的最大节点 id
            lst->m_max_vertices_id = vertices_info.max_allocated_vid;
            // 节点存储 capacity
            lst->m_storage_vertices = vertices_info.storage_capacity_vid;
            // 有效的节点个数
            lst->m_num_vertices = vertices_info.num_vertices;
            if (!s.ok()) { break; }
            {// 节点的 label-tag 列
                IVertexColumnPtr column = IVertexColumn::OpenColumn(
                        lst->GetStorageDir(),
                        SKG_GLOBAL_LABEL,
                        ColumnDescriptor(SKG_VERTEX_COLUMN_NAME_TAG, ColumnType::TAG)
                                .SetColumnID(ColumnDescriptor::ID_VERTICES_TAG),
                        &s);
                if (!s.ok()) { break; }
                lst->m_vertex_columns.insert(
                        std::make_pair(
                                std::make_pair(SKG_GLOBAL_LABEL_TAG, column->name()),
                                column));
            }
            {// 节点的属性 bitset 列
                lst->m_bitset_column = IVertexColumn::OpenColumn(
                        lst->GetStorageDir(),
                        SKG_GLOBAL_LABEL,
                        ColumnDescriptor(SKG_VERTEX_COLUMN_NAME_BITSET, ColumnType::FIXED_BYTES)
                                .SetColumnID(ColumnDescriptor::ID_VERTICES_BITSET)
                                .SetFixedLength(SKG_MAX_EDGE_PROPERTIES_SIZE / 8),
                        &s);
                if (!s.ok()) { break; }
                lst->m_vertex_columns.insert(
                        std::make_pair(
                                std::make_pair(SKG_GLOBAL_LABEL_TAG, lst->m_bitset_column->name()),
                                lst->m_bitset_column));
            }
            // 异构点属性列的元数据
            s = MetadataFileHandler::ReadVertexAttrConf(lst->GetStorageDir(), &lst->m_vertex_attr);
            if (!s.ok()) { break; }
            // 打开操作点属性列的句柄
            for (auto attributes : lst->m_vertex_attr) {
                for (const auto &col: attributes) {
                    IVertexColumnPtr column = IVertexColumn::OpenColumn(
                            lst->GetStorageDir(),
                            attributes.label,
                            col,
                            &s);
                    if (!s.ok()) { break; }
                    lst->m_vertex_columns.insert(
                            std::make_pair(
                                    std::make_pair(attributes.label_tag, col.colname()),
                                    column));
                }
                if (!s.ok()) { break; }
            }
            if (!s.ok()) { break; }
        } while (false);
        if (s.ok()) {
            pLst->reset(lst);
        } else {
            delete lst;
        }
        return s;
    }

    Status VertexColumnList::Drop() {
        Status s;
        // 关闭句柄
        for (auto &handle : m_vertex_columns) {
            s = handle.second->Drop();
            if (!s.ok()) { return s; }
        }
        // 写入空数据
        MetaHeterogeneousAttributes hetAttributes;
        s = MetadataFileHandler::WriteVertexAttrConf(GetStorageDir(), hetAttributes);
        if (!s.ok()) { return s; }
//        s = MetadataFileHandler::WriteNumVertices(GetStorageDir(), 0, 0);
//        if (!s.ok()) { return s; }
        return s;
    }

    Status VertexColumnList::SetVertexAttr(VertexRequest &req) {
        Status s;

        // 写操作, 需要保证写入的节点id有足够的存储空间
        s = this->UpdateMaxVertexID(req.m_vid);
        if (!s.ok()) { return s; }

        // 查询节点的 label-tag
        EdgeTag_t tag = 0;
        s = this->GetLabelTag(req.m_label, &tag);
        if (!s.ok()) { return s; }  // 节点 label 在 schema 中未定义

        if (req.IsInitLabel()) {// 初始化节点的 tag 属性列
            auto handle = m_vertex_columns.find(std::make_pair(SKG_GLOBAL_LABEL_TAG, SKG_VERTEX_COLUMN_NAME_TAG));
            assert(handle != m_vertex_columns.end());
            if (handle != m_vertex_columns.end()) {
                s = handle->second->Set(req.m_vid, Slice(&tag, sizeof(tag)));
                if (!s.ok()) { return s; }
            }
        }

        // 获取属性 bitset
        ResultProperties properties(0);
        s = m_bitset_column->Get(req.m_vid, &properties, 0);
        if (!s.ok()) { return s; }

        // 从 request 中取出新的 value, 更新属性列相应位置的 value
        for (size_t i = 0; i < req.m_columns.size(); ++i) {
            auto handle = m_vertex_columns.find(std::make_pair(tag, req.m_columns[i].colname()));
            if (handle != m_vertex_columns.end()) {
                if (handle->second->vertexColType() != req.m_columns[i].columnType()) {
                    // 名字相同, 但是类型不一致. FIXME 返回错误
                    SKG_LOG_ERROR("incorrect col[{}]:{} <-> exist: {}",
                                  handle->second->name(),
                                  static_cast<int32_t>(handle->second->vertexColType()),
                                  static_cast<int32_t>(req.m_columns[i].columnType()));
                    continue;
                } else {
#ifdef SKG_REQ_VAR_PROP
                    // TODO 如果设置的是 FIXED_BYTES, 确保字段长度不超过列可容纳的长度
                    Slice bytes_to_put;
                    if (handle->second->vertexColType() == ColumnType::FIXED_BYTES || handle->second->vertexColType() == ColumnType::VARCHAR) {
                        bytes_to_put = req.m_prop.getVar(req.m_columns[i].offset());
                        if (req.m_columns[i].value_size() > handle->second->value_size()) { // 过长时, 截断存储
                            const size_t storage_len = handle->second->value_size();
                            bytes_to_put.remove_suffix(bytes_to_put.size() - storage_len);
                        }
                    } else {
                        bytes_to_put = req.m_prop.get(
                                req.m_columns[i].offset(),
                                req.m_columns[i].offset() + std::min(req.m_columns[i].value_size(), handle->second->value_size())
                        );
                    }
#else
                    bytes_to_put = Slice(req.m_coldata + req.m_columns[i].offset(),
                                         req.m_columns[i].value_size());
#endif
                    if (bytes_to_put.size() < handle->second->value_size()) {
                        // fixed-bytes 清空原来的属性
                        std::string clear_bytes(handle->second->value_size(), '\0');
                        handle->second->Set(req.m_vid, clear_bytes);
                    }
                    s = handle->second->Set(req.m_vid, bytes_to_put);
                    if (!s.ok()) { return s; }
                    properties.set(handle->second->id());
                }
            } else {
                // 设置的属性列不存在
                SKG_LOG_WARNING("missing col[{}]", req.m_columns[i].colname());
            }
        }
        // 更新属性 bitset
        if (s.ok()) {
            s = m_bitset_column->Set(req.m_vid, Slice(properties.bitset().m_bitset, sizeof(properties.bitset().m_bitset)));
        }
        return s;
    }

    Status VertexColumnList::DeleteVertex(VertexRequest &req) {
        Status s;
        // 设置所有属性值为 null
        PropertiesBitset_t bitset;
        s = m_bitset_column->Set(req.m_vid, Slice(bitset.m_bitset, sizeof(bitset.m_bitset)));
        --m_num_vertices;
        return s;
    }

    Status VertexColumnList::CreateNewVertexLabel(const std::string &label) {
        return m_vertex_attr.AddAttributes(MetaAttributes(label));
    }

    Status VertexColumnList::GetVertexAttr(VertexRequest &req, VertexQueryResult *pQueryResult) const {
        assert(pQueryResult != nullptr);
        pQueryResult->Clear(); // 清空结果集, 防止结果集中存着之前的数据

        Status s;

        if (req.m_labelTag == 0) {
            s = this->GetLabelTag(req.m_label, &req.m_labelTag);
            if (!s.ok()) { return s; }
        }

        // 根据 request 中的 column-names 在存储的属性列中, 匹配指定查询的属性列信息
        MetaHeterogeneousAttributes hetPropertiesOfQuery;
        s = m_vertex_attr.MatchQueryMetadata(req.GetColumns(), &hetPropertiesOfQuery);
        if (!s.ok()) { return s; }

        s = FillOneVertex(hetPropertiesOfQuery, req.m_labelTag, req.m_vertex, req.m_vid, pQueryResult);
        if (!s.ok()) { return s; }
        for (const auto &i : req.m_more) {
            s = FillOneVertex(hetPropertiesOfQuery, i.tag, i.vertex, i.vid, pQueryResult);
            if (!s.ok()) { return s; }
        }

        // 结果集的 metadata
        s = pQueryResult->SetResultMetadata(hetPropertiesOfQuery);
        return s;
    }

    Status VertexColumnList::FillOneVertex(
            const MetaHeterogeneousAttributes &hetProp,
            const EdgeTag_t tag,
            const std::string &vertex, const vid_t vid,
            VertexQueryResult *result) const {
        // FIXME this code is awful
        Status s;

        // 查询的id超过当前最大节点id, 返回节点不存在
        if (vid > m_max_vertices_id) { return Status::NotExist(); }

        // 从属性列中获取属性value, 按照查询顺序组织回包
        auto propOfQuery = hetProp.GetAttributesByTag(tag);
        if (propOfQuery == hetProp.end()) {
            return Status::InvalidArgument(fmt::format("vertex's tag `{}' not exist.", tag));
        }

        // 获取属性是否为 null 的 bitset
        ResultProperties properties(propOfQuery->GetColumnsValueByteSize());
        s = m_bitset_column->Get(vid, &properties, 0);
        if (!s.ok()) { return s; }

        size_t offset = 0;
        for (auto col = propOfQuery->begin(); col != propOfQuery->end(); ++col) {
            SKG_LOG_TRACE("filling col`{}' data into offset:{}, {}-bytes",
                          col->colname(), offset,
                          col->value_size());
            auto handle = m_vertex_columns.find(std::make_pair(tag, col->colname()));
            if (handle != m_vertex_columns.end()) {
                s = handle->second->Get(vid, &properties, offset);
                if (!s.ok()) { return s; }
            } else {
                // 查询的属性列不存在
            }
            offset += col->value_size();
        }

        // 节点属性数据
        s = result->Receive(tag, vid, vertex, properties);
        return s;
    }

    Status VertexColumnList::CreateVertexAttrCol(const std::string &label, ColumnDescriptor config) {
        Status s;
        // 更新节点属性配置文件
        auto attributes = m_vertex_attr.GetAttributesByLabel(label);
        if (attributes == m_vertex_attr.end()) {
            return Status::NotExist(fmt::format("vertex label: `{}' not exist", label));
        }
        s = attributes->AddColumn(config);
        if (!s.ok()) { return s; }
        s = MetadataFileHandler::WriteVertexAttrConf(GetStorageDir(), m_vertex_attr);
        if (!s.ok()) { return s; }

        const ColumnDescriptor *const p = attributes->GetColumn(config, true);
        assert(p != nullptr);
        // 创建存储文件
        IVertexColumn::CreateWithCapacity(GetStorageDir(), label, *p, m_storage_vertices);
        // 开启操作句柄
        IVertexColumnPtr column = IVertexColumn::OpenColumn(
                GetStorageDir(),
                label, *p, &s
        );
        if (!s.ok()) { return s; }
        m_vertex_columns.insert(std::make_pair(
                std::make_pair(attributes->label_tag, config.colname()),
                column));

        return s;
    }

    Status VertexColumnList::DeleteVertexAttrCol(const std::string &label, const std::string &columnName) {
        Status s;

        EdgeTag_t tag;
        s = this->GetLabelTag(label, &tag);
        if (!s.ok()) { return s; }

        // 关闭操作句柄
        const auto handle = m_vertex_columns.find(std::make_pair(tag, columnName));
        if (handle == m_vertex_columns.end()) {
            return Status::NotExist(fmt::format("vertex column of label,name: `{}',`{}'not exist.", label, columnName));
        }
        s = handle->second->Close();
        if (!s.ok()) { return s; }
        // TODO 删除存储的文件

        // 更新节点属性配置文件
        auto attributes = m_vertex_attr.GetAttributesByLabel(label);
        if (attributes == m_vertex_attr.end()) {
            return Status::NotExist(fmt::format("vertex label: `{}' not exist", label));
        }
        s = attributes->DeleteColumn(columnName);
        if (!s.ok()) { return s; }
        s = MetadataFileHandler::WriteVertexAttrConf(GetStorageDir(), m_vertex_attr);
        if (!s.ok()) { return s; }

        return s;
    }

    vid_t VertexColumnList::GetNumVertices() const {
        // FIXME 暂时不考虑删除点后, 调整 num_vertices
        return m_max_vertices_id + 1;
        //return m_num_vertices;
    }

    vid_t VertexColumnList::AllocateNewVid() {
        m_max_vertices_id++;
        m_num_vertices++;
        UpdateMaxVertexID(m_max_vertices_id);
        return m_max_vertices_id;
    }

    Status VertexColumnList::UpdateMaxVertexID(vid_t vid) {
        Status s;
        if (vid >= m_max_vertices_id) {
            // 设置的节点id超过原来的最大节点id
            m_max_vertices_id = vid;
            // 节点属性存储空间不足, 需要扩充相应的存储空间
            if (m_max_vertices_id + 1 > m_storage_vertices) {
                const vid_t capacity_id = GetNextStorageCapacity(m_max_vertices_id);
                SKG_LOG_DEBUG("extending to fit {}, should be {}", m_max_vertices_id, capacity_id);
                for (auto &column : m_vertex_columns) {
                    s = column.second->EnsureStorage(capacity_id);
                    if (!s.ok()) { return s; }
                }
                // update
                m_storage_vertices = capacity_id;
            }
        }
        return s;
    }

    Status VertexColumnList::Flush() {
        Status s;
        SKG_LOG_DEBUG("flushing vertex-column-list..", "");
        // 节点个数
        MetaNumVertices vertices_info;
        vertices_info.max_allocated_vid = m_max_vertices_id;
        vertices_info.storage_capacity_vid = m_storage_vertices;
        vertices_info.num_vertices = m_num_vertices;
        s = MetadataFileHandler::WriteNumVertices(GetStorageDir(), vertices_info);
        if (!s.ok()) { return s; }
        // 节点属性
        for (auto handle: m_vertex_columns) {
            s = handle.second->Flush();
            if (!s.ok()) { return s; }
        }
        // 节点属性列配置
        s = MetadataFileHandler::WriteVertexAttrConf(GetStorageDir(), m_vertex_attr);
        if (!s.ok()) { return s; }
        return s;
    }

    Status VertexColumnList::GetLabelTag(const std::string &label, EdgeTag_t *tag) const {
        const auto attributes = m_vertex_attr.GetAttributesByLabel(label);
        if (attributes == m_vertex_attr.end()) {
            return Status::NotExist(fmt::format("vertex label: [{}]", label));
        } else {
            *tag = attributes->label_tag;
            return Status::OK();
        }
    }

    Status VertexColumnList::ExportData(const std::string &out_dir, std::shared_ptr<IDEncoder> encoder) {
        Status s;
        const std::string v_dir = fmt::format("{}/vertices", out_dir);
        s = Env::Default()->CreateDirIfMissing(v_dir);
        if (!s.ok()) { return s; }
        std::map<std::string, std::unique_ptr<WritableFile>> files;
        EnvOptions options;
        for (const auto &prop: m_vertex_attr) {
            std::unique_ptr<WritableFile> f;
            const std::string filename = fmt::format("{}/{}.csv", v_dir, prop.label);
            s = Env::Default()->NewWritableFile(filename, &f, options);
            if (!s.ok()) { return s; }
            files.insert(std::make_pair(prop.label, std::move(f)));
        }
        std::string label, vertex;
        for (vid_t id = 0; id <= m_max_vertices_id; ++id) {
            // vid -> label, vertex
            s = encoder->GetVertexByID(id, &label, &vertex);
            if (!s.ok()) {
                SKG_LOG_WARNING("vid: {}, error: {}", id, s.ToString());
                s = Status::OK();
                continue;
            }
            if (label.empty()) {
                // 
                auto labels = GetVertexLabels();
                if (labels.size() == 1) {label = labels[0];} else {label = "customer";}
            }
            // 直接复用接口查出所有属性 TODO 提高性能
            VertexRequest req(label, id);
            req.SetQueryColumnNames(IRequest::QUERY_ALL_COLUMNS);
            VertexQueryResult result;
            s = this->GetVertexAttr(req, &result);
            if (!s.ok()) {
                SKG_LOG_WARNING("vid: {}, error: {}", id, s.ToString());
                s = Status::OK();
                continue;
            }
            if (!result.MoveNext()) {
                SKG_LOG_WARNING("vid: {}, error: no query result", id);
                s = Status::OK();
                continue;
            } else {
                // FIXME 如果 find 找不到?
                std::unique_ptr<WritableFile> &f = files.find(label)->second;
                // TODO 检查返回值
                s = f->Append(vertex);
                const auto metadata = result.GetMetaData(&s);
                for (size_t i = 0; i < metadata->GetColumnCount(); ++i) {
                    s = f->Append(",");
                    // null 值
                    if (result.IsNull(i, &s)) {
                        s = f->Append("\\NULL");
                        continue;
                    }
                    // 取出值 uhuan: TODO varchar multi-value
                    std::stringstream ss;
                    switch (metadata->GetColumnType(i)) {
                        case ColumnType::INT32:
                            ss << result.GetInt32(i, &s);
                            s = f->Append(ss.str());
                            break;
                        case ColumnType::FLOAT:
                            s = f->Append(fmt::format("{}", result.GetFloat(i, &s)));
                            break;
                        case ColumnType::TIME:
                            s = f->Append(fmt::format("{}", result.GetTimeString(i, &s)));
                            break;
                        case ColumnType::FIXED_BYTES:
                            s = f->Append(fmt::format("{}", result.GetString(i, &s)));
                            break;
                        case ColumnType::INT64:
                            s = f->Append(fmt::format("{}", result.GetInt64(i, &s)));
                            break;
                        case ColumnType::DOUBLE:
                            s = f->Append(fmt::format("{}", result.GetDouble(i, &s)));
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
        }
        return s;
    }
}
