#ifndef STARKNOWLEDGEGRAPHDATABASE_VERTICESATTRIBUTEACTION_H
#define STARKNOWLEDGEGRAPHDATABASE_VERTICESATTRIBUTEACTION_H

#include "fs/VertexRequest.h"
#include "fs/IDEncoder.h"

//#include "idencoder/RocksDBIdEncoder.h"
#include "fs/ColumnDescriptorUtils.h"
#include "fs/IVertexColumn.h"
#include "fs/IVertexColumnImpl.h"
#include "fs/Metadata.hpp"
#include "fs/VertexColumnList.h"
#include "fileparser.hpp"

namespace skg { namespace preprocess {

    class VerticesAttributeAction: public Action {
    private:
        MetaAttributes m_attributes;
        std::shared_ptr<VertexColumnList> m_vertex_columns_handle;
        std::shared_ptr<IDEncoder> m_encoder;
        char m_sep;
        bool m_is_string_vertices;
        bool m_ignore_header;
    public:
        VerticesAttributeAction(
                const MetaAttributes &attributes,
                const std::shared_ptr<VertexColumnList> &vertex_columns_handle,
                char sep,
                const std::shared_ptr<IDEncoder> &encoder,
                bool isStringVertex, bool ignoreHeader=false)
                : m_attributes(attributes),
                  m_vertex_columns_handle(vertex_columns_handle),
                  m_encoder(encoder),
                  m_sep(sep),
                  m_is_string_vertices(isStringVertex),
                  m_ignore_header(ignoreHeader) {
            assert(!m_is_string_vertices || (m_is_string_vertices && encoder != nullptr));
        }

        Status operator()(size_t linenum, char *line) override {
            if (linenum == 0 && m_ignore_header) { return Status::OK(); }
            if (strlen(line) == 0) { return Status::OK(); }

            std::vector<Slice> spart = StringUtils::split(line, m_sep);
            if (spart.size() != m_attributes.GetColumnsSize() + 1) {
                return Status::InvalidArgument(fmt::format(
                        "Parse error, expect {} fields, but get {}. line({}):`{}'",
                        m_attributes.GetColumnsSize() + 1, spart.size(), linenum, line
                ));
            }
            Status s;
            const std::string vertex = spart[0].ToString();

            // 获取节点id
            vid_t vid = 0;
            if (!m_is_string_vertices) {
                try {
                    // FIXME vid_t -> uint64 的时候, 需要切换为 ParseUint64
                    vid = StringUtils::ParseUint32(vertex);
                } catch (std::out_of_range &e) {
                    SKG_LOG_ERROR("vid is out of range. `{}'", vertex);
                    return Status::NotImplement(fmt::format("vid is out of range. `{}'", vertex));
                }
            } else {
                // 分配 vertex_id
                vid = m_vertex_columns_handle->AllocateNewVid();
                //SKG_LOG_DEBUG("allocated id: {} -> {}", vertex, vid);
                s = m_encoder->Put(m_attributes.label, vertex, vid);
                if (!s.ok()) { return s; }
            }
            VertexRequest req;
            req.SetVertex(m_attributes.label, vertex);
            req.SetVertex(m_attributes.label, vid);
            req.SetInitLabel(); // 初始化节点的 label-tag

            char buff[SKG_MAX_EDGE_PROPERTIES_BYTES];
            size_t idx = 0;
            for (const auto &col: m_attributes) {
                const std::string field = spart[idx + 1].ToString();
                // 截断的时候, warning 提示
                if (col.columnType() == ColumnType::FIXED_BYTES && col.value_size() < field.size()) {
                    SKG_LOG_WARNING("Truncated while setting vertex:{} attr: {}, sz: {}/{}", vertex, col.colname(), col.value_size(), field.size());
                }
                s = ColumnDescriptorUtils::ParseValueBytes(col, field.c_str(), buff);
                if (!s.ok()) { return s; }
                switch (col.columnType()) {
                    case ColumnType::INT32:
                        s = req.SetInt32(col.colname(), *reinterpret_cast<int32_t *>(buff));
                        break;
                    case ColumnType::INT64:
                        s = req.SetInt64(col.colname(), *reinterpret_cast<int64_t *>(buff));
                        break;
                    case ColumnType::FLOAT:
                        s = req.SetFloat(col.colname(), *reinterpret_cast<float *>(buff));
                        break;
                    case ColumnType::DOUBLE:
                        s = req.SetDouble(col.colname(), *reinterpret_cast<double *>(buff));
                        break;
                    case ColumnType::FIXED_BYTES:
                        s = req.SetString(col.colname(), field.c_str(), field.size());
                        break;
                        // TODO 节点属性数据导入, time / group fields
                    default:
                        assert(false);
//                    case ColumnType::TIME:
//                        req.SetTimeString(col.colname(), field.c_str(), col.GetTimeFormat());
                }
                if (!s.ok()) { return s; }
                idx++;
            }
            s = m_vertex_columns_handle->SetVertexAttr(req);
            if (!s.ok()) { return s; }

            return s;
        }
    };

}}

#endif //STARKNOWLEDGEGRAPHDATABASE_VERTICESATTRIBUTEACTION_H
