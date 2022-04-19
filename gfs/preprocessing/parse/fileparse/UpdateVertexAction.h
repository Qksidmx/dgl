#ifndef STARKNOWLEDGEGRAPHDATABASE_UPDATE_VERTEX_ACTION_H
#define STARKNOWLEDGEGRAPHDATABASE_UPDATE_VERTEX_ACTION_H

#include "gq/VertexRequest.h"
//#include "fs/IDEncoder.h"

#include "fs/ColumnDescriptorUtils.h"
#include "fs/IVertexColumn.h"
#include "fs/IVertexColumnImpl.h"
#include "fs/Metadata.hpp"
#include "fs/VertexColumnList.h"
#include "fileparser.hpp"

namespace skg { namespace preprocess {

    class UpdateVertexAction: public Action {
    private:
        SkgDB *m_db;
        MetaAttributes m_properties;
        TextInput m_input;
        bool m_create_if_not_exist;
        metrics &m;
    public:
        UpdateVertexAction(SkgDB *db, const MetaAttributes &properties,
                           const TextInput &input,
                           bool is_create_if_not_exist, metrics &m_)
                : m_db(db), m_properties(properties), m_input(input)
                  , m_create_if_not_exist(is_create_if_not_exist)
                  , m(m_)
        {}

        Status operator()(size_t linenum, char *line) override {
            if (linenum == 0 && m_input.IsIgnoreHeader()) { return Status::OK(); }
            if (strlen(line) == 0) { return Status::OK(); }

            std::vector<Slice> spart = StringUtils::split(line, m_input.delimiter());
            if (spart.size() != m_properties.GetColumnsSize() + 1) {
                return Status::InvalidArgument(fmt::format(
                        "Parse error, expect {} fields, but get {}. line({}):`{}'",
                        m_properties.GetColumnsSize() + 1, spart.size(), linenum, line
                ));
            }
            Status s;

            m.start_time("update-vertex", metric_duration_type::MILLISECONDS);
            const std::string vertex = spart[0].ToString();

            VertexRequest req;
            req.SetVertex(m_properties.label, vertex);

            char buff[SKG_MAX_EDGE_PROPERTIES_BYTES];
            size_t idx = 0;
            for (const auto &col: m_properties) {
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
            req.SetCreateIfNotExist(m_create_if_not_exist);
            s = m_db->SetVertexAttr(req);
            m.stop_time("update-vertex");
            if (!s.ok()) { return s; }

            return s;
        }
    };

}}

#endif //STARKNOWLEDGEGRAPHDATABASE_UPDATE_VERTEX_ACTION_H
