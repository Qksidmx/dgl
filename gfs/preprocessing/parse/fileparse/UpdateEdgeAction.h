#ifndef STARKNOWLEDGEGRAPHDATABASE_UPDATE_EDGE_ACTION_H
#define STARKNOWLEDGEGRAPHDATABASE_UPDATE_EDGE_ACTION_H

#include "gq/VertexRequest.h"
#include "fs/IDEncoder.h"

#include "fs/ColumnDescriptorUtils.h"
#include "fs/IVertexColumn.h"
#include "fs/IVertexColumnImpl.h"
#include "fs/Metadata.hpp"
#include "fs/VertexColumnList.h"
#include "fileparser.hpp"

namespace skg {
namespace preprocess {

class UpdateEdgeAction : public Action {
private:
    SkgDB *m_db;
    MetaAttributes m_properties;
    TextInput m_input;
    bool m_is_create_if_not_exist;
    bool m_is_check_exist;
    size_t m_num_batch;
    metrics &m;
public:
    UpdateEdgeAction(
            SkgDB *db, const MetaAttributes &properties, const TextInput &input,
            bool isCreateIfNotExist, bool isCheckExist,
            size_t num_batch,
            metrics &m_)
            : m_db(db), m_properties(properties), m_input(input),
              m_is_create_if_not_exist(isCreateIfNotExist),
              m_is_check_exist(isCheckExist),
              m_num_batch(num_batch),
              m(m_) {
    }

    Status operator()(size_t linenum, char *line) override {
        if (linenum == 0 && m_input.IsIgnoreHeader()) { return Status::OK(); }
        if (strlen(line) == 0) { return Status::OK(); }

        std::vector<std::string> spart;
        StringUtils::split(line, m_input.delimiter(), spart);
        if (spart.size() != m_properties.GetColumnsSize() + 2) {
            return Status::InvalidArgument(fmt::format(
                    "Parse error, expect {} fields, but get {}. line({}):`{}'",
                    m_properties.GetColumnsSize() + 2, spart.size(), linenum, line
            ));
        }
        Status s;
        const std::string &src = spart[0];
        const std::string &dst = spart[1];

        m.start_time("update-edge", metric_duration_type::MILLISECONDS);

        EdgeRequest req(
                m_properties.label,
                m_properties.src_label, src,
                m_properties.dst_label, dst);

        char buff[SKG_MAX_EDGE_PROPERTIES_BYTES];
        size_t idx = 0;
        for (const auto &col: m_properties) {
            const std::string &field = spart[idx + 2];
            // 截断的时候, warning 提示
            if (col.columnType() == ColumnType::FIXED_BYTES && col.value_size() < field.size()) {
                SKG_LOG_WARNING("Truncated while setting edge:{}->{} attr: {}, sz: {}/{}", src, dst, col.colname(),
                                col.value_size(), field.size());
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
        req.SetCreateIfNotExist(m_is_create_if_not_exist);
        req.SetCheckExist(m_is_check_exist);
        //s = m_db->SetEdgeAttr(req);
        s = m_db->AddEdge(req);
        if (linenum % m_num_batch == 0 && linenum != 0) {
            s = m_db->Flush();
        }
        m.stop_time("update-edge");
        if (!s.ok()) { return s; }

        return s;
    }
};

class BulkUpdateEdgeAction : public Action {
private:
    SkgDB *m_db;
    MetaAttributes m_properties;
    TextInput m_input;
    bool m_is_create_if_not_exist;
    bool m_is_check_exist;
    metrics &m;
    std::vector<EdgeRequest> m_requests;
    BulkUpdateOptions m_bulk_options;
public:
    BulkUpdateEdgeAction(
            SkgDB *db, const MetaAttributes &properties, const TextInput &input,
            bool isCreateIfNotExist, bool isCheckExist,
            size_t num_batch, const std::string &split_type,
            metrics &m_)
            : m_db(db), m_properties(properties), m_input(input),
              m_is_create_if_not_exist(isCreateIfNotExist),
              m_is_check_exist(isCheckExist), m(m_) {
        m_bulk_options.num_edges_batch = std::max(1ul, num_batch);
        if (split_type == "sort") {
            m_bulk_options.split_type = BulkUpdateOptions::SortSplit;
        } else {
            m_bulk_options.split_type = BulkUpdateOptions::VecSplit;
        }
    }

    Status operator()(size_t linenum, char *line) override {
        if (linenum == 0 && m_input.IsIgnoreHeader()) { return Status::OK(); }
        if (strlen(line) == 0) { return Status::OK(); }

        std::vector<std::string> spart;
        StringUtils::split(line, m_input.delimiter(), spart);
        if (spart.size() != m_properties.GetColumnsSize() + 2) {
            return Status::InvalidArgument(fmt::format(
                    "Parse error, expect {} fields, but get {}. line({}):`{}'",
                    m_properties.GetColumnsSize() + 2, spart.size(), linenum, line
            ));
        }
        Status s;
        const std::string &src = spart[0];
        const std::string &dst = spart[1];

        m.start_time("update-edge", metric_duration_type::MILLISECONDS);

        EdgeRequest req(
                m_properties.label,
                m_properties.src_label, src,
                m_properties.dst_label, dst);

        char buff[SKG_MAX_EDGE_PROPERTIES_BYTES];
        size_t idx = 0;
        for (const auto &col: m_properties) {
            const std::string &field = spart[idx + 2];
            // 截断的时候, warning 提示
            if (col.columnType() == ColumnType::FIXED_BYTES && col.value_size() < field.size()) {
                SKG_LOG_WARNING("Truncated while setting edge:{}->{} attr: {}, sz: {}/{}", src, dst, col.colname(),
                                col.value_size(), field.size());
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
        req.SetCreateIfNotExist(m_is_create_if_not_exist);
        req.SetCheckExist(m_is_check_exist);
        m_requests.emplace_back(std::move(req));

        if (m_requests.size() == m_bulk_options.num_edges_batch) {
            s = this->Done();
        }
        m.stop_time("update-edge");

        return s;
    }

    Status Done() {
        Status s = m_db->BulkUpdateEdges(m_requests, BulkUpdateOptions());
        m_requests.clear();
        return s;
    }
};
}
}

#endif //STARKNOWLEDGEGRAPHDATABASE_UPDATE_EDGE_ACTION_H
