#include <string>

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "util/status.h"
#include "EdgeRequest.h"
#include "ColumnDescriptorUtils.h"

namespace skg {
    EdgeRequest::EdgeRequest()
            : EdgeRequest("", 0, 0) {
    }

    EdgeRequest::EdgeRequest(const std::string &label,
                             const std::string &srcVertexLabel, const std::string &srcVertex,
                             const std::string &dstVertexLabel, const std::string &dstVertex)
            : EdgeRequest(label, srcVertexLabel, srcVertex, dstVertexLabel, dstVertex, 0, 0)  {
    }

    EdgeRequest::EdgeRequest(const EdgeLabel &label, const std::string &srcVertex, const std::string &dstVertex)
            : EdgeRequest(label.edge_label, label.src_label, srcVertex, label.dst_label, dstVertex) {
    }

    EdgeRequest::EdgeRequest(const std::string &label, vid_t srcVid, vid_t dstVid)
            : EdgeRequest(label, "", "", "", "", srcVid, dstVid) {
    }

    EdgeRequest::EdgeRequest(
            const std::string &label,
            const std::string &srcVertexLabel, const std::string &srcVertex,
            const std::string &dstVertexLabel, const std::string &dstVertex,
            vid_t srcVid, vid_t dstVid)
            :m_label(label),
#ifndef SKG_SRC_SPLIT_SHARD
             m_srcVertexLabel(srcVertexLabel), m_srcVertex(srcVertex),
             m_dstVertexLabel(dstVertexLabel), m_dstVertex(dstVertex),
#else
             m_srcVertexLabel(dstVertexLabel), m_srcVertexLabelTag(0), m_srcVertex(dstVertex),
             m_dstVertexLabel(srcVertexLabel), m_dstVertexLabelTag(0), m_dstVertex(srcVertex),
#endif
             m_srcVid(srcVid), m_dstVid(dstVid),
             m_columns(),
#ifdef SKG_REQ_VAR_PROP
             m_prop(0)
#else
             m_offset(0)
#endif
 {
#ifndef SKG_REQ_VAR_PROP
        memset(m_coldata, 0, sizeof(m_coldata));
#endif
    }

    EdgeRequest::~EdgeRequest() {

    }

    void EdgeRequest::SetEdge(const EdgeLabel &label,
                              const std::string &srcVertex,
                              const std::string &dstVertex) {
        m_label = label.edge_label;
        m_srcVertexLabel = label.src_label;
        m_srcVertex = srcVertex;
        m_dstVertexLabel = label.dst_label;
        m_dstVertex = dstVertex;
    }

    void EdgeRequest::SetEdge(const std::string &label,
                              const std::string &srcVertexLabel, const std::string &srcVertex,
                              const std::string &dstVertexLabel, const std::string &dstVertex) {
        m_label = label;
#ifndef SKG_SRC_SPLIT_SHARD
        m_srcVertexLabel = srcVertexLabel;
        m_srcVertex = srcVertex;
        m_dstVertexLabel = dstVertexLabel;
        m_dstVertex = dstVertex;
#else
        // 逆转 src <-> dst
        m_dstVertexLabel = srcVertexLabel;
        m_dstVertex = srcVertex;
        m_srcVertexLabel = dstVertexLabel;
        m_srcVertex = dstVertex;
#endif
    }

    void EdgeRequest::SetEdge(const std::string &label, vid_t srcVid, vid_t dstVid) {
        m_label = label;
#ifndef SKG_SRC_SPLIT_SHARD
        m_srcVid = srcVid;
        m_dstVid = dstVid;
#else
        m_dstVid = srcVid;
        m_srcVid = dstVid;
#endif
    }

    void EdgeRequest::Clear() {
        IRequest::Clear(); // 调用父类的 clear
        m_label.clear();
        m_srcVid = 0;
        m_dstVid = 0;
        m_srcVertex.clear();
        m_dstVertex.clear();
#ifdef SKG_REQ_VAR_PROP
        m_prop.clear();
#else
        m_offset = 0;
#endif
    }

    Status EdgeRequest::SetQueryColumnNames(const std::vector <std::string> &columns) {
        m_columns.clear();
        for (size_t i = 0; i < columns.size(); ++i) {
            m_columns.emplace_back(columns[i], ColumnType::NONE);
        }
        return Status::OK();
    }

    Status EdgeRequest::SetWeight(EdgeWeight_t weight) {
#ifdef SKG_REQ_VAR_PROP
        const size_t offset = m_prop.fixed_bytes_length();
        m_columns.emplace_back(ColumnDescriptor("weight", ColumnType::WEIGHT).SetOffset(offset));
        m_prop.resize_fixed_bytes(offset + sizeof(EdgeWeight_t));
        return m_prop.put(weight, offset, 0);
#else
        m_columns.emplace_back(ColumnDescriptor("weight", ColumnType::WEIGHT).SetOffset(m_offset));
        memcpy(m_coldata + m_offset, &weight, sizeof(EdgeWeight_t));
        m_offset += sizeof(EdgeWeight_t);
        return Status::OK();
#endif
    }

    Status EdgeRequest::SetInt32(const std::string &column, int32_t value) {
#ifdef SKG_REQ_VAR_PROP
        const size_t offset = m_prop.fixed_bytes_length();
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::INT32).SetOffset(offset));
        m_prop.resize_fixed_bytes(offset + sizeof(int32_t));
        return m_prop.put(value, offset, 0);
#else
        // TODO check coldata 中是否满了
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::INT32).SetOffset(m_offset));
        memcpy(m_coldata + m_offset, &value, sizeof(int32_t));
        m_offset += sizeof(int32_t);
        return Status::OK();
#endif
    }

    Status EdgeRequest::SetFloat(const std::string &column, float value) {
#ifdef SKG_REQ_VAR_PROP
        const size_t offset = m_prop.fixed_bytes_length();
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::FLOAT32).SetOffset(offset));
        m_prop.resize_fixed_bytes(offset + sizeof(float));
        return m_prop.put(value, offset, 0);
#else
        // TODO check coldata 中是否满了
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::FLOAT).SetOffset(m_offset));
        memcpy(m_coldata + m_offset, &value, sizeof(float));
        m_offset += sizeof(float);
        return Status::OK();
#endif
    }

    Status EdgeRequest::SetString(const std::string &column, const char *value, size_t valueSize) {
#ifdef SKG_REQ_VAR_PROP
        const size_t offset = m_prop.fixed_bytes_length();
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::FIXED_BYTES).SetOffset(offset).SetFixedLength(valueSize));
        m_prop.resize_fixed_bytes(offset + sizeof(int32_t)); // 扩充存 varchar 的长度
        return m_prop.putVar(value, offset, 0);
#else
        // TODO check coldata 中是否满了
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::FIXED_BYTES).SetOffset(m_offset).SetFixedLength(valueSize));
        memcpy(m_coldata + m_offset, value, valueSize);
        m_offset += valueSize;
        return Status::OK();
#endif
    }

    Status EdgeRequest::SetDouble(const std::string &column, double value) {
#ifdef SKG_REQ_VAR_PROP
        const size_t offset = m_prop.fixed_bytes_length();
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::DOUBLE).SetOffset(offset));
        m_prop.resize_fixed_bytes(offset + sizeof(double));
        return m_prop.put(value, offset, 0);
#else
        // TODO check coldata 中是否满了
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::DOUBLE).SetOffset(m_offset));
        memcpy(m_coldata + m_offset, &value, sizeof(double));
        m_offset += sizeof(double);
        return Status::OK();
#endif
    }

    Status EdgeRequest::SetInt64(const std::string &column, int64_t value) {
#ifdef SKG_REQ_VAR_PROP
        const size_t offset = m_prop.fixed_bytes_length();
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::INT64).SetOffset(offset));
        m_prop.resize_fixed_bytes(offset + sizeof(int64_t));
        return m_prop.put(value, offset, 0);
#else
        // TODO check coldata 中是否满了
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::INT64).SetOffset(m_offset));
        memcpy(m_coldata + m_offset, &value, sizeof(int64_t));
        m_offset += sizeof(double);
        return Status::OK();
#endif
    }

    Status EdgeRequest::SetTimestamp(const std::string &column, time_t value) {
#ifdef SKG_REQ_VAR_PROP
        const size_t offset = m_prop.fixed_bytes_length();
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::TIME).SetOffset(offset));
        m_prop.resize_fixed_bytes(offset + sizeof(time_t));
        return m_prop.put(value, offset, 0);
#else
        // TODO check coldata 中是否满了
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::TIME).SetOffset(m_offset));
        memcpy(m_coldata + m_offset, &value, sizeof(time_t));
        m_offset += sizeof(time_t);
        return Status::OK();
#endif
    }

    Status EdgeRequest::SetTimeString(const std::string &column, const char *value, size_t valueSize) {
#ifdef SKG_REQ_VAR_PROP
        // TODO check coldata 中是否满了
        const size_t offset = m_prop.fixed_bytes_length();
        time_t t;
        ColumnDescriptor col(column, ColumnType::TIME);
        col.SetOffset(offset).SetTimeFormat("%Y-%m-%d %H:%M:%S"); // TODO 支持用户自定义格式
        Status s;
        s = ColumnDescriptorUtils::ParseValueBytes(col, value, &t);
        if (s.ok()) {
            m_columns.emplace_back(col);
            m_prop.resize_fixed_bytes(offset + sizeof(time_t));
            s = m_prop.put(t, offset, 0);
        }
        return s;
#else
        // TODO check coldata 中是否满了
        time_t t;
        ColumnDescriptor col(column, ColumnType::TIME);
        col.SetOffset(m_offset);
        col.SetTimeFormat("%Y-%m-%d %H:%M:%S"); // TODO 支持用户自定义格式
        Status s;
        s = ColumnDescriptorUtils::ParseValueBytes(col, value, &t);
        if (s.ok()) {
            m_columns.emplace_back(col);
            memcpy(m_coldata + m_offset, &t, sizeof(t));
            m_offset += sizeof(time_t);
        }
        return s;
#endif
    }

std::string EdgeRequest::ToDebugString() const {
    rapidjson::StringBuffer sb; 
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    writer.StartObject(); // {
    writer.Key("label");
    writer.String(EdgeLabel(m_label, m_srcVertexLabel, m_dstVertexLabel).ToString().c_str());
    writer.Key("src");
    writer.String(m_srcVertex.c_str());
    writer.Key("dst");
    writer.String(m_dstVertex.c_str());

    writer.Key("cols");
    writer.StartArray(); // [
    for (const auto& col : m_columns) {
        writer.StartObject(); // {
        writer.Key("name");
        writer.String(col.colname().c_str());
        writer.Key("type");
        writer.String(ColTypeToCString(col.columnType()));
        writer.EndObject();  // }
    }

    writer.Key("prop");
    writer.StartObject(); // {
    writer.Key("fixed_length");
    writer.Uint64(m_prop.fixed_bytes_length());
    writer.Key("var_length");
    writer.Uint64(m_prop.var_bytes_length());
    writer.EndObject();  // }

    writer.EndArray(); // ]
    writer.EndObject(); // }
    return sb.GetString();
}

// copy functions
EdgeRequest &EdgeRequest::operator=(const EdgeRequest &rhs) {
    if (this != &rhs) {
        m_label = rhs.m_label;
        m_srcVertexLabel = rhs.m_srcVertexLabel;
        m_srcVertex = rhs.m_srcVertex;
        m_dstVertexLabel = rhs.m_dstVertexLabel;
        m_dstVertex = rhs.m_dstVertex;
        m_srcVid = rhs.m_srcVid;
        m_dstVid = rhs.m_dstVid;
        m_columns = rhs.m_columns;
#ifdef SKG_REQ_VAR_PROP
        m_prop = rhs.m_prop;
#else
        memcpy(m_coldata, rhs.m_coldata, sizeof(m_coldata));
        m_offset = rhs.m_offset;
#endif
    }
    return *this;
}

EdgeRequest::EdgeRequest(const EdgeRequest &rhs)
#ifdef SKG_REQ_VAR_PROP
    : m_prop(0)
#endif
{
    *this = rhs;
}

// move copy functions
EdgeRequest &EdgeRequest::operator=(EdgeRequest &&rhs) noexcept {
    if (this != &rhs) {
        m_label.swap(rhs.m_label);
        m_srcVertexLabel.swap(rhs.m_srcVertexLabel);
        m_srcVertex.swap(rhs.m_srcVertex);
        m_dstVertexLabel.swap(rhs.m_dstVertexLabel);
        m_dstVertex.swap(rhs.m_dstVertex);
        m_srcVid = rhs.m_srcVid;
        m_dstVid = rhs.m_dstVid;
        m_columns.swap(rhs.m_columns);
#ifdef SKG_REQ_VAR_PROP
        m_prop = std::move(rhs.m_prop);
#else
        memcpy(m_coldata, rhs.m_coldata, sizeof(m_coldata));
        m_offset = rhs.m_offset;
#endif
    }
    return *this;
}

EdgeRequest::EdgeRequest(EdgeRequest &&rhs) noexcept
#ifdef SKG_REQ_VAR_PROP
    : m_prop(0)
#endif
{
    *this = std::move(rhs);
}
}
