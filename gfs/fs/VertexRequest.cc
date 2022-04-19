#include "VertexRequest.h"

#include "VertexRequest.h"

#include "ColumnDescriptorUtils.h"

namespace skg {

VertexRequest::VertexRequest(): VertexRequest("", "", 0) {}

VertexRequest::VertexRequest(const std::string &label, vid_t vid)
    : VertexRequest(label, "", vid) {}

VertexRequest::VertexRequest(const std::string &label, const std::string &vertex)
    : VertexRequest(label, vertex, 0) {}

VertexRequest::VertexRequest(const std::string &label, const std::string &vertex, vid_t vid)
        : m_label(label), m_labelTag(0), m_vid(vid), m_vertex(vertex), m_columns()
#ifdef SKG_REQ_VAR_PROP
          , m_prop(0)
#endif
          , m_flags(0)
{
#ifndef SKG_REQ_VAR_PROP
    m_offset = 0;
    memset(m_coldata, 0, sizeof(m_coldata));
#endif
}

    VertexRequest::~VertexRequest() {

    }

    void VertexRequest::Clear() {
        IRequest::Clear(); // 调用父类的 clear
        m_label.clear();
        m_vid = 0;
        m_vertex.clear();
#ifdef SKG_REQ_VAR_PROP
        m_prop.clear();
#else
        m_offset = 0;
#endif
    }

    void VertexRequest::SetVertex(const std::string &label, const std::string &vertex) {
        m_label = label;
        m_vertex = vertex;
    }

    void VertexRequest::SetVertex(const std::string &label, vid_t vid) {
        m_label = label;
        m_vid = vid;
    }

    Status VertexRequest::SetQueryColumnNames(const std::vector<std::string> &columns) {
        m_columns.clear();
        for (size_t i = 0; i < columns.size(); ++i) {
            m_columns.emplace_back(columns[i], ColumnType::NONE);
        }
        return Status::OK();
    }

    Status VertexRequest::SetInt32(const std::string &column, int32_t value) {
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

    Status VertexRequest::SetFloat(const std::string &column, float value) {
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

    Status VertexRequest::SetString(const std::string &column, const char *value, size_t valueSize) {
#ifdef SKG_REQ_VAR_PROP
        const size_t offset = m_prop.fixed_bytes_length();
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::FIXED_BYTES).SetOffset(offset));
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

    Status VertexRequest::SetDouble(const std::string &column, double value) {
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

    Status VertexRequest::SetInt64(const std::string &column, int64_t value) {
#ifdef SKG_REQ_VAR_PROP
        const size_t offset = m_prop.fixed_bytes_length();
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::INT64).SetOffset(offset));
        m_prop.resize_fixed_bytes(offset + sizeof(int64_t));
        return m_prop.put(value, offset, 0);
#else
        // TODO check coldata 中是否满了
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::INT64).SetOffset(m_offset));
        memcpy(m_coldata + m_offset, &value, sizeof(int64_t));
        m_offset += sizeof(int64_t);
        return Status::OK();
#endif
    }

    Status VertexRequest::SetTimestamp(const std::string &column, time_t value) {
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

    Status VertexRequest::SetTimeString(const std::string &column, const char *value, size_t valueSize) {
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
        ColumnDescriptor d(column, ColumnType::TIME);
        Status s = ColumnDescriptorUtils::ParseValueBytes(d, value, m_coldata + m_offset);
        if (s.ok()) {
            d.SetOffset(m_offset);
            m_columns.emplace_back(d);
            m_offset += d.value_size();
        }
        return s;
#endif
    }

    Status VertexRequest::SetVarChar(const std::string &column, const char *value, size_t valueSize) {
#ifdef SKG_REQ_VAR_PROP
        const size_t offset = m_prop.fixed_bytes_length();
        m_columns.emplace_back(ColumnDescriptor(column, ColumnType::VARCHAR).SetOffset(offset));
        m_prop.resize_fixed_bytes(offset + sizeof(int32_t)); // 扩充存 varchar 的长度
        return m_prop.putVar(Slice(value, valueSize), offset, 0);
#else
        //
        ColumnDescriptor d(column, ColumnType::VARCHAR);
        Status s = ColumnDescriptorUtils::ParseValueBytes(d, value, m_coldata + m_offset);
        if (s.ok()) {
            d.SetOffset(m_offset);
            m_columns.emplace_back(d);
            m_offset += d.value_size();
        }
        return s;
#endif
    }

    const std::vector<ColumnDescriptor> &VertexRequest::GetColumns() const {
        return m_columns;
    }

    const std::string &VertexRequest::GetLabel() const {
        return m_label;
    }

std::string VertexRequest::ToDebugString() const {
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    writer.StartObject(); // {
    writer.Key("label");
    writer.String(m_label.c_str());
    writer.Key("tag");
    writer.Uint(m_labelTag);
    writer.Key("vid");
    writer.Uint64(m_vid);
    writer.Key("vertex");
    writer.String(m_vertex.c_str());

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
    writer.EndArray(); // ]

    writer.Key("prop");
    writer.StartObject(); // {
    writer.Key("fixed_length");
    writer.Uint64(m_prop.fixed_bytes_length());
    writer.Key("var_length");
    writer.Uint64(m_prop.var_bytes_length());
    writer.EndObject();  // }

    writer.Key("flags");
    writer.Uint(m_flags);

    writer.EndObject(); // }
    return sb.GetString();
}

    VertexRequest::VertexRequest(VertexRequest &&rhs) noexcept : VertexRequest() {
        *this = std::move(rhs);
    }

    VertexRequest &VertexRequest::operator=(VertexRequest &&rhs) noexcept {
        if (this != &rhs) {
            std::swap(m_label, rhs.m_label);
            m_vid = rhs.m_vid;
            std::swap(m_vertex, rhs.m_vertex);
            std::swap(m_columns, rhs.m_columns);
#ifdef SKG_REQ_VAR_PROP
            std::swap(m_prop, rhs.m_prop);
#else
            memcpy(m_coldata, rhs.m_coldata, rhs.m_offset);
            m_offset = rhs.m_offset;
#endif
        }
        return *this;
    }

    VertexRequest::VertexRequest(const VertexRequest &rhs)
#ifdef SKG_REQ_VAR_PROP
        : m_prop(0)
#endif
    {
        *this = rhs;
    }

    VertexRequest &VertexRequest::operator=(const VertexRequest &rhs) {
        if (this != &rhs) {
            m_label = rhs.m_label;
            m_vid = rhs.m_vid;
            m_vertex = rhs.m_vertex;
            m_columns = rhs.m_columns;
#ifdef SKG_REQ_VAR_PROP
            m_prop = rhs.m_prop;
#else
            memcpy(m_coldata, rhs.m_coldata, rhs.m_offset);
            m_offset = rhs.m_offset;
#endif
        }
        return *this;
    }
}
