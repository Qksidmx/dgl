
#include "util/types.h"
#include "IRequest.h"
#include "IDEncoder.h"
#include "ColumnDescriptor.h"
#include "VertexQueryResult.h"

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

#include "fmt/time.h"

#include "MetaHeterogeneousAttributes.h"

namespace skg {

    VertexQueryResult::VertexQueryResult()
            : m_row_index(0),
              m_labels(), m_metadatas(),
              m_nlimit(IRequest::NO_LIMIT) {
    }

    VertexQueryResult::~VertexQueryResult() {
    }

    size_t VertexQueryResult::Size() {
        return m_vertices.size();
    }

    void VertexQueryResult::Clear() {
        m_row_index = 0;
        m_vertices.clear();
        m_labels.clear();
        m_metadatas.clear();
    }

    Status VertexQueryResult::Receive(
            EdgeTag_t tag, vid_t vid,
            const std::string &vertex,
            const ResultProperties &prop) {
#ifdef SKG_QUERY_USE_MT
        // 加锁, 防止多线程环境下, 多个线程同时写 m_vertices 导致的出错
        // 使用 std::lock_guard 获取锁, 在析构时自动释放锁. http://zh.cppreference.com/w/cpp/thread/lock_guard
        std::lock_guard<std::mutex> lock(m_receive_lock);
#endif
        if (m_nlimit == IRequest::NO_LIMIT || m_nlimit < static_cast<ssize_t>(m_vertices.size()) + 1) {
            m_vertices.emplace_back(tag, vid, vertex, prop);
            return Status::OK();
        } else {
            return Status::ResultSizeOverLimit(fmt::format("{}", m_nlimit));
        }
    }

    void VertexQueryResult::Receive(EdgeTag_t tag, vid_t vid) {
#ifdef SKG_QUERY_USE_MT
        // 加锁, 防止多线程环境下, 多个线程同时写 m_vertices 导致的出错
        // 使用 std::lock_guard 获取锁, 在析构时自动释放锁. http://zh.cppreference.com/w/cpp/thread/lock_guard
        std::lock_guard<std::mutex> lock(m_receive_lock);
#endif
        m_vertices.emplace_back(tag, vid, "", ResultProperties(0));
    }

    bool VertexQueryResult::IsOverLimit() {
        return !(m_nlimit == IRequest::NO_LIMIT || m_nlimit < static_cast<ssize_t>(m_vertices.size()));
    }

    Status VertexQueryResult::SetResultMetadata(const MetaHeterogeneousAttributes &hetAttributes) {
        Status s;
        for (const auto &attributes :hetAttributes) {
            m_labels.insert(std::make_pair(attributes.label_tag, attributes.label));
            // TODO ResultMetadata 中直接封装 MetaAttributes
            ResultMetadata metadata;
            std::vector<ColumnDescriptor> desc;
            for (const auto &descriptor: attributes) {
                if (descriptor.columnType() == ColumnType::GROUP) {
                    for (const auto &subCol: descriptor) {
                        // FIXME 改为递归的形式
                        desc.push_back(subCol);
                    }
                } else if (descriptor.columnType() == ColumnType::TIME) {
                    desc.push_back(descriptor);
                } else {
                    desc.push_back(descriptor);
                }
            }
            metadata.SetColumns(desc);
            m_metadatas.insert(std::make_pair(attributes.label, metadata));
        }
        return s;
    }

    bool VertexQueryResult::HasNext() const {
        // m_row_index == 0 -> 指向第一条数据之前
        // m_row_index == 1 -> 指向第一条数据
        if (m_row_index == 0) {
            return !m_vertices.empty();
        } else {
            return m_row_index < m_vertices.size();
        }
    }

    bool VertexQueryResult::MoveNext() {
        bool movable = HasNext();
        if (movable) {
            m_row_index++;
        }
        return movable;
    }

    const ResultMetadata *VertexQueryResult::GetMetaDataByLabel(const std::string &label) const {
        const auto iter = m_metadatas.find(label);
        if (iter == m_metadatas.end()) {
            return nullptr;
        } else {
            return &(iter->second);
        }
    }

    const ResultMetadata *VertexQueryResult::GetMetaData(Status *status) const {
        const std::string lbl = GetLabel(status);
        if (status->ok()) {
            return GetMetaDataByLabel(lbl);
        }
        return nullptr;
    }

    bool VertexQueryResult::IsBeforeFirstOrAfterLast() const {
        return (m_row_index == 0 || m_row_index > m_vertices.size());
    }

    std::string VertexQueryResult::GetLabel(Status *status) const {
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
            return "";
        }
        auto iter = m_labels.find(m_vertices[m_row_index - 1].tag);
        if (iter == m_labels.end()) {
            *status = Status::InvalidArgument("not valid label");
            return "";
        } else {
            return iter->second;
        }
    }

    EdgeTag_t VertexQueryResult::GetTag(Status *status) const {
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
            return 0;
        }
        return m_vertices[m_row_index - 1].tag;
    }

    vid_t VertexQueryResult::GetVid(Status *status) const {
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        if (status->ok()) {
            return m_vertices[m_row_index - 1].m_vertex;
        } else {
            return 0;
        }
    }

    const std::string& VertexQueryResult::GetVertex(Status *status) const {
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        if (status->ok()) {
            return m_vertices[m_row_index - 1].m_s_vertex;
        } else {
            static std::string EMPTY;
            return EMPTY;
        }
    }

    bool VertexQueryResult::IsNull(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        // FIXME 检查 columnIndex 是否在正常范围内
        if (status->ok()) {
            return m_vertices[m_row_index - 1].m_properties.is_null(columnIndex);
        } else {
            return true;
        }
    }

    bool VertexQueryResult::IsNull(const char *columnName, Status *status) const {
        assert(status != nullptr);
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        const auto *metadata = GetMetaDataByLabel(GetLabel(status));
        if (!status->ok() || metadata == nullptr) {
            return true;
        }
        const size_t columnIndex = metadata->GetColumnIndexByName(columnName);
        if (columnIndex == ResultMetadata::INVALID_COLUMN_INDEX) {
            *status = Status::InvalidArgument(fmt::format("invalid columnName: `{}'", columnName));
        }
        if (status->ok()) {
            return m_vertices[m_row_index - 1].m_properties.is_null(columnIndex);
        } else {
            return true;
        }
    }

    int32_t VertexQueryResult::GetInt32(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        size_t offset = 0;
        if (status->ok()) {
            *status = GetOffsetByIndex(columnIndex, &offset);
        }
        if (status->ok()) {
            return m_vertices[m_row_index - 1].m_properties.get<int32_t>(offset);
        } else {
            return 0;
        }
    }

    int32_t VertexQueryResult::GetInt32(const char *columnName, Status *status) const {
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        size_t offset = 0;
        if (status->ok()) {
            *status = GetOffsetByName(columnName, &offset);
        }
        if (status->ok()) {
            return m_vertices[m_row_index - 1].m_properties.get<int32_t>(offset);
        } else {
            return 0;
        }
    }


    int64_t VertexQueryResult::GetInt64(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        size_t offset = 0;
        if (status->ok()) {
            *status = GetOffsetByIndex(columnIndex, &offset);
        }
        if (status->ok()) {
            return m_vertices[m_row_index - 1].m_properties.get<int64_t>(offset);
        } else {
            return 0;
        }
    }

    int64_t VertexQueryResult::GetInt64(const char *columnName, Status *status) const {
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        size_t offset = 0;
        if (status->ok()) {
            *status = GetOffsetByName(columnName, &offset);
        }
        if (status->ok()) {
            return m_vertices[m_row_index - 1].m_properties.get<int64_t>(offset);
        } else {
            return 0;
        }
    }

    float VertexQueryResult::GetFloat(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        size_t offset = 0;
        if (status->ok()) {
            *status = GetOffsetByIndex(columnIndex, &offset);
        }
        if (status->ok()) {
            return m_vertices[m_row_index - 1].m_properties.get<float>(offset);
        } else {
            return 0.0f;
        }
    }

    float VertexQueryResult::GetFloat(const char *columnName, Status *status) const {
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        size_t offset = 0;
        if (status->ok()) {
            *status = GetOffsetByName(columnName, &offset);
        }
        if (status->ok()) {
            return m_vertices[m_row_index - 1].m_properties.get<float>(offset);
        } else {
            return 0.0f;
        }
    }

    double VertexQueryResult::GetDouble(size_t columnIndex, Status *status) const {        
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        size_t offset = 0;
        if (status->ok()) {
            *status = GetOffsetByIndex(columnIndex, &offset);
        }
        if (status->ok()) {
            return m_vertices[m_row_index - 1].m_properties.get<double>(offset);
        } else {
            return 0.0;
        }
    }


    double VertexQueryResult::GetDouble(const char *columnName, Status *status) const {
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        size_t offset = 0;
        if (status->ok()) {
            *status = GetOffsetByName(columnName, &offset);
        }
        if (status->ok()) {
            return m_vertices[m_row_index - 1].m_properties.get<double>(offset);
        } else {
            return 0.0;
        }
    }


    std::string VertexQueryResult::GetBytes(size_t columnIndex, Status *status) const {        
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
            return "";
        }
        // 获取 Schema 的 metadata
        const auto *metadata = GetMetaDataByLabel(GetLabel(status));
        if (metadata == nullptr) {
            *status = Status::InvalidArgument("invalid label");
        } else {
            size_t offset = metadata->GetColumnDataOffsetByIndex(columnIndex);
            // colName 不存在
            if (offset == ResultMetadata::INVALID_COLUMN_INDEX) {
                *status = Status::InvalidArgument(fmt::format("invalid columnIndex: `{}'", columnIndex));
            }
            if (status->ok()) {
                ColumnDescriptor desc = metadata->m_columns[columnIndex];
                if (desc.columnType() == ColumnType::FIXED_BYTES) {
                    Slice s = m_vertices[m_row_index - 1].m_properties.get(offset, offset + desc.value_size(), false);
                    return s.ToString();
                } else {
                    *status = Status::InvalidArgument("invalid column type");
                }
            }
        }
        // invalid status
        return "";
    }


    std::string VertexQueryResult::GetBytes(const char *columnName, Status *status) const { 
        assert(status != nullptr);
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
            return "";
        }
        // 获取 Schema 的 metadata
        const auto *metadata = GetMetaDataByLabel(GetLabel(status));
        if (metadata == nullptr) {
            *status = Status::InvalidArgument("invalid label");
        } else {
            size_t columnIndex = metadata->GetColumnIndexByName(columnName);
            size_t offset = metadata->GetColumnDataOffsetByIndex(columnIndex);
            // colName 不存在
            if (offset == ResultMetadata::INVALID_COLUMN_INDEX) {
                *status = Status::InvalidArgument(fmt::format("invalid columnName: `{}'", columnName));
            }
            if (status->ok()) {
                ColumnDescriptor desc = metadata->m_columns[columnIndex];
                if (desc.columnType() == ColumnType::FIXED_BYTES) {
                    Slice s = m_vertices[m_row_index - 1].m_properties.get(offset, offset + desc.value_size(), false);
                    return s.ToString();
                } else {
                    *status = Status::InvalidArgument("invalid column type");
                }
            }
        }
        // invalid status
        return "";
    }
    

    std::string VertexQueryResult::GetString(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
            return "";
        }
        // null
        if (m_vertices[m_row_index - 1].m_properties.is_null(columnIndex)) {
            return "";
        }
        // 获取 Schema 的 metadata
        const auto *metadata = GetMetaDataByLabel(GetLabel(status));
        if (metadata == nullptr) {
            *status = Status::InvalidArgument("invalid label");
        } else {
            size_t offset = metadata->GetColumnDataOffsetByIndex(columnIndex);
            // colName 不存在
            if (offset == ResultMetadata::INVALID_COLUMN_INDEX) {
                *status = Status::InvalidArgument(fmt::format("invalid columnIndex: `{}'", columnIndex));
            }
            if (status->ok()) {
                ColumnDescriptor desc = metadata->m_columns[columnIndex];
                if (desc.columnType() == ColumnType::VARCHAR) {
                    return m_vertices[m_row_index - 1].m_properties.getVar(offset).ToString();
                } else if (desc.columnType() == ColumnType::FIXED_BYTES) {
                    Slice s = m_vertices[m_row_index - 1].m_properties.get(offset, offset + desc.value_size(), true);
                    return s.ToString();
                } else {
                    *status = Status::InvalidArgument("invalid column type");
                }
            }
        }
        // invalid status
        return "";
    }

    std::string VertexQueryResult::GetString(const char *columnName, Status *status) const {
        assert(status != nullptr);
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
            return "";
        }
        // 获取 Schema 的 metadata
        const auto *metadata = GetMetaDataByLabel(GetLabel(status));
        if (metadata == nullptr) {
            *status = Status::InvalidArgument("invalid label");
        } else {
            size_t columnIndex = metadata->GetColumnIndexByName(columnName);
            size_t offset = 0;
            if (columnIndex == ResultMetadata::INVALID_COLUMN_INDEX) {
                *status = Status::InvalidArgument("invalid columnName");
            } else {
                offset = metadata->GetColumnDataOffsetByIndex(columnIndex);
                // colName 不存在
                if (offset == ResultMetadata::INVALID_COLUMN_INDEX) {
                    *status = Status::InvalidArgument(fmt::format("invalid columnName: `{}'", columnName));
                }
            }
            if (status->ok()) {
                ColumnDescriptor desc = metadata->m_columns[columnIndex];
                if (desc.columnType() == ColumnType::VARCHAR) {
                    if (m_vertices[m_row_index - 1].m_properties.is_null(columnIndex)) {// null
                        return "";
                    } else {
                        return m_vertices[m_row_index - 1].m_properties.getVar(offset).ToString();
                    }
                } else if (desc.columnType() == ColumnType::FIXED_BYTES) {
                    Slice s = m_vertices[m_row_index - 1].m_properties.get(offset, offset + desc.value_size(), true);
                    return s.ToString();
                } else {
                    *status = Status::InvalidArgument("invalid column type");
                }
            }
        }
        // invalid status
        return "";
    }

    time_t VertexQueryResult::GetTimestamp(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        size_t offset = 0;
        if (status->ok()) {
            *status = GetOffsetByIndex(columnIndex, &offset);
        }
        if (status->ok()) {
            return m_vertices[m_row_index - 1].m_properties.get<time_t>(offset);
        } else {
            return 0;
        }
    }

    time_t VertexQueryResult::GetTimestamp(const char *columnName, Status *status) const {
        assert(status != nullptr);
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        size_t offset = 0;
        *status = GetOffsetByName(columnName, &offset);
        if (status->ok()) {
            return m_vertices[m_row_index - 1].m_properties.get<time_t>(offset);
        } else {
            return 0;
        }
    }

    std::string VertexQueryResult::GetTimeString(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        size_t offset = 0;
        if (status->ok()) {
            *status = GetOffsetByIndex(columnIndex, &offset);
        }
        if (status->ok()) {
            // TODO 允许自定义格式
            time_t t = m_vertices[m_row_index - 1].m_properties.get<time_t>(offset);
            return fmt::format("{:%Y-%m-%d %H:%M:%S}", *std::localtime(&t));
        } else {
            return "";
        }
    }

    std::string VertexQueryResult::GetTimeString(const char *columnName, Status *status) const {
        assert(status != nullptr);
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        size_t offset = 0;
        *status = GetOffsetByName(columnName, &offset);
        if (status->ok()) {
            // TODO 允许自定义格式
            time_t t = m_vertices[m_row_index - 1].m_properties.get<time_t>(offset);
            return fmt::format("{:%Y-%m-%d %H:%M:%S}", *std::localtime(&t));
        } else {
            return "";
        }
    }

    Status VertexQueryResult::GetOffsetByIndex(size_t columnIndex, size_t *offset) const {
        Status s;
        // 获取 Schema 的 metadata
        const auto *metadata = GetMetaDataByLabel(GetLabel(&s));
        if (metadata == nullptr) {
            return Status::InvalidArgument("invalid label");
        }
        *offset = metadata->GetColumnDataOffsetByIndex(columnIndex);
        // columnIndex 无效
        if (*offset == ResultMetadata::INVALID_COLUMN_INDEX) {
            return Status::InvalidArgument(fmt::format("invalid columnIndex: `{}'", columnIndex));
        }
        return Status::OK();
    }

    Status VertexQueryResult::GetOffsetByName(const char *colName, size_t *offset) const {
        Status s;
        // 获取 Schema 的 metadata
        const auto *metadata = GetMetaDataByLabel(GetLabel(&s));
        if (metadata == nullptr) {
            return Status::InvalidArgument("invalid label");
        }
        *offset = metadata->GetColumnDataOffsetByName(colName);
        // colName 不存在
        if (*offset == ResultMetadata::INVALID_COLUMN_INDEX) {
            return Status::InvalidArgument(fmt::format("invalid columnName: `{}'", colName));
        }
        return Status::OK();
    }

    Status VertexQueryResult::TranslateVertex(const std::shared_ptr<IDEncoder> &encoder) {
        // vid -> label, vertex
        Status s;
        std::string label;
        for (size_t i = 0; i < m_vertices.size(); ++i) {
            s = encoder->GetVertexByID(m_vertices[i].m_vertex, &label, &m_vertices[i].m_s_vertex);
            if (!s.ok()) { return s; }
        }
        return s;
    }

    std::string VertexQueryResult::ToJsonString() const {
        assert(!IsBeforeFirstOrAfterLast()); // check cursor is valid
        if (IsBeforeFirstOrAfterLast()) {// return empty string if cursor is invalid
            return "";
        }
        return ColumnDescriptorUtils::SerializeVertex(*this);
    }

}
