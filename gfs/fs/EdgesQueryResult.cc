#include "EdgesQueryResult.h"

#include <vector>
#include <mutex>
#include <algorithm>
#include "MetaHeterogeneousAttributes.h"
#include "IRequest.h"

#include "fmt/format.h"
#include "fmt/time.h"
#include "util/types.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#if WITH_METRICS
#include "metrics/metrics.hpp"
#include "metrics/reps/basic_reporter.hpp"
#endif

namespace skg {

    EdgesQueryResult::EdgesQueryResult()
            : m_row_position(0),
              m_edges(), m_labels(), m_metadatas(),
              m_nlimit(IRequest::NO_LIMIT) {
    }

    EdgesQueryResult::~EdgesQueryResult() = default;

    size_t EdgesQueryResult::Size() {
        return m_edges.size();
    }

    Status EdgesQueryResult::ReceiveEdge(
            const vid_t src, const vid_t dst,
            const EdgeWeight_t weight, const EdgeTag_t tag,
            const char *column_bytes, const size_t column_bytes_len,
            const PropertiesBitset_t &bitset) {
#ifdef SKG_QUERY_USE_MT
        // 加锁, 防止多线程环境下, 多个线程同时写导致的出错
        // 使用 std::lock_guard 获取锁, 在析构时自动释放锁. http://zh.cppreference.com/w/cpp/thread/lock_guard
        std::lock_guard<std::mutex> lock(m_receive_lock);
#endif
        if (m_nlimit == IRequest::NO_LIMIT || m_nlimit < static_cast<ssize_t>(m_edges.size()) + 1) {
#ifndef SKG_SRC_SPLIT_SHARD
            m_edges.emplace_back(src, dst, weight, tag, column_bytes, column_bytes_len, bitset);
#else
            m_edges.emplace_back(dst, src, weight, tag, column_bytes, column_bytes_len, bitset);
#endif
            return Status::OK();
        } else {
            return Status::ResultSizeOverLimit(fmt::format("{}", m_nlimit));
        }
    }

    void EdgesQueryResult::Clear() {
        m_row_position = 0;
        m_edges.clear();
        m_labels.clear();
        m_metadatas.clear();
    }

    bool EdgesQueryResult::MoveNext() {
        bool isMovable = HasNext();
        if (isMovable) {
            m_row_position++;
        }
        return isMovable;
    }

    bool EdgesQueryResult::HasNext() const {
        if (m_row_position == 0) {
            return !m_edges.empty();
        } else {
            return m_row_position < m_edges.size();
        }
    }

    EdgeLabel EdgesQueryResult::GetEdgeLabel(Status *status) const {
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        if (status->ok() && m_labels.empty()) {
            *status = Status::InvalidArgument("result metadata not set");
        }
        const auto iter = m_labels.find(m_edges[m_row_position - 1].tag);
        if (status->ok() && iter != m_labels.end()) {
            return iter->second;
        } else {
            *status = Status::InvalidArgument("not valid label");
            return EdgeLabel("", "", "");
        }
    }

    std::string EdgesQueryResult::GetLabel(Status *status) const {
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        if (status->ok() && m_labels.empty()) {
            *status = Status::InvalidArgument("result metadata not set");
        }
        const auto iter = m_labels.find(m_edges[m_row_position - 1].tag);
        if (status->ok() && iter != m_labels.end()) {
            return iter->second.edge_label;
        } else {
            *status = Status::InvalidArgument("not valid label");
            return "";
        }
    }

    std::string EdgesQueryResult::GetSrcVertexLabel(Status *status) const {
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        if (status->ok() && m_labels.empty()) {
            *status = Status::InvalidArgument("result metadata not set");
        }
        const auto iter = m_labels.find(m_edges[m_row_position - 1].tag);
        if (status->ok() && iter != m_labels.end()) {
            return iter->second.src_label;
        } else {
            *status = Status::InvalidArgument("not valid label");
            return "";
        }
    }

    std::string EdgesQueryResult::GetDstVertexLabel(Status *status) const {
        assert(status != nullptr);
        *status = Status::OK();
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        if (status->ok() && m_labels.empty()) {
            *status = Status::InvalidArgument("result metadata not set");
        }
        const auto iter = m_labels.find(m_edges[m_row_position - 1].tag);
        if (status->ok() && iter != m_labels.end()) {
            return iter->second.dst_label;
        } else {
            *status = Status::InvalidArgument("not valid label");
            return "";
        }
    }

    const ResultMetadata *EdgesQueryResult::GetMetaDataByLabel(const std::string &label) const {
        const auto iter = m_metadatas.find(label);
        if (iter == m_metadatas.end()) {
            return nullptr;
        } else {
            return &(iter->second);
        }
    }

    const ResultMetadata *EdgesQueryResult::GetMetaData(Status *status) const {
        const std::string lbl = GetLabel(status);
        if (status->ok()) {
            return GetMetaDataByLabel(lbl);
        }
        return nullptr;
    }

    EdgeWeight_t EdgesQueryResult::GetWeight(Status *status) const {
        assert(!IsBeforeFirstOrAfterLast());
        // 检查游标是否在有效范围内
        if (IsBeforeFirstOrAfterLast()) {
            // 返回错误
            *status = Status::InvalidArgument("not on result set");
            return 0;
        }
        *status = Status::OK();
        return m_edges[m_row_position - 1].weight;
    }

    bool EdgesQueryResult::IsNull(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        if (IsBeforeFirstOrAfterLast()) {
            *status = Status::InvalidArgument("not on result set");
        }
        // FIXME 检查 columnIndex 是否在正常范围内
        if (status->ok()) {
            return !m_edges[m_row_position - 1].IsPropertySet(columnIndex);
        } else {
            return true;
        }
    }

    bool EdgesQueryResult::IsNull(const char *columnName, Status *status) const {
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
            return !m_edges[m_row_position - 1].IsPropertySet(columnIndex);
        } else {
            return true;
        }
    }

    int32_t EdgesQueryResult::GetInt32(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        size_t offset = 0;
        do {
            // 检查游标是否在有效范围内
            if (IsBeforeFirstOrAfterLast()) {
                // 返回错误
                *status = Status::InvalidArgument("not on result set");
                break;
            }
            *status = GetOffsetByIndex(columnIndex, &offset);
        } while (false);
        if (status->ok()) {
            return *reinterpret_cast<const int32_t *>(m_edges[m_row_position - 1].column_bytes() + offset);
        } else {
            return 0;
        }
    }

    int32_t EdgesQueryResult::GetInt32(const char *columnName, Status *status) const {
        assert(status != nullptr);
        size_t offset = 0;
        do {
            // 检查游标是否在有效范围内
            if (IsBeforeFirstOrAfterLast()) {
                // 返回错误
                *status = Status::InvalidArgument("not on result set");
                break;
            }
            *status = GetOffsetByName(columnName, &offset);
        } while (false);
        if (status->ok()) {
            return *reinterpret_cast<const int32_t *>(m_edges[m_row_position - 1].column_bytes() + offset);
        } else {
            return 0;
        }
    }

    float EdgesQueryResult::GetFloat(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        size_t offset = 0;
        do {
            // 检查游标是否在有效范围内
            if (IsBeforeFirstOrAfterLast()) {
                // 返回错误
                *status = Status::InvalidArgument("not on result set");
                break;
            }
            *status = GetOffsetByIndex(columnIndex, &offset);
        } while (false);
        if (status->ok()) {
            return *reinterpret_cast<const float *>(m_edges[m_row_position - 1].column_bytes() + offset);
        } else {
            return 0.0f;
        }
    }

    float EdgesQueryResult::GetFloat(const char *columnName, Status *status) const {
        assert(status != nullptr);
        size_t offset = 0;
        do {
            // 检查游标是否在有效范围内
            if (IsBeforeFirstOrAfterLast()) {
                // 返回错误
                *status = Status::InvalidArgument("not on result set");
                break;
            }
            *status = GetOffsetByName(columnName, &offset);
        } while (false);
        if (status->ok()) {
            return *reinterpret_cast<const float *>(m_edges[m_row_position - 1].column_bytes() + offset);
        } else {
            return 0.0f;
        }
    }

    std::string EdgesQueryResult::GetString(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        // 检查游标是否在有效范围内
        if (IsBeforeFirstOrAfterLast()) {
            // 返回错误
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
                    Slice s(m_edges[m_row_position - 1].column_bytes() + offset, desc.value_size());
                    // 去除后置的 '\0'
                    while (!s.empty() && s[s.size() - 1] == '\x00') { s.remove_suffix(1); }
                    return s.ToString();
                } else {
                    *status = Status::InvalidArgument("invalid column type");
                }
            }
        }
        // invalid status
        return "";
    }

    std::string EdgesQueryResult::GetString(const char *columnName, Status *status) const {
        assert(status != nullptr);
        // 检查游标是否在有效范围内
        if (IsBeforeFirstOrAfterLast()) {
            // 返回错误
            *status = Status::InvalidArgument("not on result set");
            return "";
        }
        // 获取 Schema 的 metadata
        const auto *metadata = GetMetaDataByLabel(GetLabel(status));
        if (metadata == nullptr) {
            *status = Status::InvalidArgument("invalid label");
        } else {
            size_t columnIndex = metadata->GetColumnIndexByName(columnName);
            size_t offset = metadata->GetColumnDataOffsetByName(columnName);
            // colName 不存在
            if (offset == ResultMetadata::INVALID_COLUMN_INDEX) {
                *status = Status::InvalidArgument(fmt::format("invalid columnName: `{}'", columnName));
            }
            if (status->ok()) {
                ColumnDescriptor desc = metadata->m_columns[columnIndex];
                if (desc.columnType() == ColumnType::FIXED_BYTES) {
                    Slice s(m_edges[m_row_position - 1].column_bytes() + offset, desc.value_size());
                    // 去除后置的 '\0'
                    while (!s.empty() && s[s.size() - 1] == '\x00') { s.remove_suffix(1); }
                    return s.ToString();
                } else {
                    *status = Status::InvalidArgument("invalid column type");
                }
            }
        }
        // invalid status
        return "";
    }

    double EdgesQueryResult::GetDouble(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        size_t offset = 0;
        do {
            // 检查游标是否在有效范围内
            if (IsBeforeFirstOrAfterLast()) {
                // 返回错误
                *status = Status::InvalidArgument("not on result set");
                break;
            }
            *status = GetOffsetByIndex(columnIndex, &offset);
        } while (false);
        if (status->ok()) {
            return *reinterpret_cast<const double *>(m_edges[m_row_position - 1].column_bytes() + offset);
        } else {
            return 0.0;
        }
    }

    double EdgesQueryResult::GetDouble(const char *columnName, Status *status) const {
        assert(status != nullptr);
        size_t offset = 0;
        do {
            // 检查游标是否在有效范围内
            if (IsBeforeFirstOrAfterLast()) {
                // 返回错误
                *status = Status::InvalidArgument("not on result set");
                break;
            }
            *status = GetOffsetByName(columnName, &offset);
        } while (false);
        if (status->ok()) {
            return *reinterpret_cast<const double *>(m_edges[m_row_position - 1].column_bytes() + offset);
        } else {
            return 0.0;
        }
    }

    int64_t EdgesQueryResult::GetInt64(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        size_t offset = 0;
        do {
            // 检查游标是否在有效范围内
            if (IsBeforeFirstOrAfterLast()) {
                // 返回错误
                *status = Status::InvalidArgument("not on result set");
                break;
            }
            *status = GetOffsetByIndex(columnIndex, &offset);
        } while (false);
        if (status->ok()) {
            return *reinterpret_cast<const int64_t *>(m_edges[m_row_position - 1].column_bytes() + offset);
        } else {
            return 0;
        }
    }

    int64_t EdgesQueryResult::GetInt64(const char *columnName, Status *status) const {
        assert(status != nullptr);
        size_t offset = 0;
        do {
            // 检查游标是否在有效范围内
            if (IsBeforeFirstOrAfterLast()) {
                // 返回错误
                *status = Status::InvalidArgument("not on result set");
                break;
            }
            *status = GetOffsetByName(columnName, &offset);
        } while (false);
        if (status->ok()) {
            return *reinterpret_cast<const int64_t *>(m_edges[m_row_position - 1].column_bytes() + offset);
        } else {
            return 0;
        }
    }

    time_t EdgesQueryResult::GetTimestamp(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        size_t offset = 0;
        do {
            // 检查游标是否在有效范围内
            if (IsBeforeFirstOrAfterLast()) {
                // 返回错误
                *status = Status::InvalidArgument("not on result set");
                break;
            }
            *status = GetOffsetByIndex(columnIndex, &offset);
        } while (false);
        if (status->ok()) {
            return *reinterpret_cast<const time_t *>(m_edges[m_row_position - 1].column_bytes() + offset);
        } else {
            return 0;
        }
    }

    time_t EdgesQueryResult::GetTimestamp(const char *columnName, Status *status) const {
        assert(status != nullptr);
        size_t offset = 0;
        do {
            // 检查游标是否在有效范围内
            if (IsBeforeFirstOrAfterLast()) {
                // 返回错误
                *status = Status::InvalidArgument("not on result set");
                break;
            }
            *status = GetOffsetByName(columnName, &offset);
        } while (false);
        if (status->ok()) {
            return *reinterpret_cast<const time_t *>(m_edges[m_row_position - 1].column_bytes() + offset);
        } else {
            return 0;
        }
    }

    std::string EdgesQueryResult::GetTimeString(size_t columnIndex, Status *status) const {
        assert(status != nullptr);
        size_t offset = 0;
        do {
            // 检查游标是否在有效范围内
            if (IsBeforeFirstOrAfterLast()) {
                // 返回错误
                *status = Status::InvalidArgument("not on result set");
                break;
            }
            *status = GetOffsetByIndex(columnIndex, &offset);
        } while (false);
        if (status->ok()) {
            // TODO 允许自定义格式
            return fmt::format("{:%Y-%m-%d %H:%M:%S}", *std::localtime(reinterpret_cast<const time_t*>(m_edges[m_row_position - 1].column_bytes() + offset)));
        } else {
            return "";
        }
    }

    std::string EdgesQueryResult::GetTimeString(const char *columnName, Status *status) const {
        assert(status != nullptr);
        size_t offset = 0;
        do {
            // 检查游标是否在有效范围内
            if (IsBeforeFirstOrAfterLast()) {
                // 返回错误
                *status = Status::InvalidArgument("not on result set");
                break;
            }
            *status = GetOffsetByName(columnName, &offset);
        } while (false);
        if (status->ok()) {
            // TODO 允许自定义格式
            return fmt::format("{:%Y-%m-%d %H:%M:%S}", *std::localtime(reinterpret_cast<const time_t*>(m_edges[m_row_position - 1].column_bytes() + offset)));
        } else {
            return "";
        }
    }

    Status EdgesQueryResult::TranslateEdgeVertex(const std::shared_ptr<skg::IDEncoder> &encoder) {
        Status s;
        std::string label;
        std::string sSrc, sDst;
        for (size_t i = 0; i < m_edges.size(); ++i) {
            // 查询的返回结果, 转换为字符串 FIXME 可减少查询的次数
            s = encoder->GetVertexByID(m_edges[i].src, &label, &sSrc);
            if (!s.ok()) { return s; }
            s = encoder->GetVertexByID(m_edges[i].dst, &label, &sDst);
            if (!s.ok()) { return s; }
            m_edges[i].set_vertex(sSrc, sDst);
        }
        return s;
    }

    bool EdgesQueryResult::IsBeforeFirstOrAfterLast() const {
        return (m_row_position == 0 || m_row_position > m_edges.size());
    }

    Status EdgesQueryResult::GetOffsetByIndex(size_t columnIndex, size_t *offset) const {
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

    Status EdgesQueryResult::GetOffsetByName(const char *colName, size_t *offset) const {
        Status s;
        // 获取 Schema 的 metadata
        const auto *metadata = GetMetaDataByLabel(GetLabel(&s));
        if (!s.ok()) { return s; }
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

    Status EdgesQueryResult::SetResultMetadata(const MetaHeterogeneousAttributes &hetAttributes) {
        Status s;
        for (const auto &attributes : hetAttributes) {
            m_labels.insert(std::make_pair(
                    attributes.label_tag,
                    EdgeLabel(attributes.label, attributes.src_label, attributes.dst_label)
            ));
            // TODO ResultMetadata 中直接封装 MetaAttributes
            ResultMetadata metadata;
            std::vector<ColumnDescriptor> desc;
            for (const auto &descriptor: attributes) {
                if (descriptor.columnType() == ColumnType::GROUP) {
                    for (const auto &subCol : descriptor) {
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

    vid_t EdgesQueryResult::GetSrcVid(skg::Status *status) const {
        assert(status != nullptr);
        // 检查游标是否在有效范围内
        if (IsBeforeFirstOrAfterLast()) {
            // 返回错误
            *status = Status::InvalidArgument("not on result set");
            return 0;
        }
        *status = Status::OK();
        return m_edges[m_row_position - 1].src;
    }

    std::string EdgesQueryResult::GetSrcVertex(skg::Status *status) const {
        assert(status != nullptr);
        // 检查游标是否在有效范围内
        if (IsBeforeFirstOrAfterLast()) {
            // 返回错误
            *status = Status::InvalidArgument("not on result set");
            return "";
        }
        *status = Status::OK();
        return m_edges[m_row_position - 1].s_src();
    }

    vid_t EdgesQueryResult::GetDstVid(skg::Status *status) const {
        assert(status != nullptr);
        // 检查游标是否在有效范围内
        if (IsBeforeFirstOrAfterLast()) {
            // 返回错误
            *status = Status::InvalidArgument("not on result set");
            return 0;
        }
        *status = Status::OK();
        return m_edges[m_row_position - 1].dst;
    }

    std::string EdgesQueryResult::GetDstVertex(skg::Status *status) const {
        assert(status != nullptr);
        // 检查游标是否在有效范围内
        if (IsBeforeFirstOrAfterLast()) {
            // 返回错误
            *status = Status::InvalidArgument("not on result set");
            return "";
        }
        *status = Status::OK();
        return m_edges[m_row_position - 1].s_dst();
    }

    std::string EdgesQueryResult::ToJsonString() const {
        assert(!IsBeforeFirstOrAfterLast()); // check cursor is valid
        if (IsBeforeFirstOrAfterLast()) {// return empty string if cursor is invalid
            return "";
        }
        return ColumnDescriptorUtils::SerializeEdge(*this);
    }

}
