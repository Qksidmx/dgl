#include <cstring>

#include "util/internal_types.h"
#include "ResultMetadata.h"

namespace skg {

    /**
     * @return 返回ResultSet的总列数
     */
    size_t ResultMetadata::GetColumnCount() const {
        return m_columns.size();
    }

    /**
     * 获取指定列的名称
     * @param columnIndex
     * @return
     */
    const char *ResultMetadata::GetColumnName(size_t columnIndex) const {
        assert(columnIndex < m_columns.size());
        return m_columns[columnIndex].colname().c_str();
    }

    /**
     * 获取指定列的类型
     * @param columnIndex
     * @return
     */
    ColumnType ResultMetadata::GetColumnType(size_t columnIndex) const {
        assert(columnIndex < m_columns.size());
        return m_columns[columnIndex].columnType();
    }

    void ResultMetadata::SetColumns(const std::vector<ColumnDescriptor> &cols) {
        m_columns = cols;
    }

    size_t ResultMetadata::GetColumnDataOffsetByName(const char *colName) const {
        if (colName == nullptr) {
            return INVALID_COLUMN_INDEX;
        }
        size_t offset = 0;
        for (size_t i = 0; i < m_columns.size(); ++i) {
            if (m_columns[i].columnType() == ColumnType::TAG
                || m_columns[i].columnType() == ColumnType::WEIGHT) {
                continue;
            }
            if (strcmp(m_columns[i].colname().c_str(), colName) == 0) {
                return offset;
            }
            offset += m_columns[i].value_size();
        }
        return INVALID_COLUMN_INDEX;
    }

    size_t ResultMetadata::GetColumnDataOffsetByIndex(size_t columnIndex) const {
        assert(columnIndex < m_columns.size());
        if (columnIndex > m_columns.size()) {
            return INVALID_COLUMN_INDEX;
        }
        // 计算第 columnIndex 的偏移量. aka 前面几列数据value_size的和
        size_t offset = 0;
        for (size_t i = 0; i < columnIndex; ++i) {
            if (m_columns[i].columnType() == ColumnType::TAG
                || m_columns[i].columnType() == ColumnType::WEIGHT) {
                continue;
            }
            offset += m_columns[i].value_size();
        }
        return offset;
    }

    size_t ResultMetadata::GetColumnIndexByName(const char *colName) const {
        if (colName == nullptr) {
            return INVALID_COLUMN_INDEX;
        }
        for (size_t i = 0; i < m_columns.size(); ++i) {
            if (strcmp(m_columns[i].colname().c_str(), colName) == 0) {
                return i;
            }
        }
        return INVALID_COLUMN_INDEX;
    }
}
