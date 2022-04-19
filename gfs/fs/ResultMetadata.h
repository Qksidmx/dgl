#ifndef STARKNOWLEDGEGRAPHDATABASE_RESULTMETADATA_H
#define STARKNOWLEDGEGRAPHDATABASE_RESULTMETADATA_H

#include <cstdlib>

#include "util/types.h"
#include "ColumnDescriptor.h"

namespace skg {

    class ResultMetadata {
    public:
        static const size_t INVALID_COLUMN_INDEX = static_cast<size_t>(-1);
    public:
        /**
         * @return 返回ResultSet的总列数
         */
        size_t GetColumnCount() const;

        /**
         * 获取指定列的名称
         * @param columnIndex
         * @return
         */
        const char *GetColumnName(size_t columnIndex) const;

        /**
         * 获取指定列的类型
         * @param columnIndex
         * @return
         */
        ColumnType GetColumnType(size_t columnIndex) const;

    protected:
        void SetColumns(const std::vector<ColumnDescriptor> &cols);

        size_t GetColumnDataOffsetByName(const char *colName) const;

        size_t GetColumnDataOffsetByIndex(size_t columnIndex) const;

        size_t GetColumnIndexByName(const char *colName) const;

        std::vector<ColumnDescriptor> m_columns;
    public:
        friend class EdgesQueryResult;
        friend class VertexQueryResult;
    };
}

#endif //STARKNOWLEDGEGRAPHDATABASE_RESULTMETADATA_H
