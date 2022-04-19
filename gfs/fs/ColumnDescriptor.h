#ifndef STARKNOWLEDGEGRAPHDATABASE_EDGECOLUMNCONFIG_H
#define STARKNOWLEDGEGRAPHDATABASE_EDGECOLUMNCONFIG_H


#include <string>

#include "util/types.h"

namespace skg {
    class ColumnDescriptor {
    public:
        static const uint32_t INVALID_OFFSET = (static_cast<uint32_t>(-1) & 0x00FFFFFF);
        static const int32_t ID_INVALID = -1;
        static const int32_t ID_VERTICES_TAG = -2;
        static const int32_t ID_VERTICES_BITSET = -3;
    public:
        ColumnDescriptor();

        ColumnDescriptor(const std::string &colname, ColumnType edgeColType);

        /**
         * @brief 列名字
         * @param name
         * @return
         */
        ColumnDescriptor &SetName(const std::string &name);

        /**
         * @brief 列类型. (int32, float)
         * @param type
         * @return
         */
        ColumnDescriptor &SetType(ColumnType type);

        /**
         * @brief ColumnType 为 ColumnType::FIXED_BYTES 时, 最大存储长度.
         * @param size 最大存储长度
         * @return
         */
        ColumnDescriptor &SetFixedLength(size_t size);

        /**
         * @brief ColumnType 为 ColumnType::TIME 时, 时间字符串的解析格式.
         * @param format 时间格式. 默认为 "%Y-%m-%d %H:%M:%S"
         * @return
         */
        ColumnDescriptor &SetTimeFormat(const std::string &format);

        ColumnType columnType() const;

        /**
         * @brief ColumnType 为 FIXED_BYTES 时, 返回固定的字节长度. 其他类型返回类型的字节长度
         * @return
         */
        size_t value_size() const;

        const std::string &colname() const;
        

        // 下面的方法不对外暴露
        ColumnDescriptor &AddSubEdgeColumn(const ColumnDescriptor &subConfig);

        ColumnDescriptor &SetOffset(uint32_t offset);

        uint32_t offset() const;

        ColumnDescriptor &SetColumnID(int32_t id) {
            m_id = id;
            return *this;
        }

        int32_t id() const {
            assert(m_id != -1);
            return m_id;
        }

        friend 
        bool operator==(const ColumnDescriptor &l, const ColumnDescriptor &r) {
            return l.m_colname==r.m_colname &&
		    l. m_offsetAndType == r.m_offsetAndType && 
		    l.m_id==r.m_id &&
		    l.m_fixedLength==r.m_fixedLength &&
		    l.m_timefmt==r.m_timefmt &&
		    l.m_subConfig==r.m_subConfig;
        }

        // ===== Start. ColumnGroup 使用的成员函数 ===== //
        friend class EdgeColumnGroupMMappedFilePartition;
        bool isNonEmptyColumnGroup() const;

        // 迭代器, 用于遍历 ColumnGroup 中子列
        using iterator = std::vector<ColumnDescriptor>::iterator;
        using const_iterator = std::vector<ColumnDescriptor>::const_iterator;

        iterator begin();
        const_iterator begin() const;
        iterator end();
        const_iterator end() const;
        // ===== End. ColumnGroup 使用的成员函数 ===== //

        const std::string &GetTimeFormat() const;

        friend class RequestUtilities;
    private:
        // 列名字
        std::string m_colname;

        // 存储 offset && type
        // lower 8-bits 存储 type
        // higer 24-bits 存储 offset
        uint32_t m_offsetAndType;

        int32_t m_id;

        // 当 colType 是固定长度的 bytes 串时, 最长的上限
        size_t m_fixedLength;

        // 当 colType 是 TIME 时, 字符串的解析格式
        std::string m_timefmt;

        // Column Group, 多列组合为一列存储. 列存储的一种优化方式
        // http://source.wiredtiger.com/2.3.1/schema.html#schema_column_types
        std::vector<ColumnDescriptor> m_subConfig;

    };

}
#endif //STARKNOWLEDGEGRAPHDATABASE_EDGECOLUMNCONFIG_H
