#include "util/types.h"
#include "util/status.h"
#include "ColumnDescriptor.h"

#include <string>

#include "ColumnDescriptorUtils.h"
#include "IVertexColumn.h"
#include "IVertexColumnImpl.h"

namespace skg {

    ColumnDescriptor::ColumnDescriptor(const std::string &colname, ColumnType edgeColType_)
            : m_colname(colname), m_offsetAndType(0), m_id(-1), m_fixedLength(0) {
        SetType(edgeColType_);
        SetOffset(INVALID_OFFSET);
    }

    ColumnDescriptor::ColumnDescriptor()
            : ColumnDescriptor("", ColumnType::NONE) {
        SetOffset(INVALID_OFFSET);
    }

    ColumnDescriptor &ColumnDescriptor::SetName(const std::string &name) {
        m_colname = name;
        return *this;
    }

    ColumnDescriptor &ColumnDescriptor::SetType(ColumnType type) {
        m_offsetAndType = (m_offsetAndType & 0xffffff00) | (static_cast<uint32_t>(type) & 0xff);
        return *this;
    }

    ColumnDescriptor &ColumnDescriptor::SetOffset(uint32_t offset) {
        m_offsetAndType = ((offset & 0x00ffffff) << 8) | (m_offsetAndType & 0xff);
        return *this;
    }

    ColumnDescriptor &ColumnDescriptor::SetFixedLength(size_t size) {
        m_fixedLength = size;
        return *this;
    }

    ColumnDescriptor &ColumnDescriptor::SetTimeFormat(const std::string &timefmt) {
        m_timefmt = timefmt;
        return *this;
    }

    ColumnDescriptor &ColumnDescriptor::AddSubEdgeColumn(const ColumnDescriptor &subconfig) {
        assert(subconfig.columnType() != ColumnType::TAG);
        assert(subconfig.columnType() != ColumnType::WEIGHT);
        assert(subconfig.columnType() != ColumnType::GROUP);

        // TODO 检查是否已经存在相同名字的列, 若已经存在, 返回错误
        for (const auto &iter: m_subConfig) {
            if (iter.colname() == subconfig.colname()) {
                SKG_LOG_ERROR("Trying to add an existed column: {} in column group: {}", subconfig.colname(), m_colname);
                assert(false);
                return *this;
            }
        }
        if (subconfig.columnType() == ColumnType::GROUP) {
            SKG_LOG_ERROR("Nesting edge attribute. Trying to add column group {} inside column group {}",
                          subconfig.m_colname, m_colname);
            assert(false);
            return *this;
        }
        m_subConfig.emplace_back(subconfig);
        return *this;
    }

    ColumnType ColumnDescriptor::columnType() const {
        return static_cast<ColumnType>(m_offsetAndType & 0xff);
    }

    const std::string &ColumnDescriptor::colname() const {
        return m_colname;
    }

    uint32_t ColumnDescriptor::offset() const {
        return (m_offsetAndType >> 8) & 0xffffff;
    }

    size_t ColumnDescriptor::value_size() const {
        size_t numBytes = 0;
        switch (columnType()) {
            case ColumnType::TAG:
                return sizeof(EdgeTag_t);
            case ColumnType::WEIGHT:
                return sizeof(EdgeWeight_t);
            case ColumnType::INT32:
                return sizeof(int32_t);
            case ColumnType::FLOAT:
                return sizeof(float);
            case ColumnType::FIXED_BYTES:
                return m_fixedLength; // 固定长度的字符串
            case ColumnType::DOUBLE:
                return sizeof(double);
            case ColumnType::INT64:
                return sizeof(int64_t);
            case ColumnType::TIME:
                return sizeof(time_t);
            case ColumnType::GROUP:
                for (const auto &sub : m_subConfig) {
                    numBytes += sub.value_size();
                }
                return numBytes;
            case ColumnType::VARCHAR:
                return sizeof(uint32_t);
            case ColumnType::NONE:
                //assert(false);
                break;
        }
        //fixme
        //assert(false);
        return 0;
    }

    bool ColumnDescriptor::isNonEmptyColumnGroup() const {
        return (columnType() == ColumnType::GROUP && !m_subConfig.empty());
    }

    ColumnDescriptor::iterator ColumnDescriptor::begin() {
        assert(columnType() == ColumnType::GROUP);
        return m_subConfig.begin();
    }

    ColumnDescriptor::const_iterator ColumnDescriptor::begin() const {
        assert(columnType() == ColumnType::GROUP);
        return m_subConfig.begin();
    }

    ColumnDescriptor::iterator ColumnDescriptor::end() {
        assert(columnType() == ColumnType::GROUP);
        return m_subConfig.end();
    }

    ColumnDescriptor::const_iterator ColumnDescriptor::end() const {
        assert(columnType() == ColumnType::GROUP);
        return m_subConfig.end();
    }

    const std::string &ColumnDescriptor::GetTimeFormat() const {
        if (columnType() != ColumnType::TIME) {
            return m_timefmt;
        } else {
            if (m_timefmt.empty()) {
                static const std::string default_time_format("%Y-%m-%d %H:%M:%S");
                return default_time_format;
            } else {
                return m_timefmt;
            }
        }
    }

}
