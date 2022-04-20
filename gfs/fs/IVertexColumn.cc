#include <sys/mman.h>
#include "util/types.h"
#include "util/status.h"
#include "IVertexColumn.h"

#include "util/skgfilenames.h"
#include "util/pathutils.h"
#include "IVertexColumnImpl.h"

namespace skg {

Status IVertexColumn::CreateWithCapacity(
        const std::string &storageDir,
        const std::string &label, const ColumnDescriptor &descriptor,
        const vid_t capacity) {
    if (descriptor.columnType() == ColumnType::VARCHAR) {
        //SKG_LOG_TRACE("Creating vertex var-char-column: `{}' of capacity {}", descriptor.colname(), capacity);
        //auto col = std::make_shared<VarCharVertexColumn>(storageDir, label, descriptor);
        SKG_LOG_ERROR("Creating vertex var-char-column: `{}' of capacity {}", descriptor.colname(), capacity);
	Status s=Status::NotImplement("No RocksDB!");
        return s;
    } else {
        const std::string fname = FILENAME::vertex_attr_data(storageDir, label, descriptor.colname());
        Status s = PathUtils::CreateFile(fname);
        if (!s.ok()) { return s; }
        SKG_LOG_TRACE("Creating vertex column: `{}' of capacity {}", descriptor.colname(), capacity);
        s = PathUtils::TruncateFile(fname, capacity * descriptor.value_size());
        if (!s.ok()) { return s; }
        return s;
    }
}

IVertexColumn::IVertexColumn(const ColumnDescriptor &desc)
        : m_name(desc.colname()), m_id(desc.id()) {
}


IVertexColumnPtr IVertexColumn::OpenColumn(
        const std::string &storageDir,
        const std::string &label, const ColumnDescriptor &descriptor,
        Status *s) {
    if (storageDir.empty()) {
        // error
        SKG_LOG_ERROR("Vertex-column {}, storage dir has not been initialize", descriptor.colname());
        *s = Status::InvalidArgument(fmt::format(
                "creating vertex-column `{}' without init storage dir",
                descriptor.colname()));
        return IVertexColumnPtr();
    }
    IVertexColumnPtr col;
    switch (descriptor.columnType()) {
        case ColumnType::INT32: {
            col = std::make_shared<Int32VertexColumn>(storageDir, label, descriptor);
            break;
        }
        case ColumnType::INT64: {
            col = std::make_shared<Int64VertexColumn>(storageDir, label, descriptor);
            break;
        }
        case ColumnType::FLOAT32: {
            col = std::make_shared<Float32VertexColumn>(storageDir, label, descriptor);
            break;
        }
        case ColumnType::FLOAT64: {
            col = std::make_shared<Float64VertexColumn>(storageDir, label, descriptor);
            break;
        }
        case ColumnType::FIXED_BYTES: {
            if (descriptor.value_size() != 0) {
                col = std::make_shared<FixedBytesVertexColumn>(
                        storageDir, label, descriptor);
            } else {
                *s = Status::InvalidArgument(fmt::format(
                        "can NOT create fix-bytes column `{}' without setting fixed-length",
                        descriptor.colname()));
            }
            break;
        }
        case ColumnType::TIME: {
            if (!descriptor.GetTimeFormat().empty()) {
                // TODO 支持用户自定义时间格式
                if (descriptor.GetTimeFormat() != "%Y-%m-%d %H:%M:%S") {
                    *s = Status::NotImplement(fmt::format(
                            "can NOT yet support time format: \"{}\"."
                            "only support \"%Y-%m-%d %H:%M:%S\" now",
                            descriptor.GetTimeFormat()));
                } else {
                    col = std::make_shared<TimeVertexColumn>(
                            storageDir, label, descriptor);
                }
            } else {
                *s = Status::InvalidArgument(fmt::format(
                        "can NOT create time column `{}' without setting time-format",
                        descriptor.colname()));
            }
            break;
        }
        case ColumnType::TAG: {
            col = std::make_shared<TagVertexColumn>(storageDir, label, descriptor);
            break;
        }
        case ColumnType::VARCHAR: {
            //col = std::make_shared<VarCharVertexColumn>(storageDir, label, descriptor);
            SKG_LOG_ERROR("Creating vertex var-char-column: `{}' of capacity {}", descriptor.colname());
            *s = Status::NotImplement("No RocksDB");
            break;
        }
        case ColumnType::WEIGHT:
        case ColumnType::NONE: {
            assert(false);
            break;
        }
        case ColumnType::GROUP: {
            *s = Status::NotImplement("vertex-column-group");
            break;
        }
    }
    if (!s->ok()) {
        assert(col == nullptr);
        return IVertexColumnPtr();
    }
    // 打开操作的文件句柄
    *s = col->Open();
    if (!s->ok()) { // 返回空指针
        return IVertexColumnPtr();
    }
    return col;
}

}
