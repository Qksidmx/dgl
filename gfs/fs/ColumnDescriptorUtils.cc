#include "EdgesQueryResult.h"
#include "ColumnDescriptorUtils.h"

namespace skg {

Status ColumnDescriptorUtils::ParseWeight(const char *strValue, void *val) {
    *reinterpret_cast<float *>(val) = strtof(strValue, nullptr);
    return Status::OK();
}

    Status ColumnDescriptorUtils::ParseValueBytes(const ColumnDescriptor &descriptor, const char *strValue, void *val) {
        size_t len = 0;
        std::tm tm_time;
        memset(&tm_time, 0, sizeof(std::tm));
        assert(val != nullptr);
        Status s;
        switch (descriptor.columnType()) {

            case ColumnType::INT32:
                *reinterpret_cast<int32_t *>(val) = StringUtils::ParseUint32(strValue, &s);
                break;

            case ColumnType::INT64:
                *reinterpret_cast<int64_t *>(val) = StringUtils::ParseUint64(strValue);
                break;

            case ColumnType::WEIGHT:
            case ColumnType::FLOAT:
                *reinterpret_cast<float *>(val) = strtof(strValue, nullptr);
                break;

            case ColumnType::DOUBLE:
                *reinterpret_cast<double*>(val) = strtod(strValue, nullptr);
                break;

            case ColumnType::FIXED_BYTES:
                len = std::min(descriptor.value_size(), strnlen(strValue, descriptor.value_size()) + 1);
                memcpy(val, strValue, len);
                break;

            case ColumnType::TAG:
                *reinterpret_cast<int8_t *>(val) = static_cast<uint8_t>(strtol(strValue, nullptr, 10));
                break;
            case ColumnType::TIME:
                if (strptime(strValue, descriptor.GetTimeFormat().c_str(), &tm_time) == nullptr) {
                    return Status::InvalidArgument(fmt::format(
                            "Can NOT parse {} with format {}", strValue, descriptor.GetTimeFormat().c_str()));
                }
                *reinterpret_cast<time_t *>(val) = mktime(&tm_time);
                break;
            case ColumnType::VARCHAR:
                memcpy(val, strValue, strlen(strValue)+1);
                break;
            case ColumnType::NONE:
            case ColumnType::GROUP:
                assert(false);
                return Status::InvalidArgument();
        }
        return s;
    }

    Status ColumnDescriptorUtils::CreateWriter(
            const ColumnDescriptor &descriptor, const std::string &edgelistFile,
            std::unique_ptr<IEdgeColumnPartitionWriter> *writer) {
        assert(writer != nullptr);
        assert(*writer == nullptr);
        IEdgeColumnPartitionWriter *impl = nullptr;
        switch (descriptor.columnType()) {
            case ColumnType::INT32:
            case ColumnType::FLOAT:
            case ColumnType::FIXED_BYTES:
            case ColumnType::DOUBLE:
            case ColumnType::INT64:
            case ColumnType::TIME:
            case ColumnType::GROUP:
#ifndef SKG_EDGE_DATA_COLUMN_STOAGE
                auto *impl = new EdgeColumnBlocksPartitionWriter(shardfile, name(), m_bytes_length);
#else
                impl = new EdgeColumnFragmentFileWriter(edgelistFile, descriptor);
#endif
                break;

            case ColumnType::TAG:
            case ColumnType::WEIGHT:
            case ColumnType::NONE: {
                assert(false);
                return Status::InvalidArgument(
                        fmt::format("create writer of type: {}",
                                    static_cast<uint8_t>(descriptor.columnType())));
            }
        }
        Status s = impl->Open();
        if (s.ok()) {
            *writer = std::move(std::unique_ptr<IEdgeColumnPartitionWriter>(impl));
        } else {
            delete impl;
        }
        return s;
    }

    Status ColumnDescriptorUtils::CreatePartition(
            const ColumnDescriptor &descriptor,
            const std::string &dirname,
            uint32_t shard_id, uint32_t partition_id,
            const interval_t &interval,
            EdgeTag_t tag,
            IEdgeColumnPartitionPtr *fragment) {
        assert(fragment != nullptr);
        assert(*fragment == nullptr);
        Status s;
        IEdgeColumnPartition *impl = nullptr;
        switch (descriptor.columnType()) {
            case ColumnType::TAG:
            case ColumnType::WEIGHT:
                assert(false);
                return Status::InvalidArgument("Try to create column fragment reader, which is embed with edges");

            case ColumnType::GROUP:
#ifndef SKG_EDGE_DATA_COLUMN_STOAGE
                auto *impl = new EdgeColumnBlocksPartition(m_bytes_length);
#else
//            auto *impl = new EdgeColumnFilePartition(m_bytes_length);
                impl = new EdgeColumnGroupMMappedFilePartition(descriptor);
#endif
                break;

            case ColumnType::INT32:
            case ColumnType::FLOAT:
            case ColumnType::FIXED_BYTES:
            case ColumnType::DOUBLE:
            case ColumnType::INT64:
            case ColumnType::TIME:
#ifndef SKG_EDGE_DATA_COLUMN_STOAGE
                auto *impl = new EdgeColumnBlocksPartition(m_bytes_length);
#else
//                impl = new EdgeColumnFilePartition(descriptor.value_size());
                impl = new EdgeColumnMMappedFilePartition(descriptor);
#endif
                break;

            case ColumnType::NONE:
                assert(false);
                return Status::InvalidArgument("creating edge-column-partition of type `NONE'");
        }
        s = impl->Create(dirname, shard_id, partition_id, interval, tag);
        if (s.ok()) {
            *fragment = std::move(IEdgeColumnPartitionPtr(impl));
        } else {
            delete impl;
        }
        return s;
    }

Status
ColumnDescriptorUtils::UnSerializeFromRapidJsonValue(const rapidjson::Value &col, ColumnDescriptor *config) {
    Status s;
    if (!col.IsObject() || !col.HasMember("name") || !col.HasMember("type") || !col.HasMember("size")) {
        s = Status::InvalidArgument("not valid col");
        return s;
    }
    const ColumnType type = static_cast<ColumnType>(col["type"].GetUint());
    config->SetName(col["name"].GetString())
            .SetType(type)
            .SetFixedLength(col["size"].GetUint64());
    switch (type) {
        case ColumnType::GROUP:
            if (!col.HasMember("subCols") || !col["subCols"].IsArray()) {
                s = Status::InvalidArgument("not valid group col");
            } else {
                s = UnSerializeColumnGroupFromRapidJsonValue(col["subCols"], config);
            }
            break;
        case ColumnType::TIME:
            if (!col.HasMember("format")) {
                s = Status::InvalidArgument("not valid time col");
            } else {
                config->SetTimeFormat(col["format"].GetString());
            }
            break;
        case ColumnType::FIXED_BYTES:
            // fix length: col["size"]
            // 无需额外的信息
            break;
        case ColumnType::TAG:
        case ColumnType::WEIGHT:
        case ColumnType::INT32:
        case ColumnType::INT64:
        case ColumnType::FLOAT:
        case ColumnType::DOUBLE:
        case ColumnType::VARCHAR:
            // 无需额外的信息
            break;
        case ColumnType::NONE:
            assert(false);
            break;

    }
    return s;
}

Status ColumnDescriptorUtils::UnSerializeColumnGroupFromRapidJsonValue(const rapidjson::Value &jColGroup, ColumnDescriptor *config) {
    Status s;
    for (rapidjson::SizeType i = 0; i < jColGroup.Size(); ++i) {
        ColumnDescriptor subConfig;
        if (!jColGroup[i].IsObject()) {
            s = Status::InvalidArgument();
        } else {
            s = UnSerializeFromRapidJsonValue(jColGroup[i], &subConfig);
        }
        config->AddSubEdgeColumn(subConfig);
    }
    return s;
}


std::string ColumnDescriptorUtils::SerializeEdge(const EdgesQueryResult &result) {
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    Status s;
    writer.StartObject();

    // edge's basic info
    writer.Key("label");
    writer.String(result.GetLabel(&s).c_str());
    writer.Key("src_label");
    writer.String(result.GetSrcVertexLabel(&s).c_str());
    writer.Key("src");
    writer.String(result.GetSrcVertex(&s).c_str());
    writer.Key("dst_label");
    writer.String(result.GetDstVertexLabel(&s).c_str());
    writer.Key("dst");
    writer.String(result.GetDstVertex(&s).c_str());
    // edge's properties
    s = SerializeProp(result, writer);

    writer.EndObject();
    return sb.GetString();
}

std::string ColumnDescriptorUtils::SerializeVertex(const VertexQueryResult &result) {
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    Status s;
    writer.StartObject();

    // vertex's basic info
    writer.Key("l");
    writer.String(result.GetLabel(&s).c_str());
    writer.Key("v");
    writer.String(result.GetVertex(&s).c_str());
    // vertex's properties
    s = SerializeProp(result, writer);

    writer.EndObject();
    return sb.GetString();
}

}
