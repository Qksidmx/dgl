#ifndef STARKNOWLEDGEGRAPHDATABASE_EDGECOLUMNBUILDER_H
#define STARKNOWLEDGEGRAPHDATABASE_EDGECOLUMNBUILDER_H

#include "ColumnDescriptor.h"
#include "EdgesQueryResult.h"
#include "VertexQueryResult.h"

#include "IEdgeColumnPartition.h"
#include "IEdgeColumnWriter.h"

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace skg {

class ColumnDescriptorUtils {
public:

    static
    Status ParseValueBytes(const ColumnDescriptor &descriptor, const char *strValue, void *val);

    static
    Status ParseWeight(const char *strValue, void *val);

private:
    static
    Status UnSerializeColumnGroupFromRapidJsonValue(const rapidjson::Value &jColGroup, ColumnDescriptor *config);

public:
    static
    Status UnSerializeFromRapidJsonValue(const rapidjson::Value &col, ColumnDescriptor *config);

    static
    Status CreateWriter(const ColumnDescriptor &descriptor,
                        const std::string &edgelistFile,
                        std::unique_ptr<IEdgeColumnPartitionWriter> *writer);

    static
    Status CreatePartition(const ColumnDescriptor &descriptor,
                           const std::string &dirname,
                           uint32_t shard_id, uint32_t partition_id,
                           const interval_t &interval,
                           EdgeTag_t tag,
                           IEdgeColumnPartitionPtr *fragment);

    template<typename Writer>
    static
    Status Serialize(const ColumnDescriptor &col, Writer &writer, bool is_export);

    template<typename Writer, typename QueryResult>
    static
    Status SerializeProp(const QueryResult &result, Writer &writer);

    template<typename QueryResult>
    static
    std::string SerializePropList(const QueryResult &result);

    static
    std::string SerializeEdge(const EdgesQueryResult &result);

    static
    std::string SerializeVertex(const VertexQueryResult &result);
public:
    // No instantiate allowed
    ColumnDescriptorUtils() = delete;
    // No copying allowed
    ColumnDescriptorUtils(const ColumnDescriptorUtils &) = delete;
    ColumnDescriptorUtils &operator=(const ColumnDescriptorUtils &) = delete;
};

template<typename Writer>
Status ColumnDescriptorUtils::Serialize(const ColumnDescriptor &col, Writer &writer, bool is_export) {
    Status s;
    writer.StartObject();

    writer.Key("name");
    writer.String(col.colname().c_str());

    writer.Key("type");
    if (is_export) {
        std::string ss;
        switch (col.columnType()) {
            case ColumnType::NONE:
                break;
            case ColumnType::TAG:
                break;
            case ColumnType::WEIGHT:
                break;
            case ColumnType::INT:
                ss = "int";
                break;
            case ColumnType::LONG:
                ss = "int64";
                break;
            case ColumnType::FLOAT:
                ss = "float";
                break;
            case ColumnType::DOUBLE:
                ss = "double";
                break;
            case ColumnType::FIXED_BYTES:
                ss = "bytes";
                break;
            case ColumnType::TIME:
                ss = "time";
                break;
            case ColumnType::GROUP:
                ss = "group";
                break;
            case ColumnType::VARCHAR:
                ss = "varchar";
                break;
        }
        writer.String(ss.c_str());
    } else {
        writer.Uint(static_cast<uint32_t>(col.columnType()));
    }

    writer.Key("size");
    writer.Uint(col.value_size());

    // 特殊额外信息的序列化处理
    if (col.columnType() == skg::ColumnType::GROUP) {
        writer.Key("subCols");
        writer.StartArray();
        for (const auto &subCol: col) {
            s = Serialize(subCol, writer, is_export);
            if (!s.ok()) { return s; }
        }
        writer.EndArray();
    } else if (col.columnType() == skg::ColumnType::TIME) {
        writer.Key("format");
        writer.String(col.GetTimeFormat().c_str());
    }

    writer.EndObject();
    return s;
}

template<typename Writer, typename QueryResult>
Status ColumnDescriptorUtils::SerializeProp(const QueryResult &result, Writer &writer) {
    Status s;
    const ResultMetadata * const metadata = result.GetMetaDataByLabel(result.GetLabel(&s));
    if (!s.ok()) { return s; }
    assert(metadata != nullptr);
    for (size_t i = 0; i < metadata->GetColumnCount(); ++i) {
        writer.Key(metadata->GetColumnName(i));
        switch (metadata->GetColumnType(i)) {
            case ColumnType::INT32:
                writer.Int(result.GetInt32(i, &s));
                break;
            case ColumnType::WEIGHT:
            case ColumnType::FLOAT:
                writer.Double(result.GetFloat(i, &s));
                break;
            case ColumnType::DOUBLE:
                writer.Double(result.GetDouble(i, &s));
                break;
            case ColumnType::TIME:
                writer.String(result.GetTimeString(i, &s).c_str());
                break;
            case ColumnType::FIXED_BYTES:
                writer.String(result.GetString(i, &s).c_str());
                break;
            case ColumnType::INT64:
                writer.Int64(result.GetInt64(i, &s));
                break;
            // FIXME edge not support var-char now
            case ColumnType::VARCHAR:
               writer.String(result.GetString(i, &s).c_str());
               break;
            case ColumnType::NONE:
            case ColumnType::TAG:
            case ColumnType::GROUP:
                assert(false);
                break;
        }
    }

    return s;
}

template<typename QueryResult>
std::string ColumnDescriptorUtils::SerializePropList(const QueryResult &result) {
    Status s;
    fmt::MemoryWriter w;
    const ResultMetadata * const metadata = result.GetMetaDataByLabel(result.GetLabel(&s));
    if (!s.ok()) {
        SKG_LOG_DEBUG("s: {}", s.ToString());
        return "status";
    }
    assert(metadata != nullptr);
    for (size_t i = 0; i < metadata->GetColumnCount(); ++i) {
        if (i != 0) { w.write(","); } 
        else { w.write("{{"); }
        w.write("\"{}\":",metadata->GetColumnName(i));
        switch (metadata->GetColumnType(i)) {
            case ColumnType::INT32:
                w.write("\"{}\"", result.GetInt32(i, &s));
                break;
            case ColumnType::WEIGHT:
            case ColumnType::FLOAT:
                w.write("\"{}\"", result.GetFloat(i, &s));
                break;
            case ColumnType::DOUBLE:
                w.write("\"{}\"", result.GetDouble(i, &s));
                break;
            case ColumnType::TIME:
                w.write("\"{}\"", result.GetTimeString(i, &s));
                break;
            case ColumnType::FIXED_BYTES:
                w.write("\"{}\"", result.GetString(i, &s));
                break;
            case ColumnType::INT64:
                w.write("\"{}\"", result.GetInt64(i, &s));
                break;
            // FIXME edge not support var-char now
            case ColumnType::VARCHAR:
                w.write("\"{}\"", result.GetString(i, &s));
                break;
            case ColumnType::NONE:
            case ColumnType::TAG:
            case ColumnType::GROUP:
                assert(false);
                break;
        }
        if (i+1 == metadata->GetColumnCount()) {
            w.write("}}");
        }
    }

    return w.c_str();
}

}
#endif //STARKNOWLEDGEGRAPHDATABASE_EDGECOLUMNBUILDER_H
