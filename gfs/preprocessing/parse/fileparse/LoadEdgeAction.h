#ifndef STARKNOWLEDGEGRAPHDATABASE_LOADEDGEACTION_H
#define STARKNOWLEDGEGRAPHDATABASE_LOADEDGEACTION_H

#include <string>
#include <vector>

#include "skgdb/SkgDB.h"
#include "skgdb/Status.h"
#include "fileparser.hpp"
#include "storage/meta/MetaAttributes.h"
#include "storage/ColumnDescriptorUtils.h"


namespace skg {
    struct LoadEdgeAction: public preprocess::Action {
    private:
        const char m_delim;
        const bool m_ignore_header;
        std::vector<MemoryEdge> *m_container;
        MetaAttributes m_properties;
    public:
        explicit
        LoadEdgeAction(std::vector<MemoryEdge> *container,
                       const MetaAttributes &properties,
                       const char delim = ',',
                       bool ignoreHeader = false)
                : m_delim(delim), m_ignore_header(ignoreHeader), m_container(container),
                  m_properties(properties) {
            SKG_LOG_DEBUG("Loading edges with {} bytes data.", m_properties.GetColumnsValueByteSize());
        }

        Status operator()(size_t linenum, char *line) override {
            if (linenum == 0 && m_ignore_header) return Status::OK();
            if (line[0] == '#') return Status::OK();
            if (line[0] == '%') return Status::OK();

            std::vector<Slice> parts = StringUtils::split(line, m_delim);
            size_t expect_parts_size = m_properties.GetColumnsSize() + 2;
            if (m_properties.IsWeighted()) {
                expect_parts_size += 1;
            }
            if (parts.size() != expect_parts_size) {
                return Status::InvalidArgument(fmt::format(
                        "Parse error, expect {} fields, but get {}. line({}):`{}'",
                        m_properties.GetColumnsSize() + 2, parts.size(), linenum, line
                ));
            }
            Status s;

#ifndef SKG_SRC_SPLIT_SHARD
            const vid_t src = StringUtils::ParseUint32(parts[0].ToString(), &s);
            if (!s.ok()) { return s; }
            const vid_t dst = StringUtils::ParseUint32(parts[1].ToString(), &s);
            if (!s.ok()) { return s; }
#else
            const vid_t dst = StringUtils::ParseUint32(parts[0].ToString(), &s);
            if (!s.ok()) { return s; }
            const vid_t src = StringUtils::ParseUint32(parts[1].ToString(), &s);
            if (!s.ok()) { return s; }
#endif
            if (src == dst) {
                return Status::UnSupportSelfLoop(fmt::format("Self-loop edge: {} -> {}, @ linenum {}", src, dst, linenum));
            }

            // 解析边属性
            MemoryEdge edge(src, dst, 0, m_properties.label_tag, m_properties.GetColumnsValueByteSize());
            // 边权重
            size_t parts_idx = 2;
            if (m_properties.IsWeighted()) {
                ColumnDescriptorUtils::ParseWeight(parts[parts_idx].ToString().c_str(), &edge.weight);
                parts_idx++;
            }
            for (const auto &col: m_properties) {
                if (col.columnType() != ColumnType::GROUP) {
                    s = ColumnDescriptorUtils::ParseValueBytes(
                            col, parts[parts_idx].ToString().c_str(),
                            edge.GetColsData().data() + col.offset());
                    if (!s.ok()) { return s; }
                    edge.SetProperty(col.id()); // 属性不为 null
                } else {
                    // TODO 处理 GroupColumn
                    return Status::NotImplement("can NOT load Column Group data yet");
                }
                parts_idx++;
            }
            m_container->emplace_back(std::move(edge));
            return s;
        }
    };

}

#endif //STARKNOWLEDGEGRAPHDATABASE_INSERTEDGEACTION_H
