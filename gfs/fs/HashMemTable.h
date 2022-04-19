#ifndef STARKNOWLEDGEGRAPHDATABASE_HASHMEMTABLE_H
#define STARKNOWLEDGEGRAPHDATABASE_HASHMEMTABLE_H

#include <map>
#include "util/status.h"
#include "util/options.h"
#include "EdgesQueryResult.h"

#include "MemTable.h"
#include "EdgePartition.h"
#include "IDEncoder.h"
#include "preprocessing/parse/fileparse/fileparser.hpp"
#include "sparsehash/sparse_hash_map"


namespace skg {

    class HashMemTable: public MemTable {
    public:
        struct HashKey {
        public:
            vid_t src;
            vid_t dst;
            HashKey(const vid_t src_ , const vid_t dst_):src(src_),dst(dst_) {}
            HashKey() : src(0), dst(0) {}

            static
            const HashKey &GetInvalidKey() {
                const static HashKey INVALID_HASH_KEY(-1, -1);
                return INVALID_HASH_KEY;
            }
        };

        struct HashKeyHashFunc {
            size_t operator()(const HashKey &key) const {
                const unsigned long seed = 10000007;
                unsigned long __h = (key.src + key.dst) % seed;
                return size_t(__h);
            }
        };

        struct HashKeyComparator {
            bool operator()(const HashKey &b, const HashKey &a) const {
                return (a.dst == b.dst) && (a.src == b.src);
            }
        };

        struct HashEdgeData {
        public:
            EdgeWeight_t weight;
            ResultProperties m_properties;

        public:
            HashEdgeData(EdgeWeight_t weight_, size_t col_bytes)
                    : weight(weight_), m_properties(col_bytes)
            {}
            HashEdgeData() : HashEdgeData(1.0f, 0) {}
        };

    public:
        HashMemTable(const interval_t &interval,
                     const MetaAttributes &attributes,
                     const Options &options)
                : m_interval(interval),
                  m_buffered_edges(),
                  m_options(options),
                  m_attributes(attributes) {
            m_buffered_edges.set_deleted_key(HashKey::GetInvalidKey());
        }

        const EdgeLabel GetLabel() const override {
            return EdgeLabel(m_attributes.label, m_attributes.src_label, m_attributes.dst_label);
        }

        const interval_t &GetInterval() const override {
            return m_interval;
        }

        size_t GetNumEdges() const override {
            return m_buffered_edges.size();
        }

    public:
        // 节点出发的增/删/查/改

        Status DeleteVertex(const VertexRequest &request) override;

        Status GetInEdges(const VertexRequest &request, EdgesQueryResult *result) const override ;
        Status GetOutEdges(const VertexRequest &request, EdgesQueryResult *result) const override ;
        Status GetBothEdges(const VertexRequest &request, EdgesQueryResult *result) const override ;

        Status GetInVertices(const VertexRequest &request, VertexQueryResult *result) const override ;
        Status GetOutVertices(const VertexRequest &request, VertexQueryResult *result) const override ;
        Status GetBothVertices(const VertexRequest &request, VertexQueryResult *result) const override ;

        Status GetInDegree(const vid_t &request, int *degree) const override ;
        Status GetOutDegree(const vid_t &request, int *degree) const override ;

        // 边出发的增/删/查/改

        Status AddEdge(const EdgeRequest &request) override ;
        Status DeleteEdge(const EdgeRequest &request) override ;
        Status GetEdgeAttributes(const EdgeRequest &request, EdgesQueryResult *result) override ;
        Status SetEdgeAttributes(const EdgeRequest &request) override ;

        Status ExtractEdges(std::vector<MemoryEdge> *edges, interval_t *interval) override {
            assert(edges != nullptr);
            assert(edges->empty());
            assert(interval != nullptr);
            *interval = m_interval;
            // buffer为空, 不做处理
            if (this->GetNumEdges() == 0) { return Status::OK(); }
            Status s;
            // 重新组织为 vector, 刷到
            edges->reserve(this->GetNumEdges());
            for (auto itr = m_buffered_edges.begin(); itr != m_buffered_edges.end(); itr++) {
                MemoryEdge edge(itr->first.src, itr->first.dst,
                                itr->second.weight, m_attributes.label_tag,
                                m_attributes.GetColumnsValueByteSize());
                // 填充边的属性数据
                Slice fixed_bytes = itr->second.m_properties.fixed_bytes();
                edge.SetData(fixed_bytes.data(), 0, fixed_bytes.size());
                edge.CopyProperty(itr->second.m_properties.bitset());
                edges->emplace_back(edge);
            }
            m_buffered_edges.clear();
            return s;
        }

        Status CreateEdgeAttrCol(ColumnDescriptor descriptor) override ;

        size_t GetEstimateSize() const override {
            // 估算每条边占用空间大小
            size_t bytes_per_edge = sizeof(HashKey) + sizeof(EdgeWeight_t) + m_attributes.GetColumnsValueByteSize();
            // 估算 MemTable 大小
            size_t bytes_size = this->GetNumEdges() * bytes_per_edge;
            return bytes_size;
        }

        bool IsNeedFlush() const override {
            return GetEstimateSize() > m_options.mem_buffer_mb * MB_BYTES;
        }

        Status ExportData(const std::string &outDir,
                          std::shared_ptr<IDEncoder> encoder,
                          uint32_t shard_id, uint32_t partition_id) const override;
    private:
        /**
         * 按照 columns 中的属性列名字, 获取存储的边属性值, 存放到 buff 中
         *
         * columns: ColumnDescriptor 中, 设置了待获取的属性名 (note: 没有待获取的属性类型)
         */
        inline
        void CollectProperties(
                const HashEdgeData &edge,
                const std::vector<ColumnDescriptor> &columns,
                char *buff, PropertiesBitset_t *bitset) const;

        /**
         * 按照
         */
        inline
        void ReorderAttributesToEdge(const EdgeRequest &request, HashEdgeData *edge) const;
    private:
        // 管理的顶点区间
        interval_t m_interval;

        // buffer
        google::sparse_hash_map<HashKey, HashEdgeData, HashKeyHashFunc, HashKeyComparator> m_buffered_edges;
        //std::unordered_map<HashKey, HashEdgeData, HashKeyHashFunc, HashKeyComparator> m_buffered_edges;
        const Options m_options;

        MetaAttributes m_attributes;

    public:
        // no copying allow
        HashMemTable(const HashMemTable &) = delete;
        HashMemTable& operator=(const HashMemTable &) = delete;
    };

}
#endif //STARKNOWLEDGEGRAPHDATABASE_HASHMEMTABLE_H
