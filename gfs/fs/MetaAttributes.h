#ifndef STARKNOWLEDGEGRAPHDATABASE_METAEDGEATTRIBUTES_H
#define STARKNOWLEDGEGRAPHDATABASE_METAEDGEATTRIBUTES_H

#include <string>

#include "util/types.h"
#include "ColumnDescriptor.h"

#include "rapidjson/document.h"

#include "ColumnDescriptorUtils.h"

//#include "tinyxml2.h"

namespace skg {

    /**
     * 同构网络的属性配置 (节点属性/边属性)
     *
     * label        -- 网络的类型, 如 `knows`, `call`
     * label_tag    -- 网络的类型, 内部存储使用的id
     * cols         -- 属性列的描述符, 如 `int`, `float`
     */
    class MetaAttributes {
    public:
        /**
         * @brief 边的属性集
         * @param label_        边的 label
         * @param src_label_    边的 src-label
         * @param dst_label_    边的 dst-label
         */
        MetaAttributes(
                const std::string &label_,
                const std::string &src_label_,
                const std::string &dst_label_)
                : label(label_), label_tag(0),
                  src_label(src_label_), src_tag(0),
                  dst_label(dst_label_), dst_tag(0),
                  m_flags(0x0),
                  cols() {
        }

        /**
         * @brief 边的属性集
         * @param label_.edge_label 边的 label
         *        label_.src_label  边的 src-label
         *        label_.dst_label  边的 dst-label
         */
        explicit
        MetaAttributes(const EdgeLabel &label_)
                : MetaAttributes(label_.edge_label, label_.src_label, label_.dst_label) {
        }

        /**
         * @brief 节点的属性集
         * @param label_    节点的 label
         */
        explicit
        MetaAttributes(const std::string &label_)
                : MetaAttributes(label_, "", "") {
        }

        MetaAttributes()
                : MetaAttributes("") {}

        // 节点/边的 label
        std::string label;
        EdgeTag_t label_tag;
        // 边src节点的label. valid iff IsEdge() == true
        std::string src_label;
        EdgeTag_t src_tag;
        // 边dst节点的label. valid iff IsEdge() == true
        std::string dst_label;
        EdgeTag_t dst_tag;
    public:
        /**
         * @brief 边是否带权重
         */
        inline bool IsWeighted() const {
            return (m_flags & FLAG_IS_WEIGHTED) != 0;
        }
        /**
         * @brief 定义节点的属性
         */
        inline bool IsVertex() const {
            return src_label.empty() && dst_label.empty();
        }
        /**
         * @brief 定义边的属性
         */
        inline bool IsEdge() const {
            return !IsVertex();
        }

        EdgeLabel GetEdgeLabel() const {
            assert(!src_label.empty() && !dst_label.empty());
            return EdgeLabel(label, src_label, dst_label);
        }
    private:
        enum Flags {
            // 是带权重的边
            FLAG_IS_WEIGHTED = 0x01,
        };
        // 标志位, 见 enum Flags
        uint32_t m_flags;
        // 节点/边的属性配置项
        std::vector<ColumnDescriptor> cols;
    public:
        Status MatchQueryMetadata(const std::vector<ColumnDescriptor> &queryCols, MetaAttributes *queryMetadataAttributes) const;

        const ColumnDescriptor* GetColumn(
                const ColumnDescriptor &queryCol,
                bool isCheckType) const;

        int32_t GetMaxColumnId() const {
            int32_t max_id = -1;
            for (const auto &col : cols) {
                if (col.columnType() == ColumnType::GROUP) {
                    for (const auto &subCol: col) {
                        max_id = std::max(max_id, subCol.id());
                    }
                } else {
                    max_id = std::max(max_id, col.id());
                }
            }
            return max_id;
        }

        /**
         * @brief 所有属性列, 占用的字节长度和
         */
        size_t GetColumnsValueByteSize() const;

        /**
         * @brief 添加属性列
         * @param col
         */
        Status AddColumn(ColumnDescriptor col);

        /**
         * @brief 删除属性列
         * @param columnName
         */
        Status DeleteColumn(const std::string &columnName);

        // TODO 清理这个函数
        std::vector<ColumnDescriptor> GetColumns() const {
            return cols;
        }

        /**
         * @brief 有多少个属性列
         */
        inline size_t GetColumnsSize() const {
            return cols.size();
        }

        void Clear();

#if 0
        DEPRECATED Status InitEdgeColumnsFromFile(const std::string &filename, bool isInitWithGroup);
        DEPRECATED Status InitVertexColumnsFromFile(const std::string &filename, bool isInitWithGroup);
        DEPRECATED Status InitEdgeColumnsFromLine(const std::string &str, bool isInitWithGroup);
        DEPRECATED Status InitVertexColumnsFromLine(const std::string &str, bool isInitWithGroup);
#endif

    public:
        /**
         * JSON 格式的序列化反序列化
         */

        template<typename Writer>
        Status Serialize(Writer &writer) const;

        template <typename Writer>
        Status SerializeToExportStr(Writer &writer) const;
        friend bool operator == (const MetaAttributes &l,const MetaAttributes &r );

    private:
        // for call UnSerializeRapidJsonValue
        friend class MetaHeterogeneousAttributes;
        Status UnSerializeRapidJsonValue(const rapidjson::Value &jsonAttributes);

    public:
        /**
         * XML 格式的 序列化反序列化
         */

        /**
         *
            <edge label="CALL" src_vertex_label="customer" dst_vertex_label="customer" is-weighted="true">
                <properties>
                    <property name="xxx" type="string" max-length=100 />
                    <property name="first_call_at" type="time"/>
                    <property name="total_call_in_num" type="int"/>
                    <property name="total_call_out_num" type="int"/>
                    <property name="total_call_in_duration" type="float"/>
                    <property name="total_call_out_duration" type="float"/>
                </properties>
            </edge>
         */
        //Status UnSerializeEdgePropertiesXMLString(const std::string &xmlStr);
        //Status UnSerializeEdgePropertiesXMLElement(const tinyxml2::XMLElement *entry);

        /**
         *
            <vertex label="customer">
                <properties>
                    <property name="age" type="int"/>
                    <property name="height" type="float"/>
                </properties>
            </vertex>
         */
        //Status UnSerializeVertexPropertiesXMLString(const std::string &xmlStr);
        //Status UnSerializeVertexPropertiesXMLElement(const tinyxml2::XMLElement *entry);

    private:
        //Status UnSerializeProperties(const tinyxml2::XMLElement *properties);

    private:

        static
        const ColumnDescriptor* InternalGetColumn(
                const ColumnDescriptor &queryCol,
                const ColumnDescriptor &col,
                bool isCheckType);

    public:
        /**
         *  ==== 迭代器 ====
         *  可通过 for (const auto &col: attributes) { ... } 来访问异构属性中不同 类型 的attributes
         */
        using iterator = std::vector<ColumnDescriptor>::iterator;
        using const_iterator = std::vector<ColumnDescriptor>::const_iterator;

        inline
        iterator begin() {
            return cols.begin();
        }

        inline
        iterator end() {
            return cols.end();
        }

        inline
        const_iterator begin() const {
            return cols.begin();
        }

        inline
        const_iterator end() const {
            return cols.end();
        }

        inline
        const ColumnDescriptor &operator[](size_t idx) const {
            assert(idx < cols.size());
            return cols[idx];
        }

        inline
        ColumnDescriptor &operator[](size_t idx) {
            assert(idx < cols.size());
            return cols[idx];
        }

    };

    template<typename Writer>
    Status MetaAttributes::Serialize(Writer &writer) const {
        // note 模板函数, 只能直接写在 .h 文件中, 否则编译时生成不了实例会导致编译错误
        Status s;
        writer.StartObject();

        writer.Key("flags");
        writer.Uint(m_flags);

        writer.Key("label");
        writer.String(label.c_str(), label.size());

        writer.Key("tag");
        writer.Uint(label_tag);

        writer.Key("src_label");
        writer.String(src_label.c_str());
        writer.Key("src_tag");
        writer.Uint(src_tag);

        writer.Key("dst_label");
        writer.String(dst_label.c_str());
        writer.Key("dst_tag");
        writer.Uint(dst_tag);

        writer.Key("cols");
        writer.StartArray();
        for (const auto &col: cols) {
            s = ColumnDescriptorUtils::Serialize(col, writer, false);
            if (!s.ok()) { return s; }
        }
        writer.EndArray();

        writer.EndObject();
        return s;
    }

    template<typename Writer>
    Status MetaAttributes::SerializeToExportStr(Writer &writer) const {
        Status s;
        writer.StartObject();

        writer.Key("label");
        writer.String(label.c_str(), label.size());

        if (IsEdge()) {
            writer.Key("outVLabel");
            writer.String(src_label.c_str());

            writer.Key("inVLabel");
            writer.String(dst_label.c_str());
        }

        writer.Key("properties");
        writer.StartArray();
        for (const auto &col: cols) {
            s = ColumnDescriptorUtils::Serialize(col, writer, true);
            if (!s.ok()) { return s; }
        }
        writer.EndArray();

        writer.EndObject();
        return Status();
    }

}

#endif //STARKNOWLEDGEGRAPHDATABASE_METAEDGEATTRIBUTES_H
