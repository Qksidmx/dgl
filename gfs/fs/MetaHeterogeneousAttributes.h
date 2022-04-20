#ifndef STARKNOWLEDGEGRAPHDATABASE_METAH_H
#define STARKNOWLEDGEGRAPHDATABASE_METAH_H

#include <vector>
#include <fmt/format.h>
//#include <rapidjson/filereadstream.h>
//#include <rapidjson/error/en.h>
//#include "rapidjson/writer.h"
//#include "rapidjson/document.h"

#include "MetaAttributes.h"
#include "util/skglogger.h"

namespace skg {

    /**
     * 异构网络的属性配置 (节点属性/边属性)
     */
    class MetaHeterogeneousAttributes {
    public:
        MetaHeterogeneousAttributes() = default;

    public:
        /**
         * 异构网络中含有哪些类型的节点
         */
        inline std::vector<VertexLabel> GetLabels() const {
            std::vector<VertexLabel> labels;
            for (const auto &attributes: m_attributes) {
                labels.emplace_back(attributes.label);
            }
            return labels;
        }

        /**
         * @brief 异构网络中含有哪些类型的边
         * @return
         */
        inline std::vector<EdgeLabel> GetEdgeLabels() const {
            std::vector<EdgeLabel> labels;
            for (const auto &prop: m_attributes) {
                labels.emplace_back(prop.label, prop.src_label, prop.dst_label);
            }
            return labels;
        }

        /**
         * 获取类型为`label`的属性列名字
         *
         * note: 对于 GroupColumn, 会返回其子属性列的名字
         * 如 有如下的属性列
         *   [ "age", "birth_place": ["province", "city"] , "location": ["latitude", "longitude"] ]
         * 返回 ["age", "province", "city", "latitude", "longitude"]
         */
        Status GetAttributesNames(const std::string &label, std::vector<std::string> *names) const ;
        Status GetAttributesNames(const EdgeLabel &label, std::vector<std::string> *names) const ;

        /**
         * 获取类型为`label`的属性列描述符
         *
         * note: 对于 GroupColumn, 会返回其子属性列的名字
         * 如 有如下的属性列
         *   [ "age", "birth_place": ["province", "city"] , "location": ["latitude", "longitude"] ]
         * 返回 ["age", "province", "city", "latitude", "longitude"] 对应的描述符
         */
        Status GetAttributesDescriptors(const std::string &label, std::vector<ColumnDescriptor> *descriptors) const ;
        Status GetAttributesDescriptors(const EdgeLabel &label, std::vector<ColumnDescriptor> *descriptors) const ;

        Status verify(const rapidjson::Value &d);

    private:
        std::vector<MetaAttributes> m_attributes;
    public:
        Status AddAttributes(MetaAttributes attributes);

        Status MatchQueryMetadata(
                const std::vector<ColumnDescriptor> &queryCols,
                MetaHeterogeneousAttributes *queryMetadata) const;

    public:
        /**
         *  ==== 迭代器 ====
         *  可通过 for (const auto &attributes: hetAttributes) { ... } 来访问异构属性中不同 类型 的attributes
         */

        using iterator = std::vector<MetaAttributes>::iterator;
        using const_iterator = std::vector<MetaAttributes>::const_iterator;

        inline iterator begin() {
            return m_attributes.begin();
        }

        inline iterator end() {
            return m_attributes.end();
        }

        inline const_iterator begin() const {
            return m_attributes.begin();
        }

        inline const_iterator end() const {
            return m_attributes.end();
        }

        size_t GetValSize(std::string attribute_label, std::string colname) {
            size_t unit_size = 0;
            for (auto &mab : m_attributes) {
                if (mab.label == attribute_label) {
                    const ColumnDescriptor *const p = mab.GetColumn(
                            ColumnDescriptor(colname, ColumnType::NONE), false);
                    if (p != nullptr) {
                        unit_size = p->value_size();
                    }
                    break;
                }
            }
            assert(unit_size != 0);
            return unit_size;
        }

        EdgeTag_t GetTagOfLabel(std::string attribute_label) const {
            for (const auto &prop: m_attributes) {
                if (prop.label == attribute_label) {
                    return prop.label_tag;
                }
            }
            assert(false);
            return 0;
        }

        Status GetLabelOfTag(EdgeTag_t tag, std::string *label) const {
            assert(label != nullptr);
            for (const auto &prop: m_attributes) {
                if (prop.label_tag == tag) {
                    *label = prop.label;
                    return Status::OK();
                }
            }
            return Status::NotExist();
        }

        bool GetLabel2Tag(std::map<std::string, EdgeTag_t>& label2tag) {
            label2tag.clear();
            for (size_t i = 0; i < m_attributes.size(); i ++) {
                MetaAttributes& mab = m_attributes[i];
                label2tag[mab.label] = mab.label_tag;
            }
            
            return true;
        }

        void GetLabelVec(std::vector<std::string>& label_vec) {
            for (size_t i = 0; i < m_attributes.size(); i ++) {
                label_vec.push_back(m_attributes[i].label);
            } 
        }

        std::vector<std::string> GetLabelVec() {
            std::vector<std::string> label_vec; 
            for (size_t i = 0; i < m_attributes.size(); i ++) {
                label_vec.push_back(m_attributes[i].label);
            } 
            return label_vec;
        }



        bool GetColumnVec(std::string label, std::vector<std::string>& column_vec) {
            for (size_t i = 0; i < m_attributes.size(); i ++) {
                MetaAttributes& mab = m_attributes[i];
                if (mab.label == label) {
                    for (size_t j = 0; j < mab.cols.size(); j ++) {
                        ColumnDescriptor& cd = mab.cols[j];
                        column_vec.push_back(cd.colname());
                    }
                    return true;
                }
            }
            assert(false);
            return false;
        }

        size_t GetNumOfAttributes() const {
            return m_attributes.size();
        }

    public:
        const_iterator GetAttributesByTag(EdgeTag_t tag) const;
        iterator GetAttributesByTag(EdgeTag_t tag);

        const_iterator GetAttributesByLabel(const std::string &label) const;
        iterator GetAttributesByLabel(const std::string &label);

        const_iterator GetAttributesByEdgeLabel(const EdgeLabel &label) const;
        iterator GetAttributesByEdgeLabel(const EdgeLabel &label);

        void Clear() {
            if (!this->m_attributes.empty()) 
		    this->m_attributes.clear();
        }
    public:
        /**
         * 序列化 && 反序列化
         */

        template <typename Writer>
        Status Serialize(Writer &writer) const;

        template <typename Writer>
        Status SerializeToExportStr(Writer &writer) const;

        /**
         * 从json文件中反序列化
         */
        Status ParseFromFile(const std::string &filename);

        /**
         * 从json字符串中反序列化
         */
        Status ParseFromString(const std::string &json);

        Status UnSerializeRapidJsonValue(const rapidjson::Value &jsonAttributes);
    };

    template<typename Writer>
    Status MetaHeterogeneousAttributes::Serialize(Writer &writer) const {
        // note 模板函数, 只能直接写在 .h 文件中, 否则编译时生成不了实例会导致编译错误
        Status s;
        writer.StartArray();
        for (const auto &attribute: m_attributes) {
            s = attribute.Serialize(writer);
            if (!s.ok()) { return s; }
        }
        writer.EndArray();
        return s;
    }

    template<typename Writer>
    Status MetaHeterogeneousAttributes::SerializeToExportStr(Writer &writer) const {
        Status s;
        writer.StartArray();
        for (const auto &prop: m_attributes) {
            s = prop.SerializeToExportStr(writer);
            if (!s.ok()) { return s; }
        }
        writer.EndArray();
        return Status();
    }


}
#endif //STARKNOWLEDGEGRAPHDATABASE_METAH_H
