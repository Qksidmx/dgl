#include <env/env.h>
#include "MetaHeterogeneousAttributes.h"

namespace skg {

Status MetaHeterogeneousAttributes::AddAttributes(MetaAttributes attributes) {
    if (attributes.label.empty()) {
        return Status::InvalidArgument("can NOT add properties with empty label");
    }
    EdgeTag_t existed_max_tag = 0;
    for (size_t i = 0; i < m_attributes.size(); ++i) {
        if (m_attributes[i].label == attributes.label) {
            if (attributes.IsVertex()) {
                return Status::InvalidArgument("duplicate vertex properties");
            } else if (attributes.IsEdge()) {
                if (m_attributes[i].src_label == attributes.src_label
                    && m_attributes[i].dst_label == attributes.dst_label) {
                    return Status::InvalidArgument("duplicate edge properties");
                }
                // note: 允许插入 相同 edge_label, 不同的 src_label / dst_label
            } else {
                assert(false);
                return Status::InvalidArgument("duplicate properties");
            }
        }
        existed_max_tag = std::max(m_attributes[i].label_tag, existed_max_tag);
    }
    attributes.label_tag = existed_max_tag + 1;
    m_attributes.emplace_back(attributes);
    return Status::OK();
}

Status MetaHeterogeneousAttributes::MatchQueryMetadata(
        const std::vector<ColumnDescriptor> &queryCols,
        MetaHeterogeneousAttributes *queryMetadata) const {
    Status s;
    for (size_t i = 0; i < m_attributes.size(); ++i) {
        MetaAttributes queryMetaAttributes;
        s = m_attributes[i].MatchQueryMetadata(queryCols, &queryMetaAttributes);
        if (!s.ok()) { return s; }
        s = queryMetadata->AddAttributes(queryMetaAttributes);
        if (!s.ok()) { return s; }
    }
    return s;
}

MetaHeterogeneousAttributes::iterator
MetaHeterogeneousAttributes::GetAttributesByTag(EdgeTag_t tag) {
    for (auto iter = m_attributes.begin(); iter != m_attributes.end(); ++iter) {
        if (tag == iter->label_tag) {
            return iter;
        }
    }
    return this->end();
}

MetaHeterogeneousAttributes::const_iterator
MetaHeterogeneousAttributes::GetAttributesByTag(EdgeTag_t tag) const {
    for (auto iter = m_attributes.begin(); iter != m_attributes.end(); ++iter) {
        if (tag == iter->label_tag) {
            return iter;
        }
    }
    return this->end();
}

MetaHeterogeneousAttributes::const_iterator
MetaHeterogeneousAttributes::GetAttributesByLabel(const std::string &label) const {
    for (auto iter = m_attributes.begin(); iter != m_attributes.end(); ++iter) {
        if (label == iter->label) {
            return iter;
        }
    }
    return this->end();
}

MetaHeterogeneousAttributes::iterator
MetaHeterogeneousAttributes::GetAttributesByLabel(const std::string &label) {
    for (auto iter = m_attributes.begin(); iter != m_attributes.end(); ++iter) {
        if (label == iter->label) {
            return iter;
        }
    }
    return this->end();
}

MetaHeterogeneousAttributes::const_iterator
MetaHeterogeneousAttributes::GetAttributesByEdgeLabel(const EdgeLabel &label) const {
    for (auto iter = m_attributes.begin(); iter != m_attributes.end(); ++iter) {
        if (label == iter->GetEdgeLabel()) {
            return iter;
        }
    }
    return this->end();
}

MetaHeterogeneousAttributes::iterator
MetaHeterogeneousAttributes::GetAttributesByEdgeLabel(const EdgeLabel &label) {
    for (auto iter = m_attributes.begin(); iter != m_attributes.end(); ++iter) {
        if (label == iter->GetEdgeLabel()) {
            return iter;
        }
    }
    return this->end();
}

Status MetaHeterogeneousAttributes::ParseFromFile(const std::string &filename) {
#if 0
    SKG_LOG_DEBUG("Parsing het attributes from {}", filename);
#endif
    std::string data;
    Status s = ReadFileToString(Env::Default(), filename, &data);
    if (s.ok()) {
        s = ParseFromString(data);
    }
    return s;
}

Status MetaHeterogeneousAttributes::ParseFromString(const std::string &json) {
    Status s;
    rapidjson::Document d;
    // 解析错误处理
    // https://github.com/Tencent/rapidjson/blob/master/doc/dom.zh-cn.md#%E8%A7%A3%E6%9E%90%E9%94%99%E8%AF%AF-parseerror
    if (d.Parse(json.c_str()).HasParseError()) {
        s = Status::InvalidArgument(json);
        return s;
    }
    s = UnSerializeRapidJsonValue(d);
    return s;
}

Status MetaHeterogeneousAttributes::verify(const rapidjson::Value &d) 
{
    Status s;
    if (!d.IsArray()) {
        s = Status::InvalidArgument("not json array");
        return s;
    }
#if 0
    // debug
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    d.Accept(writer);
    SKG_LOG_DEBUG("parsing json: `{}'", sb.GetString());
    // end debug
#endif
    int hitCount=0;
    for (rapidjson::SizeType i = 0; i < d.Size(); ++i) 
    {
        MetaAttributes attributes;
        s = attributes.UnSerializeRapidJsonValue(d[i]);
        if (!s.ok()) { return s; }
	for (int ii = 0; ii < this->m_attributes.size(); ii++) 
	{
            if (this->m_attributes[ii]==attributes)
	    {
		hitCount++;
		break;
	    }
	}
    }
    if (this->m_attributes.size()!=hitCount)
        s = Status::InvalidArgument("Inconsistent CheckPoint: unmatched edge property.");
    return s;
}
Status MetaHeterogeneousAttributes::UnSerializeRapidJsonValue(const rapidjson::Value &d) {
    Status s;
    if (!d.IsArray()) {
        s = Status::InvalidArgument("not json array");
        return s;
    }
#if 0
    // debug
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    d.Accept(writer);
    SKG_LOG_DEBUG("parsing json: `{}'", sb.GetString());
    // end debug
#endif
    for (rapidjson::SizeType i = 0; i < d.Size(); ++i) {
        MetaAttributes attributes;
        s = attributes.UnSerializeRapidJsonValue(d[i]);
        if (!s.ok()) { return s; }
        m_attributes.emplace_back(attributes);
    }
    return s;
}

Status MetaHeterogeneousAttributes::GetAttributesNames(
        const std::string &label,
        std::vector<std::string> *names) const {
    assert(names != nullptr);
    names->clear();
    auto attributes = this->GetAttributesByLabel(label);
    if (attributes == this->end()) {
        return Status::NotExist(fmt::format("edge label: `{}' not exist", label));
    }
    // GroupColumn 的处理
    for (const auto &col: *attributes) {
        if (col.columnType() == ColumnType::GROUP) {
            for (const auto &subCol: col) {
                names->emplace_back(subCol.colname());
            }
        } else {
            names->emplace_back(col.colname());
        }
    }
    return Status::OK();
}

Status MetaHeterogeneousAttributes::GetAttributesNames(
        const EdgeLabel &label,
        std::vector<std::string> *names) const {
    assert(names != nullptr);
    names->clear();
    auto attributes = this->GetAttributesByEdgeLabel(label);
    if (attributes == this->end()) {
        return Status::NotExist(fmt::format("edge label: `{}' not exist", label.ToString()));
    }
    // GroupColumn 的处理
    for (const auto &col: *attributes) {
        if (col.columnType() == ColumnType::GROUP) {
            for (const auto &subCol: col) {
                names->emplace_back(subCol.colname());
            }
        } else {
            names->emplace_back(col.colname());
        }
    }
    return Status::OK();
}

Status MetaHeterogeneousAttributes::GetAttributesDescriptors(
        const std::string &label,
        std::vector<ColumnDescriptor> *descriptors) const {
    assert(descriptors != nullptr);
    descriptors->clear();
    auto attributes = this->GetAttributesByLabel(label);
    if (attributes == this->end()) {
        return Status::NotExist(fmt::format("vertex label: `{}' not exist", label));
    }
    // GroupColumn 的处理
    for (const auto &col: *attributes) {
        if (col.columnType() == ColumnType::GROUP) {
            for (const auto &subCol: col) {
                descriptors->emplace_back(subCol);
            }
        } else {
            descriptors->emplace_back(col);
        }
    }
    return Status::OK();
}

Status MetaHeterogeneousAttributes::GetAttributesDescriptors(
        const EdgeLabel &label,
        std::vector<ColumnDescriptor> *descriptors) const {
    assert(descriptors != nullptr);
    descriptors->clear();
    auto attributes = this->GetAttributesByEdgeLabel(label);
    if (attributes == this->end()) {
        return Status::NotExist(fmt::format("edge label: `{}' not exist", label.ToString()));
    }
    // GroupColumn 的处理
    for (const auto &col: *attributes) {
        if (col.columnType() == ColumnType::GROUP) {
            for (const auto &subCol: col) {
                descriptors->emplace_back(subCol);
            }
        } else {
            descriptors->emplace_back(col);
        }
    }
    return Status::OK();
}

}
