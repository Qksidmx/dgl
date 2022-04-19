#include "IRequest.h"

#include "MetaAttributes.h"

#include "fmt/format.h"

#include "util/skglogger.h"

namespace skg {

    Status MetaAttributes::MatchQueryMetadata(const std::vector<ColumnDescriptor> &queryCols,
                                              MetaAttributes *queryMetadataAttributes) const {
        queryMetadataAttributes->label_tag = label_tag;
        queryMetadataAttributes->label = label;
        queryMetadataAttributes->src_label = src_label;
        queryMetadataAttributes->dst_label = dst_label;
        Status s;
        if (queryCols.size() == 1 && queryCols[0].colname() == IRequest::QUERY_ALL_COLUMNS[0]) {
            for (const auto &col : cols) {
                s = queryMetadataAttributes->AddColumn(col);
                if (!s.ok()) { return s; }
            }
        } else {
            for (const auto &queryCol: queryCols) {
                const ColumnDescriptor *col = this->GetColumn(queryCol, false);
                if (col != nullptr) {
                    s = queryMetadataAttributes->AddColumn(*col);
                    if (!s.ok()) { return s; }
                }
            }
        }
        return s;
    }

    bool operator == (const MetaAttributes &l, const MetaAttributes &r )
    {
	bool ret=true;
        ret=ret&&(r.label_tag == l.label_tag)\
        &&(r.label == l.label)\
        &&(r.src_label == l.src_label)\
        &&(r.dst_label == l.dst_label);
        if (!ret) return ret;
            int hitCount=0;
            for (const auto &rCol: r.cols) {
		for (int ii = 0; ii < l.cols.size(); ii++) 
		{
		    if (l.cols[ii]==rCol)
		    {
			hitCount++;
			break;
		    }
		}
            }
        if (hitCount!=r.cols.size()) return false;
	return true;
    }
    size_t MetaAttributes::GetColumnsValueByteSize() const {
        size_t bytes_size = 0;
        for (const auto &col: cols) {
            if (col.columnType() == ColumnType::TAG || col.columnType() == ColumnType::WEIGHT) {
                continue;
            }
            // Note: FIXED-BYTES 占用的字节为xml中指定最长支持的长度
            // Note: VarChar 占用的字节为 sizeof(uint32_t), 在 ResultProperties 中存储变长属性的偏移量
            bytes_size += col.value_size();
        }
        return bytes_size;
    }

    Status MetaAttributes::AddColumn(ColumnDescriptor col) {
        {// 检查属性列个数是否已经超过上限
            const size_t num_cols = GetColumnsSize();
            if (num_cols >= SKG_MAX_EDGE_PROPERTIES_SIZE) {
                return Status::NotSupported(fmt::format("there is already {} cols in edge of {}", num_cols, label));
            }
        }
        {// 检查如果插入属性列, 会不会超过限制的字节数上限
            const size_t bytes_all_cols = GetColumnsValueByteSize();
            assert(bytes_all_cols < SKG_MAX_EDGE_PROPERTIES_BYTES);
            if (bytes_all_cols + col.value_size() >= SKG_MAX_EDGE_PROPERTIES_BYTES) {
                return Status::NotSupported(
                        fmt::format("there is already {} bytes in edge of {}"
                                    ", can NOT add new col:{}, type:{}, size:{}",
                                    bytes_all_cols, label,
                                    col.colname(), static_cast<int32_t>(col.columnType()), col.value_size()
                        ));
            }
        }
        {// FIXME 暂时不支持创建边的变长属性列
            if (IsEdge() && col.columnType() == ColumnType::VARCHAR) {
                return Status::NotSupported("create var-char edge properties");
            }
        }
        // 检查是否已经存在同名的列
        int32_t max_existed_id = GetMaxColumnId();
        if (col.columnType() == ColumnType::GROUP) {
            // 如果是 ColumnGroup, 需要检查 ColumnGroup 中的每一个子列, 都不存在同名的列
            const ColumnDescriptor *pExistedCol = nullptr;
            for (const auto &subCol: col) {
                pExistedCol = this->GetColumn(subCol, false);
                if (pExistedCol != nullptr) {
                    return Status::InvalidArgument(fmt::format("column `{}' already exist.", col.colname()));
                }
            }
            // 不存在同名的列, 设置偏移量
            size_t subOffset = GetColumnsValueByteSize();
            col.SetOffset(static_cast<uint32_t>(subOffset));
            for (auto &subCol: col) {
                subCol.SetOffset(subOffset);
                // 设置属性列id
                subCol.SetColumnID(++max_existed_id);
                subOffset += subCol.value_size();
            }
        } else {
            // 不是 ColumnGroup, 查找是否存在同名列
            const ColumnDescriptor *pExistedCol = this->GetColumn(col, false);
            if (pExistedCol != nullptr) {
                return Status::InvalidArgument(fmt::format("column `{}' already exist.", col.colname()));
            }
            // 不存在同名的列, 设置偏移量
            // TODO 支持用户自定义时间格式
            if (col.columnType() == ColumnType::TIME && col.GetTimeFormat() != "%Y-%m-%d %H:%M:%S") {
                return  Status::NotImplement(fmt::format(
                        "can NOT yet support time format: \"{}\"."
                        "only support \"%Y-%m-%d %H:%M:%S\" now",
                        col.GetTimeFormat()));
            }
            col.SetOffset(static_cast<uint32_t>(GetColumnsValueByteSize()));
            // 设置属性列id
            col.SetColumnID(++max_existed_id);
        }
        cols.emplace_back(col);
        return Status::OK();
    }

    Status MetaAttributes::DeleteColumn(const std::string &columnName) {
        for (auto iter = cols.begin(); iter != cols.end(); ++iter) {
            if (iter->columnType() == ColumnType::GROUP) {
                // 不支持删除 GroupColumn 中的属性列
                for (const auto &subCol: *iter) {
                    if (subCol.colname() == columnName) {
                        return Status::NotImplement("can NOT delete a column inside column group");
                    }
                }
            }
            if (iter->colname() == columnName) {
                iter = cols.erase(iter);
                return Status::OK();
            }
        }
        return Status::NotExist();
    }
#if 0
    Status MetaAttributes::InitEdgeColumnsFromFile(const std::string &filename, bool isInitWithGroup) {
        std::ifstream edgefile(filename.c_str(), std::ios::in);
        if (!edgefile.is_open()) {
            return Status::IOError(fmt::format("Can NOT open file: {}", filename));
        }
        std::string line;
        getline(edgefile, line);
        Status s = InitEdgeColumnsFromLine(line, isInitWithGroup);
        edgefile.close();
        return s;
    }

    Status MetaAttributes::InitVertexColumnsFromFile(const std::string &filename, bool isInitWithGroup) {
        std::ifstream verticesFile(filename.c_str(), std::ios::in);
        if (!verticesFile.is_open()) {
            return Status::IOError(fmt::format("Can NOT open file: {}", filename));
        }
        std::string line;
        getline(verticesFile, line);
        Status s = InitVertexColumnsFromLine(line, isInitWithGroup);
        verticesFile.close();
        return s;
    }

    Status MetaAttributes::InitEdgeColumnsFromLine(const std::string &str, bool isInitWithGroup) {
        Status s;
        std::vector<std::string> eCols;
        StringUtils::split(str, ',', eCols);
        std::vector<std::string> parts;
        const std::string SUPPORTED_EDGE_TYPE = "tag,weight,string,int,float,double,byte";
        ColumnDescriptor groupColsConfig("__init_group", ColumnType::GROUP);
        bool hasLabelCol = false;
        for (size_t i = 2; i < eCols.size(); i++) {
            parts.clear();
            StringUtils::split(eCols[i], ':', parts);
            const std::string &name = parts[0];
            const std::string &type = parts[1];
            if (type == "tag") {
                hasLabelCol = true;
                s = AddColumn(ColumnDescriptor(name, ColumnType::TAG));
            } else if (type == "weight") {
                s = AddColumn(ColumnDescriptor(name, ColumnType::WEIGHT));
            } else if (type == "string") {
                if (isInitWithGroup) {
                    groupColsConfig.AddSubEdgeColumn(
                            ColumnDescriptor(name, ColumnType::FIXED_BYTES).SetFixedLength(25));
                } else {
                    s = AddColumn(ColumnDescriptor(name, ColumnType::FIXED_BYTES).SetFixedLength(25));
                }
            } else if (type == "int") {
                if (isInitWithGroup) {
                    groupColsConfig.AddSubEdgeColumn(ColumnDescriptor(name, ColumnType::INT32));
                } else {
                    s = AddColumn(ColumnDescriptor(name, ColumnType::INT32));
                }
            } else if (type == "float") {
                if (isInitWithGroup) {
                    groupColsConfig.AddSubEdgeColumn(ColumnDescriptor(name, ColumnType::FLOAT));
                } else {
                    s = AddColumn(ColumnDescriptor(name, ColumnType::FLOAT));
                }
            } else if (type == "double") {
                if (isInitWithGroup) {
                    groupColsConfig.AddSubEdgeColumn(ColumnDescriptor(name, ColumnType::DOUBLE));
                } else {
                    s = AddColumn(ColumnDescriptor(name, ColumnType::DOUBLE));
                }
            } else if (type == "byte") {
                if (isInitWithGroup) {
                    groupColsConfig.AddSubEdgeColumn(ColumnDescriptor(name, ColumnType::BYTE));
                } else {
                    s = AddColumn(ColumnDescriptor(name, ColumnType::BYTE));
                }
            } else if (type == "time") {
                if (isInitWithGroup) {
                    groupColsConfig.AddSubEdgeColumn(ColumnDescriptor(name, ColumnType::TIME));
                } else {
                    s = AddColumn(ColumnDescriptor(name, ColumnType::TIME));
                }
            } else {
                return Status::NotImplement(fmt::format("Type error: {}, support type: {}",
                                                        type, SUPPORTED_EDGE_TYPE));
            }
            if (!s.ok()) { return s; }
        }
        // 默认创建名为 "label" 的属性列
        if (!hasLabelCol) {
            s = AddColumn(ColumnDescriptor("label", ColumnType::TAG));
            if (!s.ok()) { return s; }
        }
        if (groupColsConfig.isNonEmptyColumnGroup()) {
            s = AddColumn(groupColsConfig);
            if (!s.ok()) { return s; }
        }
        return s;
    }

    Status MetaAttributes::InitVertexColumnsFromLine(const std::string &str, bool isInitWithGroup) {
        Status s;
        std::vector<std::string> eCols;
        StringUtils::split(str, ',', eCols);
        std::vector<std::string> parts;
        const std::string SUPPORTED_EDGE_TYPE = "tag,weight,string,int,float,double,byte";
        ColumnDescriptor groupColsConfig("__init_group", ColumnType::GROUP);
        for (size_t i = 1; i < eCols.size(); i++) {
            parts.clear();
            StringUtils::split(eCols[i], ':', parts);
            const std::string &name = parts[0];
            const std::string &type = parts[1];
            if (type == "tag") {
                s = AddColumn(ColumnDescriptor(name, ColumnType::TAG));
            } else if (type == "weight") {
                s = AddColumn(ColumnDescriptor(name, ColumnType::WEIGHT));
            } else if (type == "string") {
                if (isInitWithGroup) {
                    groupColsConfig.AddSubEdgeColumn(
                            ColumnDescriptor(name, ColumnType::FIXED_BYTES).SetFixedLength(25));
                } else {
                    s = AddColumn(ColumnDescriptor(name, ColumnType::FIXED_BYTES).SetFixedLength(25));
                }
            } else if (type == "int") {
                if (isInitWithGroup) {
                    groupColsConfig.AddSubEdgeColumn(ColumnDescriptor(name, ColumnType::INT32));
                } else {
                    s = AddColumn(ColumnDescriptor(name, ColumnType::INT32));
                }
            } else if (type == "float") {
                if (isInitWithGroup) {
                    groupColsConfig.AddSubEdgeColumn(ColumnDescriptor(name, ColumnType::FLOAT));
                } else {
                    s = AddColumn(ColumnDescriptor(name, ColumnType::FLOAT));
                }
            } else if (type == "double") {
                if (isInitWithGroup) {
                    groupColsConfig.AddSubEdgeColumn(ColumnDescriptor(name, ColumnType::DOUBLE));
                } else {
                    s = AddColumn(ColumnDescriptor(name, ColumnType::DOUBLE));
                }
            } else if (type == "byte") {
                if (isInitWithGroup) {
                    groupColsConfig.AddSubEdgeColumn(ColumnDescriptor(name, ColumnType::BYTE));
                } else {
                    s = AddColumn(ColumnDescriptor(name, ColumnType::BYTE));
                }
            } else if (type == "time") {
                if (isInitWithGroup) {
                    groupColsConfig.AddSubEdgeColumn(ColumnDescriptor(name, ColumnType::TIME));
                } else {
                    s = AddColumn(ColumnDescriptor(name, ColumnType::TIME));
                }
            } else {
                return Status::NotImplement(fmt::format("Type error: {}, support type: {}",
                                                        type, SUPPORTED_EDGE_TYPE));
            }
            if (!s.ok()) { return s; }
        }
        if (groupColsConfig.isNonEmptyColumnGroup()) {
            s = AddColumn(groupColsConfig);
            if (!s.ok()) { return s; }
        }
        return s;
    }
#endif


    Status MetaAttributes::UnSerializeRapidJsonValue(const rapidjson::Value &jsonAttributes) {
        if (!jsonAttributes.IsObject()) {
            return Status::InvalidArgument("fail to unserialize properties. Not json object");
        }
        if (!jsonAttributes.HasMember("flags")) {
            m_flags = 0;
        } else if (!jsonAttributes["flags"].IsUint()) {
            return Status::InvalidArgument("fail to unserialize properties. flags is not uint32_t");
        } else {
            m_flags = jsonAttributes["flags"].GetUint();
        }
        if (!jsonAttributes.HasMember("label") || !jsonAttributes["label"].IsString()
            || !jsonAttributes.HasMember("tag") || !jsonAttributes["tag"].IsUint()) {
            return Status::InvalidArgument("fail to unserialize properties. Missing label/tag");
        } else {
            label = jsonAttributes["label"].GetString();
            label_tag = static_cast<EdgeTag_t>(jsonAttributes["tag"].GetUint());
        }
        // 边属性配置, 解析 src/dst 节点的 label
        if (!jsonAttributes.HasMember("src_label") || !jsonAttributes["src_label"].IsString()
                ) {
            return Status::InvalidArgument("fail to unserialize properties. Missing src_label/src_tag");
        } else {
            src_label = jsonAttributes["src_label"].GetString();
        }
        if (jsonAttributes.HasMember("src_tag")) {
            src_tag = static_cast<EdgeTag_t>(jsonAttributes["src_tag"].GetUint());
        }
        if (!jsonAttributes.HasMember("dst_label") || !jsonAttributes["dst_label"].IsString()
                ) {
            return Status::InvalidArgument("fail to unserialize properties. Missing dst_label/dst_tag");
        } else {
            dst_label = jsonAttributes["dst_label"].GetString();
        }
        if (jsonAttributes.HasMember("dst_tag")) {
            dst_tag = static_cast<EdgeTag_t>(jsonAttributes["dst_tag"].GetUint());
        }
        Status s;
        if (!jsonAttributes.HasMember("cols") || !jsonAttributes["cols"].IsArray()) {
            return Status::InvalidArgument("not valid schema");
        } else {
            for (rapidjson::SizeType i = 0; i < jsonAttributes["cols"].Size(); ++i) {
                const auto &col = jsonAttributes["cols"][i];
                ColumnDescriptor config;
                s = ColumnDescriptorUtils::UnSerializeFromRapidJsonValue(col, &config);
                if (!s.ok()) { break; }
                s = this->AddColumn(config);
                if (!s.ok()) { break; }
            }
            if (!s.ok()) { return s; }
        }
        return s;
    }

    const ColumnDescriptor *
    MetaAttributes::InternalGetColumn(
            const ColumnDescriptor &queryCol,
            const ColumnDescriptor &col,
            bool isCheckType) {
        if (col.columnType() == ColumnType::GROUP) {
            // 如果是 ColumnGroup, 则检查Group中的子列
            const ColumnDescriptor *ret = nullptr;
            for (const auto &subCol: col) {
                ret = InternalGetColumn(queryCol, subCol, isCheckType);
                if (ret != nullptr) { return ret; }
            }
            return nullptr;
        } else {
            if (col.colname() == queryCol.colname()) {
                if (!isCheckType) {
                    // 只检查名字相同, 无需类型相同
                    return &col;
                } else {
                    // 名字 && 类型都相同
                    if (col.columnType() == queryCol.columnType()) {
                        return &col;
                    } else {
                        return nullptr;
                    }
                }
            } else {
                return nullptr;
            }
        }
    }

    const ColumnDescriptor *MetaAttributes::GetColumn(const ColumnDescriptor &queryCol, bool isCheckType) const {
        const ColumnDescriptor *ret = nullptr;
        for (size_t i = 0; i < cols.size(); ++i) {
            ret = InternalGetColumn(queryCol, cols[i], isCheckType);
            if (ret != nullptr) { return ret; }
        }
        return nullptr;
    }

    /*Status MetaAttributes::UnSerializeEdgePropertiesXMLString(const std::string &xmlStr) {
        this->Clear();
        Status s;
        tinyxml2::XMLDocument doc;
        tinyxml2::XMLError error = doc.Parse(xmlStr.c_str());
        if (error != tinyxml2::XMLError::XML_SUCCESS) {
            SKG_LOG_ERROR("load xml string failed: {}", doc.ErrorStr());
            return Status::InvalidArgument(fmt::format("load xml string failed: {}", doc.ErrorStr()));
        }
        tinyxml2::XMLElement *entry = doc.RootElement();
        if (entry == nullptr) {
            SKG_LOG_ERROR("get root element error: {}", doc.ErrorStr());
            return Status::InvalidArgument(fmt::format("get root element error: {}", doc.ErrorStr()));
        }
        return UnSerializeEdgePropertiesXMLElement(entry);
    }

    Status MetaAttributes::UnSerializeEdgePropertiesXMLElement(const tinyxml2::XMLElement *entry) {
        assert(entry != nullptr);
        this->Clear();
        Status s;
        const char *s_raw = entry->Attribute("label");
        if (s_raw == nullptr || strlen(s_raw) == 0) {
            return Status::InvalidArgument("edge label entry without `label'");
        }
        label = s_raw;
        // src label, dst label
        // "src_label" / "src_vertex_label"
        s_raw = entry->Attribute("src_label");
        if (s_raw == nullptr || strlen(s_raw) == 0) {
            s_raw = entry->Attribute("src_vertex_label");
            if (s_raw == nullptr || strlen(s_raw) == 0) {
                return Status::InvalidArgument("edge label entry without `src_label'");
            }
        }
        src_label = s_raw;
        // "dst_label" / "dst_vertex_label"
        s_raw = entry->Attribute("dst_label");
        if (s_raw == nullptr || strlen(s_raw) == 0) {
            s_raw = entry->Attribute("dst_vertex_label");
            if (s_raw == nullptr || strlen(s_raw) == 0) {
                return Status::InvalidArgument("edge label entry without `dst_label'");
            }
        }
        dst_label = s_raw;

        if (entry->Attribute("is-weighted") == nullptr) {
            m_flags &= (~FLAG_IS_WEIGHTED); // 清空标识位
        } else if (entry->Attribute("is-weighted", "true")) {
            m_flags |= FLAG_IS_WEIGHTED; // 设置标识位
        } else if (entry->Attribute("is-weighted", "false")) {
            m_flags &= (~FLAG_IS_WEIGHTED); // 清空标识位
        } else {
            return Status::InvalidArgument("is-weighted not valid."
                                           " set is-weighted=\"true\" or \"false\"");
        }

        const tinyxml2::XMLElement *properties = entry->FirstChildElement("properties");
        if (properties == nullptr) {
            // warning 无属性
            SKG_LOG_WARNING("creating edge of label: `{}' without properties.", label);
        } else {
            SKG_LOG_TRACE("    properties:", "");
            s = this->UnSerializeProperties(properties);
            if (!s.ok()) { return s; }
        }
        return s;
    }

    Status MetaAttributes::UnSerializeVertexPropertiesXMLString(const std::string &xmlStr) {
        this->Clear();
        Status s;
        tinyxml2::XMLDocument doc;
        tinyxml2::XMLError error = doc.Parse(xmlStr.c_str());
        if (error != tinyxml2::XMLError::XML_SUCCESS) {
            SKG_LOG_ERROR("load xml string failed: {}", doc.ErrorStr());
            return Status::InvalidArgument(fmt::format("load xml string failed: {}", doc.ErrorStr()));
        }
        tinyxml2::XMLElement *entry = doc.RootElement();
        if (entry == nullptr) {
            SKG_LOG_ERROR("get root element error: {}", doc.ErrorStr());
            return Status::InvalidArgument(fmt::format("get root element error: {}", doc.ErrorStr()));
        }
        return UnSerializeVertexPropertiesXMLElement(entry);
    }

    Status MetaAttributes::UnSerializeVertexPropertiesXMLElement(const tinyxml2::XMLElement *entry) {
        assert(entry != nullptr);
        this->Clear();
        Status s;
        const char *s_raw = entry->Attribute("label");
        if (s_raw == nullptr || strlen(s_raw) == 0) {
            return Status::InvalidArgument("vertex label entry without `label'");
        }
        label = s_raw;

        const tinyxml2::XMLElement *properties = entry->FirstChildElement("properties");
        if (properties == nullptr) {
            // warning 无属性
            SKG_LOG_WARNING("creating vertex of label: `{}' without properties.", label);
        } else {
            SKG_LOG_TRACE("    properties:", "");
            s = UnSerializeProperties(properties);
            if (!s.ok()) { return s; }
        }
        return s;
    }

    Status MetaAttributes::UnSerializeProperties(const tinyxml2::XMLElement *properties) {
        assert(properties != nullptr);
        Status s;
        const char *s_raw, *s_name;
        std::string type;
        for (const tinyxml2::XMLElement *property = properties->FirstChildElement("property");
             property != nullptr;
             property = property->NextSiblingElement("property")) {
            type.clear();
            s_raw = property->Attribute("name");
            if (s_raw == nullptr || strlen(s_raw) == 0) {
                return Status::InvalidArgument("vertex property without `name'");
            }
            s_name = s_raw;
            s_raw = property->Attribute("type");
            if (s_raw == nullptr || strlen(s_raw) == 0) {
                return Status::InvalidArgument("vertex property without `type'");
            }
            type = s_raw;

            size_t max_length = 25; // 默认最大长度为25个字节
            if (property->Attribute("max-length") != nullptr) {
                // 用户定义的最大长度
                max_length = StringUtils::ParseUint32(property->Attribute("max-length"), &s);
                if (!s.ok()) {// 解析失败, 默认25字节
                    max_length = 25;
                    s = Status::OK();
                }
            }

            // TODO GroupColumn
            if (type == "string") {
                s = this->AddColumn(
                        ColumnDescriptor(s_name, ColumnType::FIXED_BYTES).SetFixedLength(max_length));
            } else if (type == "char-array") {
                s = this->AddColumn(
                        ColumnDescriptor(s_name, ColumnType::FIXED_BYTES).SetFixedLength(max_length));
            } else if (type == "int") {
                s = this->AddColumn(ColumnDescriptor(s_name, ColumnType::INT32));
            } else if (type == "float") {
                s = this->AddColumn(ColumnDescriptor(s_name, ColumnType::FLOAT));
            } else if (type == "double") {
                s = this->AddColumn(ColumnDescriptor(s_name, ColumnType::DOUBLE));
            } else if (type == "time") {
                s = this->AddColumn(ColumnDescriptor(s_name, ColumnType::TIME));
            } else if (type == "varchar") {
                s = this->AddColumn(ColumnDescriptor(s_name, ColumnType::VARCHAR));
            } else {
                // TODO 返回错误
                return Status::NotImplement(fmt::format("Type error: {}", type));
            }
            if (!s.ok()) { return s; }
        }
        return s;
    }
    */

    void MetaAttributes::Clear() {
        label.clear();
        label_tag = 0;
        src_label.clear();
        dst_label.clear();
        m_flags = 0x0;
        cols.clear();
    }

}
