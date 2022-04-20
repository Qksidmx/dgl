#include <env/env.h>
#include "MetaPartition.h"

namespace skg {
    Status MetaPartition::ParseFromRapidJsonValue(const rapidjson::Value &jsonPartition) {
        Status s;
        children.clear();
        if (!jsonPartition.IsObject()
            || !jsonPartition.HasMember("interval")
            || !jsonPartition.HasMember("id")) {
            s = Status::InvalidArgument("invalid meta partition");
            return s;
        }
        interval.ParseFromRapidJsonValue(jsonPartition["interval"]);
        id = jsonPartition["id"].GetUint();
        if (jsonPartition.HasMember("children")) {
            const rapidjson::Value &jsonChildren = jsonPartition["children"];
            if (!jsonChildren.IsArray()) {
                return Status::InvalidArgument("invalid children");
            }
            for (size_t i = 0; i < jsonChildren.Size(); ++i) {
                MetaPartition child;
                s = child.ParseFromRapidJsonValue(jsonChildren[i]);
                if (!s.ok()) { return s; }
                children.emplace_back(child);
            }
        }
        return s;
    }

    Status MetaPartition::ParseFromFile(const std::string &filename) {
        children.clear();
        std::string data;
        Status s = ReadFileToString(Env::Default(), filename, &data);
        if (s.ok()) {
            rapidjson::Document d;
            // 解析错误处理
            // https://github.com/Tencent/rapidjson/blob/master/doc/dom.zh-cn.md#%E8%A7%A3%E6%9E%90%E9%94%99%E8%AF%AF-parseerror
            if (d.Parse(data.c_str()).HasParseError()) {
                s = Status::InvalidArgument(data);
                return s;
            }
            s = this->ParseFromRapidJsonValue(d.GetObject());
        }
        return s;
    }

    bool MetaPartition::operator==(const MetaPartition &rhs) const {
        if (this->interval != rhs.interval || this->id != rhs.id) {
            return false;
        } else {
            if (children.size() != rhs.children.size()) {
                return false;
            }
            for (size_t i = 0; i < children.size(); ++i) {
                if (children[i] != rhs.children[i]) { // 递归判断是否相等
                    return false;
                }
            }
            return true;
        }
    }

    Status MetaShardInfo::ParseFromFile(const std::string &filename) {
        roots.clear();
        std::string data;
        Status s = ReadFileToString(Env::Default(), filename, &data);
        if (s.ok()) {
            rapidjson::Document d;
            // 解析错误处理
            // https://github.com/Tencent/rapidjson/blob/master/doc/dom.zh-cn.md#%E8%A7%A3%E6%9E%90%E9%94%99%E8%AF%AF-parseerror
            if (d.Parse(data.c_str()).HasParseError()) {
                s = Status::InvalidArgument(data);
                return s;
            }
            if (!d.IsArray()) {
                s = Status::InvalidArgument("not json array");
                return s;
            }
            for (rapidjson::SizeType i = 0; i < d.Size(); ++i) {
                MetaPartition partition;
                partition.ParseFromRapidJsonValue(d[i]);
                roots.emplace_back(partition);
            }
        } else {
            SKG_LOG_DEBUG("status not ok at filename:{}", filename);
        }
        return s;
    }
}
