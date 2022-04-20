#ifndef STARKNOWLEDGEGRAPHDATABASE_METAPARTITION_H
#define STARKNOWLEDGEGRAPHDATABASE_METAPARTITION_H

#include <rapidjson/document.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/error/error.h>
#include <rapidjson/error/en.h>

#include "util/status.h"

#include "util/internal_types.h"

namespace skg {

    struct MetaPartition {

        explicit MetaPartition(uint32_t id_, const interval_t &interval_)
                : id(id_), interval(interval_), children() {
        }

        explicit MetaPartition(): MetaPartition(0, interval_t(0, 0)) {
        }

        uint32_t id;
        interval_t interval;
        std::vector<MetaPartition> children;

        template<typename Writer>
        Status Serialize(Writer &writer, bool is_root) const;

        Status ParseFromFile(const std::string &filename);

        vid_t get_vid_end() {
            return interval.second;
        }

        size_t get_descendant_num() const {
            size_t ret = children.size();
            for (size_t i = 0; i < children.size(); i ++) {
                ret += children[i].get_descendant_num();
            }
            return ret;
        }

        void get_descendant_interval(std::vector<interval_t>& interval_vec) const {
            //do not clear interval_vec
            for (size_t i = 0; i < children.size(); i ++) {
                interval_vec.push_back(children[i].interval);
                children[i].get_descendant_interval(interval_vec);
            }
        }

        void get_descendant_id(std::vector<uint32_t>& part_id_vec) {
            for (size_t i = 0; i < children.size(); i ++) {
                part_id_vec.push_back(children[i].id);
                children[i].get_descendant_id(part_id_vec);
            }
        }

        void get_all_meta(std::vector<interval_t>& interval_vec,
                          std::vector<uint32_t>& part_id_vec) {
            interval_vec.push_back(interval);
            get_descendant_interval(interval_vec);
            part_id_vec.push_back(id);
            get_descendant_id(part_id_vec);
        }

    private:
        Status ParseFromRapidJsonValue(const rapidjson::Value &jsonPartition);

        // for call ParseFromRapidJsonValue
        friend class MetaShardInfo;

    public:
        bool operator==(const MetaPartition &rhs) const;

        inline bool operator!=(const MetaPartition &rhs) const {
            return !(*this == rhs);
        }
    };

    template<typename Writer>
    Status MetaPartition::Serialize(Writer &writer, bool is_root) const {
        Status s;
        writer.StartObject();

        writer.Key("id");
        if (is_root) { // shard-tree 内, 根节点的 id 为 0
            writer.Uint(0);
        } else {
            writer.Uint(id);
        }

        writer.Key("interval");
        interval.Serialize(writer);

        if (!children.empty()) {
            writer.Key("children");
            writer.StartArray();
            for (const auto &partition: children) {
                s = partition.Serialize(writer, false);
                if (!s.ok()) { return s; }
            }
            writer.EndArray();
        }

        writer.EndObject();
        return s;
    }


    struct MetaShardInfo {
        std::vector<MetaPartition> roots;

        template <typename Writer>
        Status Serialize(Writer &writer) const;

        Status ParseFromFile(const std::string &filename);

        inline
        bool operator==(const MetaShardInfo &rhs) const {
            if (roots.size() != rhs.roots.size()) {
                return false;
            }
            for (size_t i = 0; i < roots.size(); ++i) {
                if (roots[i] != rhs.roots[i]) {
                    return false;
                }
            }
            return true;
        }

        inline bool operator!=(const MetaShardInfo &rhs) const {
            return !(*this == rhs);
        }

        /* deprecated
        void get_sub_interval_vec(size_t tree_id, std::vector<interval_t>& sub_interval_vec) {
            sub_interval_vec.push_back(roots[tree_id].interval);
            roots[tree_id].get_descendant_interval(sub_interval_vec);
        }   
        */

        size_t get_shard_num() {
            return roots.size();
        }

        interval_t get_shard_interval(size_t tree_id) {
            for (size_t i = 0; i < roots.size(); i ++) {
                if ( roots[i].id == tree_id) {
                    return roots[i].interval;
                }
            }
            assert(false);
            return roots[0].interval;
        }   

        size_t get_tree_num() {
            return roots.size();
        }

        size_t get_max_vid() {
            assert(roots.size() > 0);
            return (*(roots.rbegin())).get_vid_end();
        }

        size_t get_vertex_num() {
            return get_max_vid() + 1;
        }

        /*
        size_t get_forest_node_num() {
            size_t ret = roots.size();
            for (size_t i = 0; i < roots.size(); i ++) {
                ret += roots[i].get_descendant_num();
            }            

            return ret;
        }
        */

    };

    template<typename Writer>
    Status MetaShardInfo::Serialize(Writer &writer) const {
        Status s;
        writer.StartArray();
        for (const auto &p: roots) {
            s = p.Serialize(writer, false);
            if (!s.ok()) { return s; }
        }
        writer.EndArray();
        return s;
    }

}

#endif //STARKNOWLEDGEGRAPHDATABASE_METAPARTITION_H
