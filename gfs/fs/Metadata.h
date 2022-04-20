#ifndef STARKNOWLEDGEGRAPH_METADATA_HPP
#define STARKNOWLEDGEGRAPH_METADATA_HPP

#include <string>
#include <fstream>
#include <sstream>
#include <rapidjson/reader.h>
#include <rapidjson/writer.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/filewritestream.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include "util/skgfilenames.h"
#include "ColumnDescriptor.h"
#include "MetaHeterogeneousAttributes.h"
#include "MetaPartition.h"
//#include "MetaJournal.h"
#include "env/env.h"

namespace skg {

class MetaNumVertices {
public:
    // 已分配的最大节点 id
    vid_t max_allocated_vid;
    // 磁盘上存储节点 id 的 capacity
    vid_t storage_capacity_vid;
    // 有效的节点数量 FIXME 未实际使用, 在 id-encoder 为 Long 型 DB, 插入 vid=1亿 场景下, 仍会出现不符合预期的情况
    vid_t num_vertices;
public:
    MetaNumVertices() : max_allocated_vid(0), storage_capacity_vid(0), num_vertices(0) {
    }
};

    class MetadataFileHandler {
    public:
        static
        Status IsFilesExist(const std::string &dirname) {
            std::string filename;
            filename = FILENAME::intervals(DIRNAME::meta(dirname));
            if (!PathUtils::FileExists(filename)) {
                return Status::NotExist(fmt::format("db metadata: {}", filename));
            }
            filename = FILENAME::num_vertices(DIRNAME::meta(dirname));
            if (!PathUtils::FileExists(filename)) {
                return Status::NotExist(fmt::format("db metadata: {}", filename));
            }
            filename = FILENAME::vertex_attr_conf(DIRNAME::meta(dirname));
            if (!PathUtils::FileExists(filename)) {
                return Status::NotExist(fmt::format("db metadata: {}", filename));
            }
            filename = FILENAME::edge_attr_conf(DIRNAME::meta(dirname));
            if (!PathUtils::FileExists(filename)) {
                return Status::NotExist(fmt::format("db metadata: {}", filename));
            }
            return Status::OK();
        }

    public:

        static
        Status CreateMetaDirIfMissing(const std::string &dirname) {
            return PathUtils::CreateDirIfMissing(DIRNAME::meta(dirname));
        }

        static
        Status WriteLSMIntervals(const std::string &dirname, const MetaShardInfo &shard_info) {
            const std::string filename = FILENAME::intervals(DIRNAME::meta(dirname));
            FILE *f = fopen(filename.c_str(), "w");
            if (f == nullptr) {
                const std::string errMsg = fmt::format("Could not open file: {} error: {}",
                                                       filename, strerror(errno));
                SKG_LOG_ERROR("{}", errMsg);
                assert(f != nullptr);
                return Status::IOError(errMsg);
            }

            Status s;
            char writeBuff[4096];
            rapidjson::FileWriteStream stream(f, writeBuff, sizeof(writeBuff));
            rapidjson::PrettyWriter<rapidjson::FileWriteStream> writer(stream);
            writer.SetIndent(' ', 2);

            s = shard_info.Serialize(writer);
            stream.Put('\n');
            stream.Flush();
            fclose(f);
            return s;
        }

        static
        Status ReadLSMIntervals(const std::string &dirname, MetaShardInfo *shard_info) {
            const std::string filename = FILENAME::intervals(DIRNAME::meta(dirname));
            return shard_info->ParseFromFile(filename);
        }

        static
        Status ReadShardTreeIntervals(const std::string &treedirname, MetaPartition *shard_partitions) {
            const std::string filename = FILENAME::intervals(DIRNAME::meta(treedirname));
            return shard_partitions->ParseFromFile(filename);
        }

        static 
        Status ReadShardTreeIntervals(const std::string &dirname, uint32_t shard_id, MetaPartition *shard_partitions) {
            const std::string treedirname = DIRNAME::shardtree(dirname, shard_id); 
            return ReadShardTreeIntervals(treedirname, shard_partitions);
        }

        static
        Status WriteShardTreeIntervals(const std::string &dirname, const MetaPartition &shard_partitions) {
            const std::string filename = FILENAME::intervals(DIRNAME::meta(dirname));
            rapidjson::StringBuffer sb;
            rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
            writer.SetIndent(' ', 2);

            Status s = shard_partitions.Serialize(writer, true);
            if (!s.ok()) {
                return s;
            } else {
                return WriteStringToFile(Env::Default(), sb.GetString(), filename, true);
            }
        }

        static
        Status ReadNumVertices(const std::string &dirname,
                               MetaNumVertices *const vertices_info) {
            assert(vertices_info != nullptr);
            const std::string filename = FILENAME::num_vertices(DIRNAME::meta(dirname));
            std::ifstream inf(filename);
            if (!inf.good()) {
                SKG_LOG_ERROR("Could not load {}. error: {}", filename, strerror(errno));
                assert(inf.good());
                return Status::FileNotFound(filename);
            }

            vertices_info->num_vertices = static_cast<vid_t>(-1);
            inf >> vertices_info->max_allocated_vid >> vertices_info->storage_capacity_vid;
            // TODO Try to load third number: num_vertices
            inf >> vertices_info->num_vertices;
            if (vertices_info->num_vertices == static_cast<vid_t>(-1)) {
                // if can not load, rollback
                vertices_info->num_vertices = vertices_info->max_allocated_vid + 1;
            }
            return Status::OK();
        }

        static
        Status WriteNumVertices(const std::string &dirname, const MetaNumVertices &vertices_info) {
            const std::string filename = FILENAME::num_vertices(DIRNAME::meta(dirname));
            std::string data = fmt::format("{} {} {}\n",
                    vertices_info.max_allocated_vid,
                    vertices_info.storage_capacity_vid,
                    vertices_info.num_vertices);
            return WriteStringToFile(Env::Default(), data, filename, /*should_sync*/true);
        }

	/*
        static
        Status ReadMetaJournal(const std::string &dirname, MetaJournal *journal) {
            assert(journal != nullptr);
            const std::string filename = FILENAME::meta_journal(DIRNAME::meta(dirname));
            std::string data;
            Status s=Env::Default()->FileExists(filename);
            if ( s.IsFileNotFound() )
	    {
                return Status::FileNotFound();
	    }
            s = ReadFileToString(Env::Default(), filename, &data);
            if (!s.ok()) { return s; }
            std::stringstream ss(data);
            ss >> journal->m_last_sequence >> journal->m_log_number >> journal->m_prev_log_number;
            return s;
        }
	*/

	/*
        static
        Status WriteMetaJournal(const std::string &dirname, const MetaJournal &metaJournal) {
            const std::string filename = FILENAME::meta_journal(DIRNAME::meta(dirname));
            std::string data = fmt::format("{} {} {}\n",
                    metaJournal.m_last_sequence,
                    metaJournal.m_log_number,
                    metaJournal.m_prev_log_number);
            return WriteStringToFile(Env::Default(), data, filename, *//*should_sync*//*true)
        }*/

        static
        Status WriteEdgeAttrConf(const std::string &dirname, const MetaHeterogeneousAttributes &confs) {
            Status s;
            const std::string filename = FILENAME::edge_attr_conf(DIRNAME::meta(dirname));
            // TODO 错误处理: fopen 返回 nullptr, fprintf 返回错误
            FILE *f = fopen(filename.c_str(), "wb");
            if (f == nullptr) {
                SKG_LOG_ERROR("Could not open file: {} error: {}", filename, strerror(errno));
                assert(f != nullptr);
                return Status::IOError();
            }
            char writeBuff[4096];
            rapidjson::FileWriteStream stream(f, writeBuff, sizeof(writeBuff));
            rapidjson::PrettyWriter<rapidjson::FileWriteStream> writer(stream);
            writer.SetIndent(' ', 2);

            s = confs.Serialize(writer);
            stream.Put('\n');
            stream.Flush();
            fclose(f);
            return s;
        }

        static 
        Status ReadEdgeAttrConf(const std::string &dirname, MetaHeterogeneousAttributes *confs) {
            const std::string filename = FILENAME::edge_attr_conf(DIRNAME::meta(dirname));
            return confs->ParseFromFile(filename);
        }

        static
        Status ReadVertexAttrConf(const std::string &dirname, MetaHeterogeneousAttributes *confs) {
            const std::string filename = FILENAME::vertex_attr_conf(DIRNAME::meta(dirname));
            return confs->ParseFromFile(filename);
        }

        static
        Status WriteVertexAttrConf(const std::string &dirname, const MetaHeterogeneousAttributes &confs) {
            Status s;
            const std::string filename = FILENAME::vertex_attr_conf(DIRNAME::meta(dirname));
            // TODO 错误处理: fopen 返回 nullptr, fprintf 返回错误
            FILE *f = fopen(filename.c_str(), "wb");
            if (f == nullptr) {
                SKG_LOG_ERROR("Could not open file: {} error: {}", filename, strerror(errno));
                assert(f != nullptr);
                return Status::IOError();
            }
            char writeBuff[4096];
            rapidjson::FileWriteStream stream(f, writeBuff, sizeof(writeBuff));
            rapidjson::PrettyWriter<rapidjson::FileWriteStream> writer(stream);
            writer.SetIndent(' ', 2);

            s = confs.Serialize(writer);
            stream.Put('\n');
            stream.Flush();
            fclose(f);
            return s;
        }

    private:
        MetadataFileHandler() = delete;
    };

}

#endif //STARKNOWLEDGEGRAPH_METADATA_HPP
