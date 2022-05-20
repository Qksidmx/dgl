#include <string>
#include <vector>

#include "fs/skgfs.h"
#include "fs/ShardTree.h"
//#include "fs/log_writer.h"
#include "fs/VertexColumnList.h"
#include "util/status.h"
#include "util/options.h"

#include "env/env.h"
#include "IDEncoder.h"
//#include "idencoder/RocksDBIdEncoder.h"
//#include "idencoder/StringToLongIdEncoder.h"
#include "StringToLongIdEncoder.h"
#include "fs/SkgDBImpl.h"
//#include "dbquery/SkgDDBImp.hpp"
//#include "dbquery/SkgSocket.hpp"
//#include "dbengine/CtrlCommandClient.h"
#include "util/file_reader_writer.h"
#include "fs/Metadata.h"
//#include "fs/MetaJournal.h"

namespace skg {
    Status SkgDB::Create(const std::string &name, const Options &options) {
        // 检查 db-name 不含特殊字符
        if (!StringUtils::IsValidName(name)) {
            return Status::InvalidArgument(fmt::format("db-name: `{}' contains special chars", name));
        }
        Status s;
        const std::string dir = options.GetDBDir(name);
	SKG_LOG_DEBUG("DB create dir: {}", dir);

        // 创建数据库元数据
        s = MetadataFileHandler::CreateMetaDirIfMissing(dir);
        if (!s.ok()) { return s; }

        // 边的属性列
        MetaHeterogeneousAttributes hetEProperties;
        s = MetadataFileHandler::WriteEdgeAttrConf(dir, hetEProperties);
        if (!s.ok()) { return s; }

        // 节点数
        const vid_t max_vertex_id = 0;
        MetaHeterogeneousAttributes hetVProperties;
        s = VertexColumnList::Create(dir, hetVProperties, max_vertex_id);
        if (!s.ok()) { return s; }

        // 创建节点 string -> int 转换 mapping
        std::shared_ptr<IDEncoder> encoder;
        // 节点 string -> int 转换
        switch (options.id_type) {
            case Options::VertexIdType::STRING:{
                SKG_LOG_ERROR("id-encoder: {} not available.", "RocksDBIdEncoder");
                //encoder = std::make_shared<RocksDBIdEncoder>( options.id_convert_cache_mb, options.id_convert_num_hot_key);
                encoder = std::make_shared<StringToLongIdEncoder>();
                break;
            }
            case Options::VertexIdType::LONG: {// LONG 型, 不创建 id_mapp
                SKG_LOG_DEBUG("id-encoder: {}", "StringToLongIdEncoder");
                encoder = std::make_shared<StringToLongIdEncoder>();
                break;
            }
        }
        s = encoder->Open(dir, IDEncoder::OpenMode::READ_WRITE);
        if (!s.ok()) { return s; }

        // 创建空的 shard
        MetaShardInfo forest_info;
        forest_info.roots.emplace_back(MIN_SHARD_ID, interval_t(0, 0));
        s = MetadataFileHandler::WriteLSMIntervals(dir, forest_info);
        if (!s.ok()) { return s; }

        for (const auto &shard: forest_info.roots) {
            s = ShardTree::Create(dir, shard.id, shard, hetEProperties);
            if (!s.ok()) { return s; }
        }

        // 日志信息
	/*
        Options journal_options = options;
        {// 根据选项设置 WAL 的目录
            if (options.wal_dir.empty()) {
                journal_options.wal_dir = DIRNAME::default_wal_dir(dir);
            }
            s = Env::Default()->CreateDirIfMissing(journal_options.wal_dir, true);
            if (!s.ok()) { return s; }
        }
        MetaJournal meta_journal;
        meta_journal.m_last_sequence = 0;
        meta_journal.m_log_number = 1;
        meta_journal.m_prev_log_number = 0;
        s = MetadataFileHandler::WriteMetaJournal(dir, meta_journal);
        {
            // 创建新的恢复日志句柄
            uint64_t log_file_no = meta_journal.m_log_number;
            std::unique_ptr<WritableFile> log_file;
            EnvOptions env_options; // TODO 从 db_options 中获取配置项
            env_options.use_clib_writes = false; // 不使用 CLib 中缓冲的 FILE 句柄
            env_options.use_mmap_writes = false; // 不使用 MMap
            env_options.use_direct_writes = true;
            assert(!journal_options.wal_dir.empty());
            s = Env::Default()->NewWritableFile(
                    FILENAME::journal_name(journal_options.wal_dir, log_file_no), &log_file, env_options);
            if (s.ok()) {
                std::unique_ptr<WritableFileWriter> file_writer(
                        new WritableFileWriter(std::move(log_file), env_options));
                std::unique_ptr<skg::log::Writer> log_writer =
                        std::unique_ptr<skg::log::Writer>(
                                new skg::log::Writer(std::move(file_writer), log_file_no, false));
            }
        }
	*/

        return s;
    }


    /*Status SkgDB::CreateRemote(const std::string &name, const Options &options) {
        // 检查 db-name 不含特殊字符
        if (!StringUtils::IsValidName(name)) {
            return Status::InvalidArgument(fmt::format("db-name: `{}' contains special chars", name));
        }
        if(name.empty()){
            return Status::InvalidArgument(fmt::format("db-name is empty"));
        }
        Status s;
        do {
            SkgDDBImp *impl = new SkgDDBImp(name,options.master_ip,options.master_port,options.socket_timeout);
            SkgSocket udpSocket(impl->m_master_ip,impl->m_master_port,"udp",impl->m_timeout);

            //发请求给master执行静态导入功能
            request_ddbquery request;
            respond_ddbquery respond;

            //设置命令字
            request.set_cmd(CREAT_DATABASE);

            //设置bulkload info信息
            db_info *info = request.mutable_db();
            info->set_db_name(name);
            //其他信息
                        
            //执行请求
            s = impl->m_transmitor.SendRecv(udpSocket.m_sockfd,request, respond,&udpSocket.m_addr);
            if (!s.ok()) { break; }

            //取数据
            std::cout << respond.DebugString() << std::endl;

            if (!respond.status().ok()) {
                s = Status::NotImplement(
                        fmt::format("error code:{},error message:{}",
                                    respond.status().error_code(),
                                    respond.status().error_message()));
                break;
            }
        } while (false);
        return s;
    }*/

    /*Status
    SkgDB::BuildFromFile(const std::string &name,
                         const Options &options,
                         const std::string &config) {
        if (!StringUtils::IsValidName(name)) {
            return Status::InvalidArgument(fmt::format("db-name: `{}' contains special chars", name));
        }
        Status s;
        request_ddbquery request;
        //设置命令字
        request.set_cmd(BULKLOAD);

        //设置bulkload info信息
        request_bulkload_info *info = request.mutable_bulkload();
        info->set_db_name(name);
        info->set_user_id(options.user_id);
        info->set_cluster_id(options.cluster_id);
        info->set_config(config);
        // FIXME 其他配置项
        auto *pb_options = info->mutable_options();
        pb_options->set_force_create(options.force_create);
        pb_options->set_shard_size_mb(options.shard_size_mb);
        pb_options->set_shard_init_per(options.shard_init_per);
        pb_options->set_shard_split_factor(options.shard_split_factor);
        pb_options->set_sample_rate(options.sample_rate);
        pb_options->set_sample_interval(options.sample_interval);
        pb_options->set_sample_seed(options.sample_seed);
        pb_options->set_parallelism_per_bulkload_worker(options.parallelism_per_bulkload_worker);
        pb_options->set_bulkload_router_recv_inproc_timeout_ms(options.bulkload_router_recv_inproc_timeout_ms);
        pb_options->set_use_elias_gamma_compress(options.use_elias_gamma_compress);

        CtrlCommandClient client;
        s = client.Connect(500);
        if (!s.ok()) { return s; }
        SKG_LOG_INFO("connec ok, submit job {}, pb:{}", name, request.ShortDebugString());
        s = client.SubmitJob(request);
        if (!s.ok()) { return s; }

        return s;
    }*/
    
    /*Status 
    SkgDB::BuildFromFileRemote(
            const std::string &name, 
            const skg::Options &options,
            const std::string &config)
    {
        //没必要绕一圈，直接给control发起静态导入请求
        return BuildFromFile(name,options,config);
        // 检查 db-name 不含特殊字符
        if (!StringUtils::IsValidName(name)) {
            return Status::InvalidArgument(fmt::format("db-name: `{}' contains special chars", name));
        }
        if(name.empty()){
            return Status::InvalidArgument(fmt::format("db-name is empty"));
        }
        Status s;
        SkgDDBImp *impl = new SkgDDBImp(name,options.master_ip,options.master_port,options.socket_timeout);
        SkgSocket udpSocket(impl->m_master_ip,impl->m_master_port,"udp",impl->m_timeout);
        do {

            //发请求给master执行静态导入功能
            request_ddbquery request;
            respond_ddbquery respond;

            //设置命令字
            request.set_cmd(BULKLOAD);

            //设置bulkload info信息
            request_bulkload_info *info = request.mutable_bulkload();
            info->set_db_name(name);
            info->set_user_id(options.user_id);
            info->set_cluster_id(options.cluster_id);
            info->set_config(config);
            // FIXME 其他配置项
            auto *pb_options = info->mutable_options();
            pb_options->set_force_create(options.force_create);
            pb_options->set_shard_size_mb(options.shard_size_mb);
            pb_options->set_shard_init_per(options.shard_init_per);
            pb_options->set_shard_split_factor(options.shard_split_factor);
            pb_options->set_sample_rate(options.sample_rate);
            pb_options->set_sample_interval(options.sample_interval);
            pb_options->set_sample_seed(options.sample_seed);
            pb_options->set_parallelism_per_bulkload_worker(options.parallelism_per_bulkload_worker);
            pb_options->set_bulkload_router_recv_inproc_timeout_ms(options.bulkload_router_recv_inproc_timeout_ms);
            pb_options->set_use_elias_gamma_compress(options.use_elias_gamma_compress);

            //执行请求
            s = impl->m_transmitor.SendRecv(udpSocket.m_sockfd,request, respond, &udpSocket.m_addr);
            if (!s.ok()) { break; }

            //取数据
            std::cout << respond.DebugString() << std::endl;

            if (!respond.status().ok()) {
                s = Status::NotImplement(
                        fmt::format("error code:{},error message:{}",
                                    respond.status().error_code(),
                                    respond.status().error_message()));
                break;
            }
        } while (false);


        delete impl;
        
        return s;
    }*/

    Status SkgDB::Open(const std::string &name, const Options &options, SkgDB **pDB) {
        // 检查 db-name 不含特殊字符
        if (!StringUtils::IsValidName(name)) {
            return Status::InvalidArgument(fmt::format("db-name: `{}' contains special chars", name));
        }
        SkgDBImpl *impl = new SkgDBImpl(name, options);
        // 打开过程中锁, 不让进行更新操作. TODO 读操作也禁止? 否则一个线程 Open, 另外一个线程已经尝试开始读/写的情况
        std::lock_guard<std::mutex> lock(impl->m_write_lock);
        const std::string basedir = impl->GetStorageDirname();
        SKG_LOG_DEBUG("Opening {} with mem-table: {}, buff: {}mb", name, static_cast<int>(options.mem_table_type), options.mem_buffer_mb);
        Status s;
#if 0
        {
            impl->m_name = name;
            s = Env::Default()->FileExists(basedir);
            if (!s.ok()) {
                if (s.IsFileNotFound()) {
                    s = Status::NotExist(fmt::format("db-name: `{}' disk file cannot be created.", name));
		    return s;
                }
            }

	    /*
            {// 根据选项设置 WAL 的目录
                if (impl->m_options.wal_dir.empty()) {
                    impl->m_options.wal_dir = DIRNAME::default_wal_dir(basedir);
                }
                s = Env::Default()->CreateDirIfMissing(impl->m_options.wal_dir, true);
                if (!s.ok()) { break; }
            }
	    */

            // 恢复数据库元数据信息
            //s = impl->Recover();
	    // 节点 string -> int 转换
	    impl->m_options.id_type = Options::VertexIdType::LONG;
	    impl->m_id_encoder = std::make_shared<StringToLongIdEncoder>();
            s = impl->m_id_encoder->Open(basedir);
            if (!s.ok()) { 
                    s = Status::NotExist(fmt::format("Encoder cannot be created.", name));
		    return s;
	    }
	    // 节点相关的操作句柄
	    s = VertexColumnList::Open(basedir, &(impl->m_vertex_columns));
            if (!s.ok()) { 
                    s = Status::NotExist(fmt::format("Vertex storage cannot be created.", name));
		    return s;
	    }
	}
#endif
	//the following code are extracted from SkgDBImpl_Recover.cc::26
        s = MetadataFileHandler::IsFilesExist(basedir);
        if (!s.ok()) { return s; }

        // 异构边属性列的元数据
        s = MetadataFileHandler::ReadEdgeAttrConf(basedir, &(impl->m_edge_attr));
        if (!s.ok()) 
	{
            return Status::Corruption("edge properties:" + s.ToString());
        }

        MetaShardInfo meta_shard_info;
	s = MetadataFileHandler::ReadLSMIntervals(basedir, &meta_shard_info);
        if (!s.ok()) {
            return Status::Corruption("intervals:" + s.ToString());
        }
        s = impl->RecoverHandlers(meta_shard_info);
        if (s.ok()) {
            *pDB = impl;
        } else {
            delete impl;
            *pDB = nullptr;
        }
        return s;
    }


    /*Status SkgDB::OpenRemote(const std::string &name, const Options &options, SkgDB **pDB) {
        // 检查 db-name 不含特殊字符
        if (!StringUtils::IsValidName(name) || name.empty()) {
            return Status::InvalidArgument(fmt::format("db-name: `{}' contains special chars", name));
        }
        if(name.empty()){
            return Status::InvalidArgument(fmt::format("db-name is empty"));
        }
        Status s;
        SkgDDBImp *impl = new SkgDDBImp(name,options.master_ip,options.master_port,options.socket_timeout);
        SkgSocket udpSocket(impl->m_master_ip,impl->m_master_port,"udp",impl->m_timeout);
        do {
            //发请求给master执行打开数据库操作
            request_ddbquery request;
            respond_ddbquery respond;

            //设置命令字
            request.set_cmd(OPEN_DATABASE);

            //设置db信息
            db_info *dbinfo = request.mutable_db();
            dbinfo->set_db_name(impl->m_dbname);

            //执行请求
            s = impl->m_transmitor.SendRecv(udpSocket.m_sockfd,request,respond,&udpSocket.m_addr);
            if (!s.ok()) { break; }

            //取数据
            std::cout << respond.DebugString() << std::endl;

            if (!respond.status().ok()) {
                s = Status::NotImplement(
                        fmt::format("error code:{},error message:{}",
                                    respond.status().error_code(),
                                    respond.status().error_message()));
                break;
            }
        } while (false);

        if (s.ok()) {
            *pDB = impl;
        } else {
            delete impl;
        }
        return s;
    }
*/
}
