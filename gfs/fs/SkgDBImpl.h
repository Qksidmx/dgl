#ifndef STARKNOWLEDGEGRAPHDATABASE_SKGDBIMPL_H
#define STARKNOWLEDGEGRAPHDATABASE_SKGDBIMPL_H

#include "fs/skgfs.h"

#include <mutex>

#include "ShardTree.h"
#include "VertexColumnList.h"
#include "util/internal_types.h"
#include "metrics/metrics.hpp"
#include "util/ThreadPool.h"
//#include "log_writer.h"
//#include "version.h"

namespace skg {
    class SkgDBImpl : public SkgDB {
    private:
        explicit SkgDBImpl(const std::string &name, const Options &options)
                : m_closed(false),
                  m_name(name), m_options(options),
                  m_trees(),
                  m_query_pool(options.query_threads) {
        }

        Status Drop(const std::set<std::string> &ignore) override;

        Status Close() override;
    public:
        ~SkgDBImpl() override = default;

    public:

        Status RecoverHandlers(const MetaShardInfo &meta_shard_info);

        /* =============================
         *     从节点出发的增/删/查/改
         *     增/改: -> SetVertexAttr 设置节点属性
         *      删除: ->
         *      查询: -> GetVertexAttr 查询节点属性
         *           -> GetInEdges  查询节点入边
         *           -> GetOutEdges 查询节点出边
         * ============================= */

        /**
         * 设置节点属性
         * @param req
         * @return
         */
        Status SetVertexAttr(/* const */VertexRequest &req) override;

        /**
         * @brief 删除节点及节点相关联的边
         * @param req
         * @return
         */
        Status DeleteVertex(/* const */VertexRequest &req) override;

        /**
         * 查询节点属性
         * @param req
         * @param pQueryResult
         * @return
         */
        Status GetVertexAttr(/* const */VertexRequest &req, VertexQueryResult *pQueryResult) const override;

        /**
         * @brief 获取入边
         * @param req
         * @param pQueryResult
         * @return
         */
        Status GetInEdges(/* const */VertexRequest &req, EdgesQueryResult *pQueryResult) const override;

        /**
         * @brief 获取出边
         * @param req
         * @param pQueryResult
         * @return
         */
        Status GetOutEdges(/* const */VertexRequest &req, EdgesQueryResult *pQueryResult) const override;

        /**
         * @brief 获取 出 && 入 边
         * @param req
         * @param pQueryResult
         * @return
         */
        Status GetBothEdges(/* const */VertexRequest &req, EdgesQueryResult *pQueryResult) const override;

        /**
         * @brief 获取入边的节点
         * @param req
         * @param pQueryResult
         * @return
         */
        Status GetInVertices(/* const */VertexRequest &req, VertexQueryResult *pQueryResult) const override;

        /**
         * @brief 获取出边的节点
         * @param req
         * @param pQueryResult
         * @return
         */
        Status GetOutVertices(/* const */VertexRequest &req, VertexQueryResult *pQueryResult) const override;

        /**
         * @brief 获取 出 && 入 顶点
         * @param req
         * @param pQueryResult
         * @return
         */
        Status GetBothVertices(/* const */VertexRequest &req, VertexQueryResult *pQueryResult) const override;

        /* =============================
         *     从边出发的增/删/查/改
         *      增加: -> AddEdge 插入边
         *      删除: -> DeleteEdge 删除边
         *      查询: -> GetEdgeAttr 查询节点属性
         *      修改: -> SetEdgeAttr
           ============================= */

        /**
         * 插入边
         * @param req
         * @return
         */
        Status AddEdge(/* const */EdgeRequest &req) override;

        /**
         * 删除边
         * @param req
         * @return
         */
        Status DeleteEdge(/* const */EdgeRequest &req) override;

        /**
         * 获取单条边的属性
         * @param req
         * @param pQueryResult
         * @return
         */
        Status GetEdgeAttr(/* const */EdgeRequest &req, EdgesQueryResult *pQueryResult) override;

        /**
         * 修改边属性
         * @param req
         * @return
         */
        Status SetEdgeAttr(/* const */EdgeRequest &req) override;

        /* ===================================
         *          database的元数据
         * =================================== */

        /**
         * db名称
         */
        std::string GetName() const override;

        /**
         * 存储的目录
         */
        std::string GetStorageDirname() const override;

        /**
         * 存储的节点数
         */
        vid_t GetNumVertices() const override;

        /**
         * 存储的边数
         */
        size_t GetNumEdges() const override;

        /**
         * 所有更改刷到磁盘 (Flush过程中加锁, 阻止写操作)
         * shard-buffer的边
         * interval信息
         * 节点数
         */
        Status Flush() override;


        /* ===================================
         *       database的Schema元数据
         * =================================== */

        /**
         * database 中含有多少种类型的边
         */
        std::vector<EdgeLabel> GetEdgeLabels() const override;

        /**
         * database 中含有多少种类型的节点
         */
        std::vector<std::string> GetVertexLabels() const override;

        // ======== 节点属性列 ======== //
        Status CreateNewVertexLabel(const std::string &label) override;

        Status CreateVertexAttrCol(const std::string &label, ColumnDescriptor config) override;

        Status DeleteVertexAttrCol(const std::string &label, const std::string &columnName) override;

        Status GetVertexAttrs(const std::string &label, std::vector<ColumnDescriptor> *configs) const override;

        Status GetVertexAttrNames(const std::string &label, std::vector<std::string> *names) const override;

        // ======== 边属性列 ======== //

        Status CreateNewEdgeLabel(
                const std::string &elabel,
                const std::string &src_label, const std::string &dst_label) override {
            return CreateNewEdgeLabel(EdgeLabel(elabel, src_label, dst_label));
        }

        Status CreateNewEdgeLabel(const EdgeLabel &dstLabel) override;
        
        /**
         * 若类型为 label 的边属性列中不存在与 config 同名的属性列, 则创建新的属性列
         * 否则返回 Status::InvalidArgument
         */
        Status CreateEdgeAttrCol(const EdgeLabel &label, ColumnDescriptor config) override;

        /**
         * 删除边属性列
         */
        Status DeleteEdgeAttrCol(const EdgeLabel &label, const std::string &columnName) override;

        /**
         * 获取所有边属性列描述
         */
        Status GetEdgeAttrs(const EdgeLabel &label, std::vector<ColumnDescriptor> *configs) const override;

        /**
         * 获取所有边属性列名字
         */
        Status GetEdgeAttrNames(const EdgeLabel &label, std::vector<std::string> *names) const override;

        /**
         * 生成节点的度文件
         */
        Status GenDegreeFile() override;

        /**
         * @brief 导出数据(including 节点/关系/Schema)
         * @param out_dir   导出的文件夹
         * @return
         */
        Status ExportData(const std::string &out_dir) override ;

        /**
         * @brief 批量更新/插入边
         * @param reqs
         * @param bulkOptions
         * @return
         */
        //Status BulkUpdateEdges(/* const */ std::vector<EdgeRequest> &reqs,
         //                      const BulkUpdateOptions &bulkOptions) override;

        /**
         * @brief 批量更新/插入节点
         * @param reqs
         * @param bulkOptions
         * @return
         */
        //Status BulkUpdateVertices(/* const */ std::vector<VertexRequest> &reqs,
         //                         const BulkUpdateOptions &bulkOptions) override;

        /**
         * one shortest path
         * @param req
         * @return json over one shortest path
         */
        std::string ShortestPath(const PathRequest& path_req) const override;

        //std::string TimeShortestPath(const PathRequest& path_req) const override;

        /**
         * all  path
         * @param req
         * @return json over all  path
         */
        std::string AllPath(const PathRequest& path_req) const override;

        //std::string TimeAllPath(const PathRequest& path_req) const override;

        Status Kout(const TraverseRequest& traverse_req,
            std::vector<PVpair> *e_visited) const override;

        Status KoutSize(const TraverseRequest& traverse_req,
            size_t *v_size, size_t *e_size) const override;

        Status Kneighbor(const TraverseRequest& traverse_req,
            std::vector<PathVertex> *visited) const override;
        /**
         * engine api
        virtual
        Status PageRank(const HetnetRequest& hn_req) const override;
            
        virtual
        Status LPA(const HetnetRequest& hn_req) const override;
            
        virtual
        Status FastUnfolding(const HetnetRequest& hn_req) const override;
         */

    private:

        std::shared_ptr<IDEncoder> GetIDEncoder() const ;

        void LogShardInfos() const;

        Status BulkUpdateEdgesRange(
                std::vector<EdgeRequest>::iterator beg,
                std::vector<EdgeRequest>::iterator end,
                const BulkUpdateOptions &bulkOptions);

        Status FlushUnlocked();

        static
        Status PrepareRequest(VertexRequest *req, const std::shared_ptr<VertexColumnList> &lst, std::shared_ptr<IDEncoder> encoder);
        static
        Status PrepareRequest(EdgeRequest *req, const std::shared_ptr<VertexColumnList> &lst, std::shared_ptr<IDEncoder> encoder);

	/*
        Status Recover();
        Status RecoverVersionOld(
                MetaHeterogeneousAttributes *pHetEdgeProp, MetaShardInfo *pShardInfo,
                MetaJournal *pMetaJournal);
        Status RecoverVersionNew(
                const std::string &meta_dir,
                MetaHeterogeneousAttributes *pHetEdgeProp, MetaShardInfo *pShardInfo,
                MetaJournal *pMetaJournal);

        Status RecoverHandlers(const MetaShardInfo &meta_shard_info);
	*/
        //Status RecoverJournalFile(const uint64_t log_no, uint64_t *max_sequence);

        Status RedoAddEdge(/* const */ EdgeRequest &req);
        Status RedoSetEdgeAttr(/* const */ EdgeRequest &req);
        Status RedoDeleteEdge(/* const */ EdgeRequest &req);
        Status RedoDeleteVertex(/* const */ VertexRequest &req);
        Status RedoSetVertexAttr(/* const */ VertexRequest &req);
    private:
        friend class SkgDB;
        // 标示db打开/关闭状态
        std::atomic<bool> m_closed;
        std::string m_name;
        Options m_options;
        std::vector<ShardTreePtr> m_trees;

        MetaHeterogeneousAttributes m_edge_attr;
        std::shared_ptr<VertexColumnList> m_vertex_columns;
        std::shared_ptr<IDEncoder> m_id_encoder;
#ifdef SKG_QUERY_USE_MT
        // 查询的线程池
        mutable ::ThreadPool m_query_pool;
#endif
        std::mutex m_write_lock;

        //Version m_version;
        // 恢复日志的写入句柄.
        //std::unique_ptr<skg::log::Writer> m_log_writer;
    };
}
#endif //STARKNOWLEDGEGRAPHDATABASE_SKGDBIMPL_H
