#ifndef STARKNOWLEDGEGRAPH_SKGDB_H
#define STARKNOWLEDGEGRAPH_SKGDB_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <map>
#include <set>
#include <vector>
#include <limits>

#include "util/types.h"
#include "util/options.h"
#include "util/status.h"

#include "VertexRequest.h"
#include "EdgeRequest.h"
#include "EdgesQueryResult.h"
#include "VertexQueryResult.h"
#include "PathAux.h"
//#include "HetnetAux.h"
#include "IDEncoder.h"

namespace skg {

    class SkgDB {
    public:
        /*=====================
         *    基本的打开、关闭、删除数据库
         *    options传递ip和port
         *    ignore 传递忽略文件
         *=====================*/

        /**
         * @brief 创建空的数据库
         * @param name      生成的数据库名字
         * @param options
         * @return
         */

        static
        Status Create(const std::string &name, const Options &options);

	/*
        static
        Status CreateRemote(const std::string &name, const Options &options);
	*/

        /**
         * 从文件中批量导入数据
         * @param name      生成的数据库名字
         * @param options
         * @param descFile  xml 描述文件. 定义图的 Schema
         * @return
         */
	/*
        static
        Status BuildFromFile(
                const std::string &name,
                const Options &options,
                const std::string &descFile);

        static
        Status BuildFromFileRemote(
                const std::string &name,
                const Options &options,
                const std::string &descFile);
		*/

        /**
         * 打开单机版本的逻辑
         */
        static
        Status Open(const std::string &name, const Options &options,
                    SkgDB **pDB);

        /**
         * 打开分布式版本, 与 Master 通讯的 Client 逻辑
         * Options 中传递 Master 的 IP:Port
         */

	/*
        static
        Status OpenRemote(const std::string &name, const Options &options,
                          SkgDB **pDB);
			  */

        virtual
        Status Drop(const std::set<std::string> &ignore={}) = 0;

        virtual
        Status Close() = 0;

    public:
        SkgDB() = default;
        virtual ~SkgDB() = default;

        /* =============================
         *     从节点出发的增/删/查/改
         *     增/改: -> SetVertexAttr 设置节点属性
         *      删除: -> DeleteVertex  删除节点及节点相关连的边
         *      查询: -> GetVertexAttr 查询节点属性
         *           -> GetInEdges  查询节点入边
         *           -> GetOutEdges 查询节点出边
         * ============================= */

        /**
         * 设置节点属性
         * @param req
         * @return
         */
        virtual
        Status SetVertexAttr(/* const */VertexRequest &req) = 0;

        /**
         * @brief 删除节点及节点相关联的边
         * @param req
         * @return
         */
        virtual
        Status DeleteVertex(/* const */VertexRequest &req) = 0;

        /**
         * 查询节点属性
         * @param req
         * @param pQueryResult
         * @return
         */
        virtual
        Status GetVertexAttr(/* const */VertexRequest &req, VertexQueryResult *pQueryResult) const = 0;

        /**
         * @brief 获取入边
         * @param req
         * @param pQueryResult
         * @return
         */
        virtual
        Status GetInEdges(/* const */VertexRequest &req, EdgesQueryResult *pQueryResult) const = 0;

        /**
         * @brief 获取出边
         * @param req
         * @param pQueryResult
         * @return
         */
        virtual
        Status GetOutEdges(/* const */VertexRequest &req, EdgesQueryResult *pQueryResult) const = 0;

        /**
         * @brief 获取 出 && 入 边
         * @param req
         * @param pQueryResult
         * @return
         */
        virtual
        Status GetBothEdges(/* const */VertexRequest &req, EdgesQueryResult *pQueryResult) const = 0;

        /**
         * @brief 获取入顶点
         * @param req
         * @param pQueryResult
         * @return
         */
        virtual
        Status GetInVertices(/* const */VertexRequest &req, VertexQueryResult *pQueryResult) const = 0;

        /**
         * @brief 获取出顶点
         * @param req
         * @param pQueryResult
         * @return
         */
        virtual
        Status GetOutVertices(/* const */VertexRequest &req, VertexQueryResult *pQueryResult) const = 0;

        /**
         * @brief 获取 出 && 入 顶点
         * @param req
         * @param pQueryResult
         * @return
         */
        virtual
        Status GetBothVertices(/* const */VertexRequest &req, VertexQueryResult *pQueryResult) const = 0;

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
        virtual
        Status AddEdge(/* const */EdgeRequest &req) = 0;

        /**
         * 删除边
         * @param req
         * @return
         */
        virtual
        Status DeleteEdge(/* const */EdgeRequest &req) = 0;

        /**
         * 获取单条边的属性
         * @param req
         * @param pQueryResult
         * @return
         */
        virtual
        Status GetEdgeAttr(/* const */EdgeRequest &req, EdgesQueryResult *pQueryResult) = 0;

        /**
         * 修改边属性
         * @param req
         * @return
         */
        virtual
        Status SetEdgeAttr(/* const */EdgeRequest &req) = 0;

        /* ===================================
         *          database的元数据
         * =================================== */

        /**
         * db名称
         */
        virtual
        std::string GetName() const = 0;

        /**
         * 存储的目录
         */
        virtual
        std::string GetStorageDirname() const = 0;

        /**
         * 存储的节点数
         */
        virtual
        vid_t GetNumVertices() const = 0;

        /**
         * 存储的边数
         */
        virtual
        size_t GetNumEdges() const = 0;

        /**
         * 所有更改刷到磁盘
         * shard-buffer的边
         * interval信息
         * 节点数
         */
        virtual
        Status Flush() = 0;


        /* ===================================
         *       database的Schema元数据
         * =================================== */

        /**
         * database 中含有多少种类型的边
         */
        virtual
        std::vector<EdgeLabel> GetEdgeLabels() const = 0;

        /**
         * database 中含有多少种类型的节点
         */
        virtual
        std::vector<std::string> GetVertexLabels() const = 0;

        // ======== 节点属性列 ======== //
        /**
         * @brief 创建新的节点类型
         * @param label     节点类型
         * @return
         */
        virtual
        Status CreateNewVertexLabel(const std::string &label) = 0;

        virtual
        Status CreateVertexAttrCol(const std::string &label, ColumnDescriptor config) = 0;

        virtual
        Status DeleteVertexAttrCol(const std::string &label, const std::string &columnName) = 0;

        virtual
        Status GetVertexAttrs(const std::string &label, std::vector<ColumnDescriptor> *configs) const = 0;

        virtual
        Status GetVertexAttrNames(const std::string &label, std::vector<std::string> *names) const = 0;

        // ======== 边属性列 ======== //
        virtual
        Status CreateNewEdgeLabel(
                const std::string &elabel,
                const std::string &src_label, const std::string &dst_label) = 0;
        /**
         * @brief 创建新的关系类型
         * @param label     边类型
         * @param srcLabel  节点类型
         * @param dstLabel  节点类型
         * @return
         */
        virtual
        Status CreateNewEdgeLabel(const EdgeLabel &label) = 0;

        /**
         * 若colname的列不存在, 创建新的一列类型为edgeColType的边属性
         * 否则检查类型是否相同, 相同则通过column返回
         * 其他情况返回Status::InvalidArgument
         */
        virtual
        Status CreateEdgeAttrCol(const EdgeLabel &label, ColumnDescriptor config) = 0;

        /**
         * 删除边属性列
         */
        virtual
        Status DeleteEdgeAttrCol(const EdgeLabel &label, const std::string &columnName) = 0;

        /**
         * 获取所有边属性列描述
         */
        virtual
        Status GetEdgeAttrs(const EdgeLabel &label, std::vector<ColumnDescriptor> *configs) const = 0;

        /**
         * 获取所有边属性列名字
         */
        virtual
        Status GetEdgeAttrNames(const EdgeLabel &label, std::vector<std::string> *names) const = 0;

        /**
         * Deprecated. use tools/dump_degree instead
         * 生成节点的度文件
         */
        virtual
        Status GenDegreeFile() = 0;

        /**
         * @brief 导出数据(including 节点/关系/Schema)
         * @param out_dir   导出的文件夹
         * @return
         */
        virtual
        Status ExportData(const std::string &out_dir) = 0;

        /**
         * @brief 批量更新/插入边
         * @param reqs
         * @param bulkOptions
         * @return
         */
       // virtual
        //Status BulkUpdateEdges(/* const */ std::vector<EdgeRequest> &reqs,
                               //const BulkUpdateOptions &bulkOptions) = 0;
        /**
         * @brief 批量更新/插入节点
         * @param reqs
         * @param bulkOptions
         * @return
         */
        //virtual
        //Status BulkUpdateVertices(/* const */ std::vector<VertexRequest> &reqs,
                                  //const BulkUpdateOptions &bulkOptions) = 0;

        /* =============================
         *    Path Computation 
         *       shortest path: -> ShortestPath
         *       all path     : -> AllPath
         *
         * ============================= */

        /**
         * one shortest path
         * @param req
         * @return json over one shortest path
         */
        virtual
        std::string ShortestPath(const PathRequest& path_req) const = 0;

        //virtual
        //std::string TimeShortestPath(const PathRequest& path_req) const = 0;

        /**
         * all  path
         * @param req
         * @return json over all  path
         */
        virtual
        std::string AllPath(const PathRequest& path_req) const = 0;

        //virtual
        //std::string TimeAllPath(const PathRequest& path_req) const = 0;

        virtual
        Status Kout(const TraverseRequest& traverse_req, std::vector<PVpair> *e_visited) const = 0;

        virtual
        Status KoutSize(const TraverseRequest& traverse_req, size_t *v_size, size_t *e_size) const = 0;

        virtual
        Status Kneighbor(const TraverseRequest& traverse_req, std::vector<PathVertex> *visited) const = 0;

        /**
         * engine api
        virtual
        Status PageRank(const HetnetRequest& hn_req) const = 0;
            
        virtual
        Status LPA(const HetnetRequest& hn_req) const = 0;
            
        virtual
        Status FastUnfolding(const HetnetRequest& hn_req) const = 0;
         */
            
    public:
        // 禁止复制
        SkgDB(const SkgDB&) = delete;
        SkgDB& operator=(const SkgDB&) = delete;
    };

}
#endif //STARKNOWLEDGEGRAPH_SKGDB_H
