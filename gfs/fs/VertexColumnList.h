#ifndef STARKNOWLEDGEGRAPHDATABASE_VERTEXCOLUMNLIST_H
#define STARKNOWLEDGEGRAPHDATABASE_VERTEXCOLUMNLIST_H

#include <memory>

#include "util/status.h"
#include "fs/VertexRequest.h"
#include "fs/VertexQueryResult.h"
#include "fs/IDEncoder.h"

#include "Metadata.h"
#include "IVertexColumn.h"

namespace skg {
    class VertexColumnList;
    using VertexColumnListPtr = std::shared_ptr<VertexColumnList>;

    class VertexColumnList {
    public:
        static
        Status Create(const std::string &dir, const MetaHeterogeneousAttributes &hAttributes, vid_t max_vertex_id);
        static
        Status Open(const std::string &dir, std::shared_ptr<VertexColumnList> *pLst);

    private:
        VertexColumnList()
                : m_storage_dir(),
                  m_max_vertices_id(0),
                  m_storage_vertices(0),
                  m_vertex_attr(),
                  m_vertex_columns()
        {
        }
    public:

        ~VertexColumnList() {
            this->Flush();
        }

        /**
         * 删除所有节点属性列.
         */
        Status Drop();

        Status Flush();
        /**
         * 存储的节点数
         */
        vid_t GetNumVertices() const;

        vid_t AllocateNewVid() ;

        Status UpdateMaxVertexID(vid_t vid);

        Status GetLabelTag(const std::string &label, EdgeTag_t *tag) const;

        /**
         * 设置节点属性
         * @param req
         * @return
         */
        Status SetVertexAttr(/* const */VertexRequest &req);

        /**
         * @brief 删除节点
         * @param req
         * @return
         */
        Status DeleteVertex(/* const */VertexRequest &req);

        /**
         * 查询节点属性
         * @param req
         * @param pQueryResult
         * @return
         */
        Status GetVertexAttr(/* const */VertexRequest &req, VertexQueryResult *pQueryResult) const ;

        /**
         * database 中含有多少种类型的节点
         */
        inline
        std::vector<std::string> GetVertexLabels() const {
            return m_vertex_attr.GetLabels();
        }

        /**
         * @brief 创建新的节点类型
         * @param label     节点类型
         * @return
         */
        Status CreateNewVertexLabel(const std::string &label);

        Status CreateVertexAttrCol(const std::string &label, ColumnDescriptor config);

        Status DeleteVertexAttrCol(const std::string &label, const std::string &columnName);

        inline
        Status GetVertexAttrs(const std::string &label, std::vector<ColumnDescriptor> *descriptors) const {
            return m_vertex_attr.GetAttributesDescriptors(label, descriptors);
        }

        inline
        Status GetVertexAttrNames(const std::string &label, std::vector<std::string> *names) const {
            return m_vertex_attr.GetAttributesNames(label, names);
        }

        Status ExportData(const std::string &out_dir, std::shared_ptr<IDEncoder> ptr);

        const MetaHeterogeneousAttributes &GetVerticesProperties() const {
            return m_vertex_attr;
        }
    private:

        Status FillOneVertex(const MetaHeterogeneousAttributes &hetProp,
                             const EdgeTag_t tag,
                             const std::string &vertex, const vid_t vid,
                             VertexQueryResult *result) const ;

        inline
        std::string GetStorageDir() const {
            return m_storage_dir;
        }

    private:

        std::string m_storage_dir;
        // 已分配的最大节点ID
        std::atomic<vid_t> m_max_vertices_id;
        // 磁盘上存储节点的空间(capacity)
        vid_t m_storage_vertices;
        // 目前存储有多少个节点
        std::atomic<vid_t> m_num_vertices;
        MetaHeterogeneousAttributes m_vertex_attr;
        std::map<std::pair<EdgeTag_t, std::string>, IVertexColumnPtr> m_vertex_columns;
        IVertexColumnPtr m_bitset_column;

    public:
        // no copy allow
        VertexColumnList(const VertexColumnList&) = delete;
        VertexColumnList& operator=(const VertexColumnList &) = delete;
    };

}
#endif //STARKNOWLEDGEGRAPHDATABASE_VERTEXCOLUMNLIST_H
