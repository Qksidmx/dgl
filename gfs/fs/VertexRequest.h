#ifndef STARKNOWLEDGEGRAPHDATABASE_VERTEXQUERY_H
#define STARKNOWLEDGEGRAPHDATABASE_VERTEXQUERY_H

#include <string>

#include "util/types.h"
#include "util/status.h"
#include "ColumnDescriptor.h"
#include "IRequest.h"
#include "ResultProperties.h"

namespace skg {

    class VertexRequest: public IRequest {
    public:
        VertexRequest();
        VertexRequest(const std::string &label, const std::string &vertex);
        VertexRequest(const std::string &label, vid_t vid);
        ~VertexRequest() override;

        void Clear() override;

        /**
         * 设置查询的节点
         */
        void SetVertex(const std::string &label, const std::string &vertex);

        /**
         * 设置查询的节点
         */
        void SetVertex(const std::string &label, vid_t vid);

        /* ===========================
         *  插入/更新属性时,设置列的属性值
         * =========================== */

        Status SetInt32(const std::string &column, int32_t value) override ;
        Status SetFloat(const std::string &column, float value) override ;
        Status SetString(const std::string &column, const char *value, size_t valueSize) override ;
        Status SetDouble(const std::string &column, double value) override ;
        Status SetInt64(const std::string &column, int64_t value) override ;
        Status SetTimestamp(const std::string &column, time_t value) override ;
        Status SetTimeString(const std::string &column, const char *value, size_t valueSize) override ;
        Status SetVarChar(const std::string &column, const char *value, size_t valueSize);
        /**
         * 查询时, 设置查询的列名称
         */
        Status SetQueryColumnNames(const std::vector<std::string> &columns) override ;

        std::string ToDebugString() const;
    public:
        const std::vector<ColumnDescriptor> &GetColumns() const;
        const std::string &GetLabel() const;
        inline EdgeTag_t GetLabelTag() const {
            return m_labelTag;
        }
        inline const std::string &GetVertex() const {
            return m_vertex;
        }
        inline vid_t GetVid() const {
            return m_vid;
        }
        inline void SetInitLabel() {
            m_flags |= Flags::INIT_LABEL;
        }
        inline bool IsInitLabel() const {
            return (m_flags & Flags::INIT_LABEL) != 0;
        }
    private:
        // 隐藏的构造函数, 供上面几个public的构造函数统一调用
        VertexRequest(const std::string &label, const std::string &vertex, vid_t vid);
        friend class ShardTree;
        friend class VertexColumnList;
        friend class SkgDB;
        friend class SkgDBImpl;
        friend class SkgDDBImp;
        friend class dbquery_grpc_server;
        friend class SubEdgePartition;
        friend class VecMemTable;
        friend class HashMemTable;
        friend class RequestUtilities;

        std::string m_label;
        EdgeTag_t m_labelTag;
        vid_t m_vid;
        std::string m_vertex;
        std::vector<ColumnDescriptor> m_columns;
#ifdef SKG_REQ_VAR_PROP
        ResultProperties m_prop;
#else
        char m_coldata[4096];
        uint32_t m_offset;
#endif
        // 一次性批量查询
        struct TmpQueryV {
            EdgeTag_t tag;
            vid_t vid;
            std::string vertex;
        };
        std::vector<TmpQueryV> m_more;
        
        enum Flags {
            INIT_LABEL = 0x01,
        };
        uint32_t m_flags;
    public:
        // no copy allow
        VertexRequest(const VertexRequest&);
        VertexRequest& operator=(const VertexRequest &);
    public:
        // move copy functions
        VertexRequest(VertexRequest&&) noexcept;
        VertexRequest& operator=(VertexRequest &&) noexcept;
    };

}

#endif //STARKNOWLEDGEGRAPHDATABASE_VERTEXQUERY_H
