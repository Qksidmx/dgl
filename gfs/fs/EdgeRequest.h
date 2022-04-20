#ifndef STARKNOWLEDGEGRAPHDATABASE_EDGEQUERY_H
#define STARKNOWLEDGEGRAPHDATABASE_EDGEQUERY_H

#include <string>
#include <vector>

#include "util/types.h"
#include "util/status.h"
#include "ColumnDescriptor.h"
#include "IRequest.h"
#include "ResultProperties.h"

namespace skg {

    class EdgeRequest: public IRequest {
    public:
        EdgeRequest();
        EdgeRequest(const std::string &label,
                    const std::string &srcVertexLabel, const std::string &srcVertex,
                    const std::string &dstVertexLabel, const std::string &dstVertex);
        EdgeRequest(const EdgeLabel &label, const std::string &srcVertex, const std::string &dstVertex);
        EdgeRequest(const std::string &label, vid_t srcVid, vid_t dstVid);
        ~EdgeRequest() override;

        void Clear() override;

        /**
         * 设置操作的边
         */
        void SetEdge(const EdgeLabel &label,
                     const std::string &srcVertex,
                     const std::string &dstVertex);
        void SetEdge(const std::string &label,
                     const std::string &srcVertexLabel, const std::string &srcVertex,
                     const std::string &dstVertexLabel, const std::string &dstVertex);

        /**
         * 设置操作的边
         */
        void SetEdge(const std::string &label, vid_t srcVid, vid_t dstVid);

        /* ===========================
         *  插入/更新属性时,设置列的属性值
         * =========================== */

        Status SetWeight(EdgeWeight_t weight);
        Status SetInt32(const std::string &column, int32_t value) override ;
        Status SetFloat(const std::string &column, float value) override ;
        Status SetString(const std::string &column, const char *value, size_t valueSize) override ;
        Status SetDouble(const std::string &column, double value) override ;
        Status SetInt64(const std::string &column, int64_t value) override ;
        Status SetTimestamp(const std::string &column, time_t value) override ;
        Status SetTimeString(const std::string &column, const char *value, size_t valueSize) override ;

        /**
         * 查询时, 设置查询的列名称
         */
        Status SetQueryColumnNames(const std::vector<std::string> &columns) override ;

        std::string ToDebugString() const;
    public:
        const std::vector<ColumnDescriptor> &GetColumns() const {
                return m_columns;
        }
        const EdgeLabel GetLabel() const {
                return EdgeLabel(m_label, m_srcVertexLabel, m_dstVertexLabel);
        }
        const std::string SrcLabel() const {
                return m_srcVertexLabel;
        }
        const std::string DstLabel() const {
                return m_dstVertexLabel;
        }
        const std::string SrcVertex() const {
                return m_srcVertex;
        }
        const std::string DstVertex() const {
                return m_dstVertex;
        }
        const vid_t SrcVid() const {
                return m_srcVid;
        }
        const vid_t DstVid() const {
                return m_dstVid;
        }

    private:
        // 隐藏的构造函数, 供上面几个public的构造函数统一调用
        EdgeRequest(const std::string &label,
                    const std::string &srcVertexLabel, const std::string &srcVertex,
                    const std::string &dstVertexLabel, const std::string &dstVertex,
                    vid_t srcVid, vid_t dstVid);
    private:
        friend class ShardTree;
        friend class EdgePartition;
        friend class SkgDBImpl;
        friend class SkgDDBImp;
        friend class dbquery_grpc_server;
        friend class SubEdgePartition;
        friend class SubEdgePartitionWithMemTable;
        friend class VecMemTable;
        friend class HashMemTable;
        friend class RequestUtilities;

        std::string m_label;
        std::string m_srcVertexLabel;
        std::string m_srcVertex;
        std::string m_dstVertexLabel;
        std::string m_dstVertex;
        vid_t m_srcVid;
        vid_t m_dstVid;
        std::vector<ColumnDescriptor> m_columns;
#ifdef SKG_REQ_VAR_PROP
        ResultProperties m_prop;
#else
        char m_coldata[4096];
        uint32_t m_offset;
#endif
    public:
        // no copy allow
        EdgeRequest(const EdgeRequest&);
        EdgeRequest& operator=(const EdgeRequest&);
        // move copy functions
        EdgeRequest& operator=(EdgeRequest &&rhs) noexcept;
        EdgeRequest(EdgeRequest &&rhs) noexcept;
    };

}

#endif //STARKNOWLEDGEGRAPHDATABASE_EDGEQUERY_H
