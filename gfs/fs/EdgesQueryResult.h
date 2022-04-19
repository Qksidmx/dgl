#ifndef STARKNOWLEDGEGRAPH_EDGES_QUERY_RESULT_H
#define STARKNOWLEDGEGRAPH_EDGES_QUERY_RESULT_H

#include <vector>
#include <mutex>
#include <algorithm>
#include <memory>
#include <map>
#include <cstdint>

#include "util/types.h"
#include "ColumnDescriptor.h"
#include "ResultMetadata.h"
#include "IDEncoder.h"
#include "ResultEdge.h"

namespace skg {

    class MetaHeterogeneousAttributes;

    class EdgesQueryResult {
    public:
        EdgesQueryResult();
        virtual ~EdgesQueryResult();

    public:
        /******* 获取拓扑信息 *******/

        size_t Size();

        /**
         * @brief 获取边的 label, src 节点的 label, dst 节点的 label
         * @param status
         */
        EdgeLabel GetEdgeLabel(Status *status) const;

        /**
         * @brief 获取边的 label
         * @param status
         */
        std::string GetLabel(Status *status) const;

        /**
         * @brief 获取边的 src 节点 label
         * @param status
         */
        std::string GetSrcVertexLabel(Status *status) const;

        /**
         * @brief 获取边的 dst 节点 label
         * @param status
         */
        std::string GetDstVertexLabel(Status *status) const;

        /*** 获取 src, dst 节点的 vid ***/

        vid_t GetSrcVid(Status *status) const;

        vid_t GetDstVid(Status *status) const;

        /*** 获取 src, dst 节点的 vertex-id ****/

        std::string GetSrcVertex(Status *status) const;

        std::string GetDstVertex(Status *status) const;

        /**
         * @brief 获取边的权重
         * @param status
         */
        EdgeWeight_t GetWeight(Status *status) const;

        /******* 以下 EdgesQueryResult / VertexQueryResult 相同的接口 *******/

        /**
         * @brief 清空结果集
         */
        void Clear();

        /**
         * @brief cursor 是否有下一个元素.
         */
        bool HasNext() const;

        /**
         * cursor 移动到下一个位置. EdgeQueryResult 初始时指向第一行之前；第一次调用 `next`, 指向第一行
         * 当 `next` 返回 false 时, cursor 指向最后一行之后.
         * @return true -- 移动到下一行数据; false 没有更多的数据了
         */
        bool MoveNext();

        /******* 获取属性描述 *******/

        /**
         * 获取 QueryResult 中列的列数, 类型.
         * @return description of this Result columns
         */
        const ResultMetadata *GetMetaDataByLabel(const std::string &label) const;

        /**
         * 获取 QueryResult 中当前边对应的列数, 类型.
         * @return  description of this Result columns
         */
        const ResultMetadata *GetMetaData(Status *status) const;

        /******* 判断属性值是否为空 *******/

        /**
         * @brief 获取 QueryResult 中, 当前数据的第 columnIndex 列是否为 null
         * @param columnIndex
         * @param status
         * @return
         */
        bool IsNull(size_t columnIndex, Status *status) const;

        /**
         * @brief 获取 QueryResult 中, 当前数据的 columnName 列是否为 null
         * @param columnName
         * @param status
         * @return
         */
        bool IsNull(const char *columnName, Status *status) const;

        /******* 获取属性值的接口 *******/

        /**
         * 获取 QueryResult 中, 当前数据的第 columnIndex 列, 解析为 int
         * @param columnIndex
         * @return
         */
        int32_t GetInt32(size_t columnIndex, Status *status) const;

        /**
         * 获取 QueryResult 中, 当前数据的 columnName 列, 解析为 int
         * @param columnName
         * @return
         */
        int32_t GetInt32(const char *columnName, Status *status) const;

        /**
         * 获取 QueryResult 中, 当前数据的第 columnIndex 列, 解析为 float
         * @param columnIndex
         * @return
         */
        float GetFloat(size_t columnIndex, Status *status) const;

        /**
         * 获取 QueryResult 中, 当前数据的 columnName 列, 解析为 float
         * @param columnName
         * @return
         */
        float GetFloat(const char *columnName, Status *status) const;

        /**
         * 获取 QueryResult 中, 当前数据的第 columnIndex 列, 解析为 string
         * @param columnIndex
         * @return
         */
        std::string GetString(size_t columnIndex, Status *status) const;

        /**
         * 获取 QueryResult 中, 当前数据的 columnName 列, 解析为 string
         * @param columnName
         * @return
         */
        std::string GetString(const char *columnName, Status *status) const;

        /**
         * 获取 QueryResult 中, 当前数据的第 columnIndex 列, 解析为 double
         * @param columnIndex
         * @return
         */
        double GetDouble(size_t columnIndex, Status *status) const;

        /**
         * 获取 QueryResult 中, 当前数据的 columnName 列, 解析为 double
         * @param columnName
         * @return
         */
        double GetDouble(const char *columnName, Status *status) const;

        /**
         * 获取 QueryResult 中, 当前数据的第 columnIndex 列, 解析为 int64
         * @param columnIndex
         * @return
         */
        int64_t GetInt64(size_t columnIndex, Status *status) const;

        /**
         * 获取 QueryResult 中, 当前数据的 columnName 列, 解析为 int64
         * @param columnName
         * @return
         */
        int64_t GetInt64(const char *columnName, Status *status) const;

        /**
         * 获取 QueryResult 中, 当前数据的第 columnIndex 列, 解析为 时间戳
         * @param columnIndex
         * @return
         */
        time_t GetTimestamp(size_t columnIndex, Status *status) const;

        /**
         * 获取 QueryResult 中, 当前数据的 columnName 列, 解析为 时间戳
         * @param columnName
         * @return
         */
        time_t GetTimestamp(const char *columnName, Status *status) const;

        /**
         * 获取 QueryResult 中, 当前数据的第 columnIndex 列, 解析为 时间 (解析为字符串格式)
         * @param columnIndex
         * @return
         */
        std::string GetTimeString(size_t columnIndex, Status *status) const;

        /**
         * 获取 QueryResult 中, 当前数据的 columnName 列, 解析为 时间 (解析为字符串格式)
         * @param columnName
         * @return
         */
        std::string GetTimeString(const char *columnName, Status *status) const;

        std::string ToJsonString() const;
    private:

        bool IsBeforeFirstOrAfterLast() const;

        Status GetOffsetByIndex(size_t columnIndex, size_t *offset) const;
        Status GetOffsetByName(const char *colName, size_t *offset) const;

#ifdef SKG_QUERY_USE_MT
        std::mutex m_receive_lock;
#endif

        /**
         * 0 -- before first row
         * 1 -- first row
         * `m_edges.size() + 1` -- after last row
         */
        uint64_t m_row_position;

        std::vector<ResultEdge> m_edges;
        std::map<EdgeTag_t, EdgeLabel> m_labels;
        std::map<std::string, ResultMetadata> m_metadatas;

        // 结果集最大返回条数
        ssize_t m_nlimit;

    private:
        Status SetResultMetadata(const MetaHeterogeneousAttributes &hetAttributes);

        Status TranslateEdgeVertex(const std::shared_ptr<IDEncoder> &encoder);

        // For call SetResultMetadata/TranslateEdgeVertex
        friend class SkgDBImpl;

        Status ReceiveEdge(
                const vid_t src, const vid_t dst,
                const EdgeWeight_t weight, const EdgeTag_t tag,
                const char *column_bytes, const size_t column_bytes_len,
                const PropertiesBitset_t &bitset);

        // For call ReceiveEdge
        friend class ShardTree;
        friend class dbquery_grpc_server;
        friend class SkgDDBImp;
        // For call GetQueryColumns
        friend class HashMemTable;
        friend class VecMemTable;
        friend class SubEdgePartition;

        friend class EdgesQueryResultUtils;
    };

}

#endif //STARKNOWLEDGEGRAPH_EDGES_QUERY_RESULT_H
