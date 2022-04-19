#ifndef STARKNOWLEDGEGRAPH_VERTEX_QUERY_RESULT_H
#define STARKNOWLEDGEGRAPH_VERTEX_QUERY_RESULT_H

#include <vector>
#include <map>
#include <mutex>
#include <memory>
#include <algorithm>

#include "util/types.h"
#include "util/status.h"
#include "ColumnDescriptor.h"
#include "ResultMetadata.h"
#include "ResultProperties.h"
#include "ResultVertex.h"
#include "IDEncoder.h"

namespace skg {

    class MetaHeterogeneousAttributes;

    class VertexQueryResult {
    public:
        VertexQueryResult();
        virtual ~VertexQueryResult();

    public:
        /******* 获取节点信息 *******/

        /**
         * @brief 获取边的 label
         * @param status
         */
        std::string GetLabel(Status *status) const;

        EdgeTag_t GetTag(Status *status) const;

        /*** 获取节点的 vid ***/
        vid_t GetVid(Status *status) const;

        /*** 获取节点的 vertex-id ****/
        const std::string& GetVertex(Status *status) const;

        /******* 以下 EdgesQueryResult / VertexQueryResult 相同的接口 *******/

        size_t Size();

        /**
         * @brief 清空结果集
         */
        void Clear();

        /**
         * @brief cursor 是否有下一个元素.
         */
        bool HasNext() const;

        /**
         * cursor 移动到下一个位置. VertexQueryResult 初始时指向第一行之前；第一次调用 `next`, 指向第一行
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

        /******* 获取属性描述 *******/

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
         * 获取 QueryResult 中, 当前数据的第 columnIndex 列, 解析为 int64
         * @param columnIndex
         * @return
         */
        int64_t GetInt64(size_t columnIndex, Status *status) const;

        /**
         * 获取 QueryResult 中, 当前数据的 columnName 列, 解析为 int64
         * @param columnIndex
         * @return
         */
        int64_t GetInt64(const char *columnName, Status *status) const;

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
         * 获取 QueryResult 中, 当前数据的第 columnIndex 列, 解析为 Bytes
         * @param columnIndex
         * @return
         */
        std::string GetBytes(size_t columnIndex, Status *status) const;
       

        /**
         * 获取 QueryResult 中, 当前数据的 columnName 列, 解析为 Bytes
         * @param columnName
         * @return
         */
        std::string GetBytes(const char *columnName, Status *status) const;

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
        // 以下方法/成员不对外暴露
    private:

        bool IsBeforeFirstOrAfterLast() const;

        Status GetOffsetByIndex(size_t columnIndex, size_t *offset) const;

        Status GetOffsetByName(const char *colName, size_t *offset) const;

    protected:
#ifdef SKG_QUERY_USE_MT
        std::mutex m_receive_lock;
#endif

        /**
         * 0 -- before first row
         * 1 -- first row
         * `m_vertices.size() + 1` -- after last row
         */
        size_t m_row_index;

        // ===== 节点数据 ===== //
        std::vector<ResultVertex> m_vertices;

        // label_tag -> label
        std::map<EdgeTag_t, std::string> m_labels;
        // label -> 属性描述
        std::map<std::string, ResultMetadata> m_metadatas;

        // 结果集最大返回条数
        ssize_t m_nlimit;
    private:

        Status TranslateVertex(const std::shared_ptr<IDEncoder> &encoder);

        Status Receive(EdgeTag_t tag,
                     vid_t vid,
                     const std::string &vertex,
                     const ResultProperties &prop);

        void Receive(EdgeTag_t tag, vid_t vid);

        bool IsOverLimit();

        Status SetResultMetadata(const MetaHeterogeneousAttributes &hetAttributes);

        // For call SetResultMetadata/SetVertex/Receive
        friend class SkgDBImpl;
        friend class VertexColumnList;
        friend class SkgDDBImp;
        friend class dbquery_grpc_server;
        // For call SetResultMetadata/SetVertex/Receive
        friend class ShardTree;
        friend class SubEdgePartition;
        friend class MemTable;
        friend class VecMemTable;
        friend class HashMemTable;

    };

}

#endif //STARKNOWLEDGEGRAPH_VERTEX_QUERY_RESULT_H
