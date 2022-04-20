#ifndef STARKNOWLEDGEGRAPHDATABASE_IQUERY_H
#define STARKNOWLEDGEGRAPHDATABASE_IQUERY_H

#include <cstdlib>
#include <vector>
#include <string>

#include "util/status.h"

namespace skg {

    class IRequest {
    public:
        static const std::vector<std::string> QUERY_ALL_COLUMNS;
        static const ssize_t NO_LIMIT = -1;
    public:
        IRequest()
                : m_create_if_not_exist(false),
                  m_check_exist(true),
                  m_timeout_ms(-1),
                  m_nlimit(NO_LIMIT),
                  m_is_wal_enabled(true),
                  m_is_sync_enabled(false)
        {
            //m_is_wal_enabled = false; // FIXME 暂时默认不开启
        }

        virtual ~IRequest() {
        }

        virtual void Clear() {
            m_create_if_not_exist = false;
        }

        void SetQueryTimeout(int milliseconds) {
            m_timeout_ms = milliseconds;
        }

        void SetLimit(ssize_t numLimit) {
            if (numLimit < 0) {
                if (numLimit != -1) { numLimit = NO_LIMIT; }
                m_nlimit = numLimit;
            } else {
                m_nlimit = numLimit;
            }
        }

        /**
         * 调用 SkgDB 以下接口时.
         *
         * SetVertexAttr
         *      默认为 false
         *      -- false -> 如果节点不存在, 返回NotExist
         *      -- true  -> 如果节点不存在, 则尝试插入新的节点并设置属性
         * SetEdgeAttr
         *      默认为 false
         *      -- false -> 如果边不存在, 返回 NotExist
         *      -- true  -> 如果边不存在, 则尝试插入新的边并设置属性
         * AddEdge
         *      -- 默认为 true
         */
        void SetCreateIfNotExist(bool create) {
            m_create_if_not_exist = create;
        }

        /**
         * 调用 SkgDB 以下接口时.
         *
         * SetEdgeAttr
         *      默认为 true
         * AddEdge
         *      默认为 true
         *      -- true  -> 先检查边是否在图中, 如果存在, 则更新边的值.
         *                  相当于调用 SetEdgeAttr, 且设置了 `CreateIfNotExist`
         *      -- false -> 如果确认插入的边不存在于原来的图中, 直接插入到 MemTable 缓存中.
         */
        void SetCheckExist(bool check) {
            m_check_exist = check;
        }

        /* ===========================
         *  插入/更新属性时,设置列的属性值
         * =========================== */

        virtual
        Status SetInt32(const std::string &column, int32_t value) = 0;
        virtual
        Status SetFloat(const std::string &column, float value) = 0;
        virtual
        Status SetString(const std::string &column, const char *value, size_t valueSize) = 0;
        virtual
        Status SetDouble(const std::string &column, double value) = 0;
        virtual
        Status SetInt64(const std::string &column, int64_t value) = 0;
        virtual
        Status SetTimestamp(const std::string &column, time_t value) = 0;
        virtual
        Status SetTimeString(const std::string &column, const char *value, size_t valueSize) = 0;

        /**
         * 查询时, 设置查询的列名称
         */
        virtual
        Status SetQueryColumnNames(const std::vector<std::string> &columns) = 0;

        int GetQueryTimeout() const {
            return m_timeout_ms;
        }

        ssize_t GetLimit() const {
            return m_nlimit;
        }

        bool IsCreateIfNotExist() const {
            return m_create_if_not_exist;
        }

        inline
        bool IsCheckExist() const {
            return m_check_exist;
        }

        inline
        void DisableWAL() {
            m_is_wal_enabled = false;
        }

        inline
        bool IsWALEnabled() const {
            return m_is_wal_enabled;
        }

        inline
        void EnableSync() {
            m_is_sync_enabled = true;
        }

        inline
        bool IsSyncEnabled() const {
            return m_is_sync_enabled;
        }

    private:
        friend class SkgDDBImp;
        friend class dbquery_grpc_server;
        // 更新时, 如果节点/边不存在, 是否创建
        bool m_create_if_not_exist;
        // 插入边时, 是否检查边已经存在
        bool m_check_exist;
        // 查询超时(ms)
        int m_timeout_ms;
        // 查询返回结果集, 数据条数上限
        ssize_t m_nlimit;
        // 是否不需要写WAL
        bool m_is_wal_enabled;
        // 在 `write()` 后 是否使用 `fsync()` 进行强同步, 默认为 false
        bool m_is_sync_enabled;
    };

}

#endif //STARKNOWLEDGEGRAPHDATABASE_IQUERY_H
