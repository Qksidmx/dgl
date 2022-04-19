#ifndef STARKNOWLEDGEGRAPH_STATUS_HPP
#define STARKNOWLEDGEGRAPH_STATUS_HPP

#include <cstdint>
#include <cstring>
#include <string>
#include <utility>

namespace skg{

namespace proto { namespace idservice {
class RetStatus;
}}

    class Status {
    public:
        enum class Code {
            OK = 0,
            INVALID_ARGUMENT,
            FILE_NOT_FOUND,
            IO_ERROR,
            NOT_EXIST,
            NOT_IMPLEMENT,
            END_OF_FILE,
            SEND_RECV_TIMEOUT,
            CONNECT_TIMEOUT,
            PACKAGE_FORMAT_ERROR,
            RPC_ERROR,
            MT_JOB_FAILED,
            UNSUPPORT_SELF_LOOP,
            CORRUPTION,
            NETWORK_ERROR,
            TIMEOUT,
            RESULT_SIZE_OVER_LIMIT, // 结果集超出大小
        };
    public:
        Status();
        ~Status();

        Code code() const;
        bool ok() const;
        bool IsOverLimit() const;
        bool IsFileNotFound() const;
        bool IsInvalidArgument() const;
        bool IsNotExist() const;

        std::string ToString() const;
    public:
        // Return a success status
        static Status OK();
        /**
         * 错误状态: 读取的文件不存在
         * @param msg
         * @return
         */
        static Status FileNotFound(const std::string &msg="");

        /**
         * 错误状态: 参数有误
         */
        static Status InvalidArgument(const std::string &msg="");

        /**
         * 错误状态: IO 过程中出错
         */
        static Status IOError(const std::string &msg="");

        /**
         * IOError 子类型. 空间不足
         */
        static Status NoSpace(const std::string &msg="");

        static Status EndOfFile(const std::string &msg="");

        /**
         * 错误状态：操作的点/边不存在
         */
        static Status NotExist(const std::string &msg="");

        /**
         * 错误状态: 结果集超出设置的大小限制
         */
        static Status ResultSizeOverLimit(const std::string &msg="");

        static Status SendRecvTimeout(const std::string &msg="");
        static Status ConnectTimeout(const std::string &msg="");
        static Status PackageFormatError(const std::string &msg="");
        static Status RpcError(const std::string &msg="");
        static Status MtJobFailed(const std::string &msg="");
        static Status Timeout(const std::string &msg="");
        static Status NetworkError(const std::string &msg="");

        /**
         * 不支持存储某节点自己到自己的 loop-edge
         */
        static Status UnSupportSelfLoop(const std::string &msg="");

        /**
         * 错误状态：未实现的功能/未fix的已知bug
         */
        static Status NotImplement(const std::string &msg="");
        /**
         * 错误状态：未实现的功能/未fix的已知bug
         * 与 NotImplement 一样, 兼容旧代码
         */
        static Status NotSupported(const std::string &msg="");

        static Status Corruption(const std::string &msg="");
    private:
        explicit Status(Code _code, const std::string &msg="");
        Code m_code;
        const char *m_state;

        static const char* CopyState(const char *state);
    public:
        // copy functions
        Status(const Status &rhs);
        Status& operator=(const Status &rhs);
        // move copy functions
        Status& operator=(Status &&rhs) noexcept;
        Status(Status &&rhs) noexcept;
        // compare functions
        bool operator==(const Status &rhs) const;
        bool operator!=(const Status &rhs) const;

        friend Status FromRetStatus(const proto::idservice::RetStatus&);
    };
}

#endif //STARKNOWLEDGEGRAPH_STATUS_HPP
