#include "status.h"
#include "edge_list_mmap_reader.h"

#include "fmt/format.h"

namespace gfs {
    Status::Status() : m_code(Code::OK), m_state(nullptr) {}
    Status::~Status() {
        delete [] m_state;
    }

    Status::Code Status::code() const { return m_code; }
    bool Status::ok() const { return (m_code == Code::OK || m_code == Code::RESULT_SIZE_OVER_LIMIT); }
    bool Status::IsOverLimit() const { return m_code == Code::RESULT_SIZE_OVER_LIMIT; }
    bool Status::IsFileNotFound() const { return code() == Code::FILE_NOT_FOUND; }
    bool Status::IsInvalidArgument() const { return m_code == Code::INVALID_ARGUMENT; }
    bool Status::IsNotExist() const { return m_code == Code::NOT_EXIST; }

    std::string Status::ToString() const {
        std::string result;
        switch (m_code) {
            case Code::OK:
                return "OK";
            case Code::FILE_NOT_FOUND:
                result = "File NotFound: ";
                break;
            case Code::INVALID_ARGUMENT:
                result = "Invalid argument: ";
                break;
            case Code::IO_ERROR:
                result = "IO error: ";
                break;
            case Code::NOT_EXIST:
                result = "Not exist: ";
                break;
            case Code::SEND_RECV_TIMEOUT:
                result = "Socket Send Or Recv Timeout: ";
                break;
            case Code::CONNECT_TIMEOUT:
                result = "Connect To Server Timeout: ";
                break;
            case Code::TIMEOUT:
                result = "Timeout: ";
                break;
            case Code::NETWORK_ERROR:
                result = "Network Error: ";
                break;
            case Code::PACKAGE_FORMAT_ERROR:
                result = "Invalid Package Format: ";
                break;
            case Code::RPC_ERROR:
                result = "Rpc Error: ";
                break;
            case Code::MT_JOB_FAILED:
                result = "Micro Thread Job Failed: ";
                break;
            case Code::NOT_IMPLEMENT:
                result = "Not Implement: ";
                break;
            case Code::UNSUPPORT_SELF_LOOP:
                result = "Not support self-loop-edges: ";
                break;
            case Code::CORRUPTION:
                result = "Corruption: ";
                break;
            default:
                result = fmt::format("Unknown code({}): ",
                                     static_cast<int>(m_code));
                break;
        }
        if (m_state != nullptr) {
            result.append(m_state);
        }
        return result;
    }

    Status Status::OK() { return Status(); }
    Status Status::FileNotFound(const std::string &msg) { return Status(Code::FILE_NOT_FOUND, msg); }
    Status Status::InvalidArgument(const std::string &msg) { return Status(Code::INVALID_ARGUMENT, msg); }
    Status Status::IOError(const std::string &msg) { return Status(Code::IO_ERROR, msg); }
    Status Status::NoSpace(const std::string &msg) { return Status(Code::IO_ERROR, fmt::format("No space left on device. {}", msg)); }
    Status Status::NotExist(const std::string &msg) { return Status(Code::NOT_EXIST, msg); }
    Status Status::NotImplement(const std::string &msg) { return Status(Code::NOT_IMPLEMENT, msg); }
    Status Status::NotSupported(const std::string &msg) { return Status(Code::NOT_IMPLEMENT, msg); }
    Status Status::EndOfFile(const std::string &msg) { return Status(Code::END_OF_FILE, msg); }
    Status Status::ResultSizeOverLimit(const std::string &msg) { return Status(Code::RESULT_SIZE_OVER_LIMIT, msg); }

    Status Status::SendRecvTimeout(const std::string &msg){ return Status(Code::SEND_RECV_TIMEOUT, msg); }
    Status Status::Timeout(const std::string &msg){ return Status(Code::TIMEOUT, msg); }
    Status Status::NetworkError(const std::string &msg){ return Status(Code::NETWORK_ERROR, msg); }
    Status Status::ConnectTimeout(const std::string &msg){ return Status(Code::CONNECT_TIMEOUT, msg); }
    Status Status::PackageFormatError(const std::string &msg){ return Status(Code::PACKAGE_FORMAT_ERROR, msg); }
    Status Status::RpcError(const std::string &msg){ return Status(Code::RPC_ERROR, msg); }
    Status Status::MtJobFailed(const std::string &msg){ return Status(Code::MT_JOB_FAILED, msg); }
    Status Status::UnSupportSelfLoop(const std::string &msg) { return Status(Code::UNSUPPORT_SELF_LOOP, msg); }
    Status Status::Corruption(const std::string &msg) { return Status(Code::CORRUPTION, msg); }

    Status::Status(Status::Code _code, const std::string &msg)
            : m_code(_code), m_state(nullptr) {
        if (!msg.empty()) {
            char *const result = new char[msg.size() + 1];
            memcpy(result, msg.data(), msg.size());
            result[msg.size()] = '\0';
            m_state = result;
        }
    }

    const char* Status::CopyState(const char *state) {
        char *const result = new char[strlen(state) + 1];
        strcpy(result, state);
        return result;
    }

    // copy functions
    Status::Status(const Status &rhs): m_code(rhs.m_code), m_state(nullptr) {
        m_state = (rhs.m_state == nullptr) ? nullptr : CopyState(rhs.m_state);
    }
    Status& Status::operator=(const Status &rhs) {
        if (this != &rhs) {
            m_code = rhs.m_code;
            delete[] m_state;
            m_state = (rhs.m_state == nullptr) ? nullptr : CopyState(rhs.m_state);
        }
        return *this;
    }

    // move copy functions
    Status& Status::operator=(Status &&rhs) noexcept {
        if (this != &rhs) {
            m_code = rhs.m_code;
            rhs.m_code = Code::OK;
            delete[] m_state;
            m_state = nullptr;
            std::swap(m_state, rhs.m_state);
        }
        return *this;
    }
    Status::Status(Status &&rhs) noexcept : Status() {
        *this = std::move(rhs);
    }

    // compare functions
    bool Status::operator==(const Status &rhs) const {
        return m_code == rhs.m_code;
    }
    bool Status::operator!=(const Status &rhs) const {
        return !(*this == rhs);
    }
}
