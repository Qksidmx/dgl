#ifndef STARKNOWLEDGEGRAPH_RAWFILEPARSER_HPP
#define STARKNOWLEDGEGRAPH_RAWFILEPARSER_HPP

#include <cstdio>
#include <string>
#include <utility>

#include "util/status.h"
#include "util/types.h"
#include "preprocessing/parse/valparser.hpp"
#include "util/StringUtils.h"
#include "util/pathutils.h"
#include "util/skglogger.h"
#include "metrics/metrics.hpp"

namespace skg { namespace preprocess {

    struct Action {
    public:
        Action() = default;
        virtual Status operator()(size_t linenum, char *line) = 0;
//        Action(const Action &) = delete;
//        Action& operator=(const Action &) = delete;
    };

    /**
     * example for creating an parse action
     */
    struct PrintAction: public Action {
    public:
        Status operator()(size_t linenum, char *line) override {
            fmt::print("{}\n", line);
            return Status::OK();
        }
    };

    /**
     * 文件格式解析基类
     * @tparam EdgeDataType
     */
    class FileParser {
    public:
        typedef std::function<Status(size_t linenum, char *line)> parse_action_t;
        static const size_t NOT_JUMP_OVER = static_cast<const size_t>(-1);
        static const size_t DEFAULT_PROGRESS = 10000000;
    public:
        /**
         *
         * @param progress_threshold    每隔多少行打印进度信息
         * @param exit_if_err           遇到一行解析错误则马上退出
         * @param
         */
        explicit
        FileParser(size_t progress_threshold=DEFAULT_PROGRESS,
                   bool exit_if_err=true, size_t num_lines_jump_over=NOT_JUMP_OVER)
                : m_inf(nullptr), m_filename(),
                  m_progress_threshold(progress_threshold),
                  m_exit_if_error(exit_if_err), m_num_lines_jump_over(num_lines_jump_over),
                  m_offset_beg(0), m_offset_end(0) {
        }
        virtual ~FileParser() {
            this->Close();
        }

        /**
         * @brief 打开文件
         * @param filename  文件路径
         * @param offset_start    如果 > 0, 则尝试偏移到 offset
         * @param offset_end    
         * @return NotFound -- 文件不存在
         *         IOError  -- 偏移失败
         */
        Status Open(const std::string &filename, off64_t offset_start=0, off64_t offset_end=0) {
            this->Close();
            m_filename = filename;
            m_inf = fopen(m_filename.c_str(), "r");
            if (m_inf == nullptr) {
                SKG_LOG_ERROR("Could not load: {} error: {}",
                              m_filename, strerror(errno));
                return Status::FileNotFound(fmt::format("input file: {}",
                                                        m_filename));
            }

            offset_start = std::max(0l, offset_start); // 开始的位置不为负数
            m_offset_beg = offset_start;
            int ret = fseeko(m_inf, offset_start, SEEK_SET);
            if (ret != 0) {
                return Status::IOError(fmt::format(
                        "Could not seek to `{}':{}, error: {}",
                        m_filename, offset_start, strerror(errno)));
            }
            if (offset_end <= 0){ // 默认读到文件尾
                m_offset_end = PathUtils::getsize(m_filename);
            } else {
                m_offset_end =  offset_end;
            }
            return Status::OK();
        }

        /**
         * 关闭文件
         */
        void Close() {
            if (m_inf != nullptr) {
                fclose(m_inf);
            }
            m_inf = nullptr;
        }

        void SetParseAction(std::function<Status(size_t linenum,char* line)> action) {
            m_parse_action = std::move(action);
        }

        /**
         * @brief 提取文件中每一行, 调用 ParseAction 进行解析, 并输出进度信息.
         * 若其中一行文件格式有错误, 则返回 s.ok() != true
         * @param msg
         * @return
         */
        Status Parse(const std::string &msg) {
            assert(m_parse_action != nullptr);
            if (m_parse_action == nullptr) {
                return Status::InvalidArgument("Have NOT set parse action. First set parse action.");
            }
            return Parse(msg, m_parse_action);
        }

        /**
         * @brief 提取文件中每一行, 调用 action 进行解析, 并输出进度信息.
         * 若其中一行文件格式有错误, 则返回 s.ok() != true
         * @tparam Func
         * @param msg
         * @param action
         * @return
         */
        template <typename Func>
        Status Parse(const std::string &msg, Func &&action) {
            assert(m_inf != nullptr);
            if (m_inf == nullptr) {
                return Status::IOError("Parser.file is nullptr. First open the file to be parsed.");
            }
            const size_t MAX_LINE_BUFFSIZE = 1 * 1024 * 1024; // 1MB
            char *s = new char[MAX_LINE_BUFFSIZE];
            size_t linenum = 0;
            size_t bytesread = 0;
            Status status;
            if (m_num_lines_jump_over != NOT_JUMP_OVER) {
                SKG_LOG_INFO("Jump over until line: {}", m_num_lines_jump_over);
            }
            double filesize_mb = 0;
            const std::string range_msg = fmt::format(
                    " offset[{:.1f}MB~{:.1f}MB], ",
                    1.0 * m_offset_beg / MB_BYTES, 1.0 * m_offset_end / MB_BYTES);
            filesize_mb = (m_offset_end - m_offset_beg) * 1.0 / MB_BYTES;
            SKG_LOG_INFO("Reading file `{}',{}mode[{}].", m_filename, range_msg, msg);
            double total_cost_secs = 0.0;
            metrics_entry timer(metrictype::TIME);
            timer.timer_start();
            off64_t curOffset = ftello(m_inf);
            while (true) {
                if (feof(m_inf) || curOffset >= m_offset_end) {
                    break;
                }
                if (fgets(s, MAX_LINE_BUFFSIZE, m_inf) == nullptr) {
                    break;
                }
                // 进度信息打印
                if (linenum % m_progress_threshold == 0 && linenum != 0) {
                    double cost_secs = 0.0;
                    timer.timer_stop(&cost_secs);
                    timer.timer_start();
                    total_cost_secs += cost_secs;
                    const double bytesread_mb = bytesread * 1.0 / (1024 * 1024);
                    SKG_LOG_INFO("Read {} lines, {:.1f}/{:.1f} MB ({:.1f}%), "
                                 "{:.2f} lines/sec, total {:.2f} lines/sec",
                                 linenum, bytesread_mb, filesize_mb,
                                 100 * bytesread_mb / filesize_mb,
                                 m_progress_threshold / cost_secs,
                                 linenum / total_cost_secs);
                }
                bytesread += strlen(s);

                // 是否需要跳过, 不解析(为了断点之后, 能够从文件的中间某行开始解析数据)
                if (m_num_lines_jump_over != NOT_JUMP_OVER && linenum < m_num_lines_jump_over) {
                    ++linenum;
                    continue;
                }
                if (m_num_lines_jump_over != NOT_JUMP_OVER && linenum == m_num_lines_jump_over) {
                    // log 已经结束跳过的阶段, 开始真正解析数据。
                    SKG_LOG_INFO("Jump over done. Begin to parse lines @ {}", linenum);
                }

                // 处理 BOM 头
                if (linenum == 0) { StringUtils::FIXBOM(s); }
                // 处理行尾换行符
                StringUtils::FIXLINE(s);
                // 执行解析的回调函数
                status = action(linenum, s);
                if (!status.ok()) {
                    if (status.code() == Status::Code::UNSUPPORT_SELF_LOOP) {
                        // ignore error
                        SKG_LOG_DEBUG("warning: {}, file: `{}', linenum: {}", status.ToString(), m_filename, linenum);
                        ++linenum;
                        continue;
                    }
                    //SKG_LOG_WARNING("Parse error: {}", status.ToString());
                    if (m_exit_if_error) { // 如果遇到解析错误, 马上退出文件解析
                        break;
                    }
                }
                ++linenum;
                curOffset = ftello(m_inf);
            }
            double cost_secs = 0.0;
            timer.timer_stop(&cost_secs);
            total_cost_secs += cost_secs;
            // 结束读入文件
            SKG_LOG_INFO("File `{}' read,{}{} lines, {:.1f}/{:.1f} MB ({:.1f}%)"
                         ", total {:.2f} lines/sec",
                         m_filename, range_msg,
                         linenum, filesize_mb, filesize_mb, 100.0,
                         linenum / total_cost_secs);
            delete []s;
            if (!status.ok()) {
                // 再判断是否可忽略的错误
                if (status.code() == Status::Code::UNSUPPORT_SELF_LOOP) {
                    return Status::OK();
                } else if (!m_exit_if_error) {
                    return Status::OK();
                } else {
                    return status;
                }
            } else {
                return status;
            }
        }
    public:
        // 禁止复制
        FileParser(const FileParser &) = delete;
        FileParser& operator=(const FileParser &) = delete;

    protected:
        FILE *m_inf;
        std::string m_filename;
        parse_action_t m_parse_action;
        size_t m_progress_threshold;
        bool m_exit_if_error;
        size_t m_num_lines_jump_over;
        off64_t m_offset_beg;
        off64_t m_offset_end;
    };

    // TODO CassovaryFileParser
    // TODO MetisFileParser
}}

#endif //STARKNOWLEDGEGRAPH_RAWFILEPARSER_HPP
