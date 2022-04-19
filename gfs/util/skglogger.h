#ifndef STARKNOWLEDGEGRAPHDATABASE_SKGLOGGER_H
#define STARKNOWLEDGEGRAPHDATABASE_SKGLOGGER_H

#include "spdlog/spdlog.h"
#include "spdlog/sinks/null_sink.h"

namespace skg {

#define SKG_LOG(lvl,fmt, ...) \
    (skg::SkgLogger::GetInstance().Log(fmt, __FILE__, __func__, __LINE__, lvl, __VA_ARGS__))

#ifndef NDEBUG
// debug 模式下打log
#define SKG_LOG_TRACE(fmt, ...) \
    (SKG_LOG(spdlog::level::level_enum::trace, fmt, __VA_ARGS__))
#else
// release 模式下不干活
#define SKG_LOG_TRACE(fmt, ...) (void)(0)
#endif

#define SKG_LOG_DEBUG(fmt, ...) \
    (SKG_LOG(spdlog::level::level_enum::debug, fmt, __VA_ARGS__))
#define SKG_LOG_INFO(fmt, ...) \
    (SKG_LOG(spdlog::level::level_enum::info, fmt, __VA_ARGS__))
#define SKG_LOG_WARNING(fmt, ...) \
    (SKG_LOG(spdlog::level::level_enum::warn, fmt, __VA_ARGS__))
#define SKG_LOG_ERROR(fmt, ...) \
    (SKG_LOG(spdlog::level::level_enum::err, fmt, __VA_ARGS__))
#define SKG_LOG_FATAL(fmt, ...) \
    (SKG_LOG(spdlog::level::level_enum::critical, fmt, __VA_ARGS__))

    class SkgLogger {
    public:
        static SkgLogger& GetInstance() {
            static SkgLogger l;
            return l;
        }

        enum class level {
            trace = spdlog::level::level_enum::trace,
            debug = spdlog::level::level_enum::debug,
            info = spdlog::level::level_enum::info,
            warn = spdlog::level::level_enum::warn,
            err = spdlog::level::level_enum::err,
            fatal = spdlog::level::level_enum::critical,
        };

    public:
        SkgLogger(): m_log_to_colsole(true) {
            SetLogConsole();
            m_logger->set_level(spdlog::level::debug);
        }

        ~SkgLogger() = default;

        void SetLogConsole() {
            m_log_to_colsole = true;
            try {
		    //fix: swith between mt and st
                //m_logger = spdlog::stderr_color_st("skg_console");
                m_logger = spdlog::stderr_color_mt("skg_console");
            } catch (const spdlog::spdlog_ex& ex) {
                m_logger = spdlog::get("skg_console");
            }
            m_logger->set_pattern("%L: [%m/%d %H:%M:%S] (%P:%t) %v");
        }

        void SetLogFile(const std::string &filename) {
            m_log_to_colsole = false;
            try {
		    //fix: swith between mt and st
                //m_logger = spdlog::basic_logger_st("skg_log", filename);
                m_logger = spdlog::basic_logger_mt("skg_log", filename);
            } catch (const spdlog::spdlog_ex& ex) {
                m_logger = spdlog::get("skg_log");
            }
            m_logger->set_pattern("%L: [%m/%d %H:%M:%S] (%P:%t) %v");
        }

        inline void SetLevel(SkgLogger::level lvl) {
#ifndef PATH_SEARCH 
            this->Log("logging level is set to {}",
                      __FILE__, __FUNCTION__, __LINE__,
                      spdlog::level::level_enum::warn, spdlog::level::level_names[int(lvl)]);
#endif
            m_logger->set_level(static_cast<spdlog::level::level_enum>(lvl));
        }

        void SetLogNone() {
            m_log_to_colsole = false;
            try {
		    //fix: swith between mt and st
                //auto null_sink = std::make_shared<spdlog::sinks::null_sink_st>();
                auto null_sink = std::make_shared<spdlog::sinks::null_sink_mt>();
                m_logger = std::make_shared<spdlog::logger>("skg_null", null_sink);
            } catch (const spdlog::spdlog_ex& ex) {
                m_logger = spdlog::get("skg_null");
            }
        }

        template <typename... Args>
        void Log(const char *fmt,
                 const char *file, const char *func, int line,
                 spdlog::level::level_enum level,
                 const Args&... args) {
            // get just the filename. this line found on a forum on line.
            // claims to be from google.
            file = ((strrchr(file, '/') ? : file - 1) + 1);
            const std::string file_func_prefix = fmt::format("{}({}:{}): ", file, func, line);
            m_logger->log(level, (file_func_prefix + fmt).c_str(), args...);
            // 遇到 critical 错误, DEBUG模式下, 程序异常退出
            assert(level != spdlog::level::critical);
        }

    private:
        std::shared_ptr<spdlog::logger> m_logger;
        bool m_log_to_colsole;
    };

}

#endif //STARKNOWLEDGEGRAPHDATABASE_SKGLOGGER_H
