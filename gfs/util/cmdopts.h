/*
 */

#ifndef GRAPHCHI_CMDOPTS_DEF
#define GRAPHCHI_CMDOPTS_DEF


#include <stdint.h>
#include <string>
#include <iostream>
#include <map>

#include "StringUtils.h"

namespace skg {
    
    /** GNU COMPILER HACK TO PREVENT IT FOR COMPILING METHODS WHICH ARE NOT USED IN
        THE PARTICULAR APP BEING BUILT */
#ifdef __GNUC__
#define VARIABLE_IS_NOT_USED __attribute__ ((unused))
#else
#define VARIABLE_IS_NOT_USED
#endif

    class Config {
    private:
        static Config *pInstance;
    public:
        int argc;
        char **argv;
        std::map<std::string, std::string> conf;

//    private:
    public:
        bool cmd_configured;

    public:
        Config(): argc(0), argv(nullptr), conf(), cmd_configured(false) {}
    public:
        static Config &instance() {
            if (pInstance == nullptr) {
                pInstance = new Config();
            }
            return *pInstance;
        }

        /**
         * Configuration file name
         */
        static std::string filename_config() {
            char * chi_root = getenv("SKG_ROOT");
            if (chi_root != nullptr) {
                return std::string(chi_root) + "/conf/skg.cnf";
            } else {
                return "conf/skg.cnf";
            }
        }

        /**
         * Configuration file name - local version which can
         * override the version in the version control.
         */
        static std::string filename_config_local() {
            char * chi_root = getenv("SKG_ROOT");
            if (chi_root != nullptr) {
                return std::string(chi_root) + "/conf/skg.local.cnf";
            } else {
                return "conf/skg.local.cnf";
            }
        }

        void CheckInited() {
//            if (!cmd_configured) {
//                std::cout << "ERROR: command line options not initialized." << std::endl;
//                std::cout << "       You need to call skg_init() in the beginning of the program." << std::endl;
//            }
        }

        /**
         * Parse a key-value map of a configuration file key-values.
         * If file is not found, fails with an assertion.
         * @param filename filename of the configuration file
         * @param secondary_filename secondary filename if the first version is not found.
         */
        void ParseInlineOpts() {
                    conf["loadthreads"] = "4";
                    conf["nioreads"] = "2";
                    conf["shelter_threads "] = "2";
                    conf["io.blocksize"] = "1048576";
                    conf["mmap"] = "0";
                    conf["cachesize_mb"] = "0";
                    conf["db_dir"] = "./db";
                    conf["loglevel"] = "debug";
                    conf["shard_size_mb"] = "3";
                    conf["mem_buffer_mb"] = "128";
                    conf["edata_cache_mb"] = "128";
	            conf["mem-table"] = "hash";
		    conf["use_mmap_read"]="1";
		    conf["use_mmap_populate"]="0";
		    conf["use_mmap_locked"] = "0";
		    conf["use_elias_gamma_index"] = "0";
	}

        void ParseFileOpts(const std::string &filename, const std::string &secondary_filename) {
            FILE * f = fopen(filename.c_str(), "r");
            if (f == nullptr) {
                f = fopen(secondary_filename.c_str(), "r");
                if (f == nullptr) {
                    SKG_LOG_WARNING("{} not exists, using inline options",filename);
		    ParseInlineOpts();
		    return;
                }
            }

            // I like C parsing more than C++, that is why this is such a mess
            char s[4096];
            while(fgets(s, 4096, f) != nullptr) {
                StringUtils::FIXLINE(s);
                if (s[0] == '#') continue; // Comment
                if (s[0] == '%') continue; // Comment

                char delims[] = "=";
                char *t = strtok(s, delims);
                const char * ckey = t;
                t = strtok(nullptr, delims);
                const char * cval = t;

                if (ckey != nullptr && cval != nullptr) {
                    std::string key = StringUtils::trim(ckey);
                    std::string val = StringUtils::trim(cval);
                    conf[key] = val;
                }
            }

            fclose(f);
        }

        void ParseCmdOpts(int argc, char **argv) {
            /* Load --key=value type arguments into the conf map */
            this->argc = argc;
            this->argv = argv;
            this->cmd_configured = true;
            const std::string prefix = "--";
            for (int i = 1; i < argc; i++) {
                std::string arg = std::string(Config::instance().argv[i]);

                if (arg.substr(0, prefix.size()) == prefix) {
                    arg = arg.substr(prefix.size());
                    size_t a = arg.find_first_of('=', 0);
                    if (a != std::string::npos) {
                        std::string key = arg.substr(0, a);
                        std::string val = arg.substr(a + 1);

                        Config::instance().conf[key] = val;
                    }
                }
            }
        }

        void PrintOpts(bool logging) {
            if (logging) {
                SKG_LOG_INFO("Config:", "");
            } else {
                fmt::print("Config:\n");
            }
            for (auto &iter : conf) {
                if (logging) {
                    SKG_LOG_INFO("[{}] => [{}]", iter.first, iter.second);
                } else {
                    fmt::print("[{}] => [{}]\n", iter.first, iter.second);
                }
            }
            if (logging) {
                SKG_LOG_INFO("=========", "");
            } else {
                fmt::print("=========\n");
            }
        }

        inline
        bool IsKeyExist(const std::string &key) const {
            return conf.find(key) != conf.end();
        }

        inline
        void SetConf(const std::string &key, const std::string &value) {
            conf[key] = value;
        }
    };

// Config file
static std::string VARIABLE_IS_NOT_USED get_config_option_string(const char *option_name) {
    if (Config::instance().IsKeyExist(option_name)) {
        return Config::instance().conf[option_name];
    } else {
        std::cout << "ERROR: option `" << option_name << "` is required." << std::endl;
        assert(false);
        return "";
    }
}

static std::string VARIABLE_IS_NOT_USED get_config_option_string(
        const char *option_name, std::string default_value) {
    if (Config::instance().IsKeyExist(option_name)) {
        return Config::instance().conf[option_name];
    } else {
        return default_value;
    }

}
static int VARIABLE_IS_NOT_USED get_config_option_int(const char *option_name, int default_value) {
    if (Config::instance().IsKeyExist(option_name)) {
        return StringUtils::ParseInt32(Config::instance().conf[option_name]);
    } else {
        return default_value;
    }
}

static int VARIABLE_IS_NOT_USED get_config_option_int(const char *option_name) {
    if (Config::instance().IsKeyExist(option_name)) {
        return StringUtils::ParseInt32(Config::instance().conf[option_name]);
    } else {
        std::cout << "ERROR: option `" << option_name << "` is required." << std::endl;
        assert(false);
        return 0;
    }
}

static uint64_t VARIABLE_IS_NOT_USED get_config_option_long(const char *option_name, uint64_t default_value) {
    if (Config::instance().IsKeyExist(option_name)) {
        return StringUtils::ParseUint64(Config::instance().conf[option_name]);
    } else {
        return default_value;
    }
}
static double VARIABLE_IS_NOT_USED get_config_option_double(const char *option_name, double default_value) {
    if (Config::instance().IsKeyExist(option_name)) {
        return atof(Config::instance().conf[option_name].c_str());
    } else {
        return default_value;
    }
}

/**
 * @brief 优先找命令行中设置的 `option_name` 参数. 如果找不到, 再到配置文件的参数中查找
 * @param option_name
 * @param default_value
 * @return
 */
static std::string VARIABLE_IS_NOT_USED get_option_string(
        const char *option_name, const std::string &default_value)
{
    Config::instance().CheckInited();
    int i;

    for (i = Config::instance().argc - 2; i >= 0; i -= 1)
        if (strcmp(Config::instance().argv[i], option_name) == 0)
            return std::string(Config::instance().argv[i + 1]);
    return get_config_option_string(option_name, default_value);
}

/**
 * @brief 优先找命令行中设置的 `option_name` 参数. 如果找不到, 再到配置文件的参数中查找
 * @param option_name
 * @return
 */
static std::string VARIABLE_IS_NOT_USED get_option_string(const char *option_name)
{
    Config::instance().CheckInited();

    for (int i = Config::instance().argc - 2; i >= 0; i -= 1)
        if (strcmp(Config::instance().argv[i], option_name) == 0)
            return std::string(Config::instance().argv[i + 1]);
    return get_config_option_string(option_name);
}

/**
 * @brief 优先找命令行中设置的 `option_name` 参数. 如果找不到, 再到配置文件的参数中查找. 都找不到则由用户输入
 * @param option_name
 * @return
 */
static std::string VARIABLE_IS_NOT_USED get_option_string_interactive(const char *option_name, const std::string &options)
{
    Config::instance().CheckInited();

    for (int i = Config::instance().argc - 2; i >= 0; i -= 1)
        if (strcmp(Config::instance().argv[i], option_name) == 0)
            return std::string(Config::instance().argv[i + 1]);
    if (Config::instance().IsKeyExist(option_name)) {
        return Config::instance().conf[option_name];
    }

    std::cout << "Please enter value for command-line argument [" << std::string(option_name) << "]"<< std::endl;
    std::cout << "  (Options are: " << options << ")" << std::endl;

    std::string val;
    std::cin >> val;

    return val;
}

/**
 * @brief 优先找命令行中设置的 `option_name` 参数. 如果找不到, 再到配置文件的参数中查找
 * @param option_name
 * @return
 */
static uint32_t VARIABLE_IS_NOT_USED get_option_uint(const char *option_name)
{
    Config::instance().CheckInited();

    for (int i = Config::instance().argc - 2; i >= 0; i -= 1)
        if (strcmp(Config::instance().argv[i], option_name) == 0)
            return StringUtils::ParseUint32(Config::instance().argv[i + 1]);

    return static_cast<uint32_t>(get_config_option_int(option_name));
}

/**
 * @brief 优先找命令行中设置的 `option_name` 参数. 如果找不到, 再到配置文件的参数中查找
 * @param option_name
 * @return
 */
static int VARIABLE_IS_NOT_USED get_option_int(const char *option_name, int default_value)
{
    Config::instance().CheckInited();

    for (int i = Config::instance().argc - 2; i >= 0; i -= 1)
        if (strcmp(Config::instance().argv[i], option_name) == 0)
            return StringUtils::ParseInt32(Config::instance().argv[i + 1]);

    return get_config_option_int(option_name, default_value);
}

static uint32_t VARIABLE_IS_NOT_USED get_option_uint(const char *option_name, int default_value)
{
    // FIXME
    return static_cast<uint32_t>(get_option_int(option_name, default_value));
}

/**
 * @brief 优先找命令行中设置的 `option_name` 参数. 如果找不到, 再到配置文件的参数中查找
 * @param option_name
 * @return
 */
static int VARIABLE_IS_NOT_USED get_option_int(const char *option_name)
{
    Config::instance().CheckInited();

    for (int i = Config::instance().argc - 2; i >= 0; i -= 1)
        if (strcmp(Config::instance().argv[i], option_name) == 0)
            return StringUtils::ParseUint32(Config::instance().argv[i + 1]);

    return get_config_option_int(option_name);

}

/**
 * @brief 优先找命令行中设置的 `option_name` 参数. 如果找不到, 再到配置文件的参数中查找
 * @param option_name
 * @return
 */
static uint64_t VARIABLE_IS_NOT_USED get_option_long(const char *option_name, uint64_t default_value)
{
    Config::instance().CheckInited();

    for (int i = Config::instance().argc - 2; i >= 0; i -= 1)
        if (strcmp(Config::instance().argv[i], option_name) == 0)
            return atol(Config::instance().argv[i + 1]);
    return get_config_option_long(option_name, default_value);
}

/**
 * @brief 优先找命令行中设置的 `option_name` 参数. 如果找不到, 再到配置文件的参数中查找
 * @param option_name
 * @return
 */
static float VARIABLE_IS_NOT_USED get_option_float(const char *option_name, float default_value)
{
    Config::instance().CheckInited();

    for (int i = Config::instance().argc - 2; i >= 0; i -= 1)
        if (strcmp(Config::instance().argv[i], option_name) == 0)
            return (float)atof(Config::instance().argv[i + 1]);
    return (float) get_config_option_double(option_name, default_value);
}

inline void skg_init(int argc, char **argv, bool verbose=false,
        const std::map<std::string, std::string> &opts={}) {
    if (!Config::instance().cmd_configured) {
        Config::instance().ParseFileOpts(Config::filename_config_local(), Config::filename_config());
        Config::instance().ParseCmdOpts(argc, argv);
        for (const auto &iter: opts) {
            Config::instance().conf[iter.first] = iter.second;
        }
    }
    // 初始化 logging 文件
    const std::string logfile = get_option_string("logfile", "");
    if (!logfile.empty()) {
        SkgLogger::GetInstance().SetLogFile(logfile);
    }
    // 初始化 logging level
    SkgLogger::level level = SkgLogger::level::debug;
    const std::string lvl = get_option_string("loglevel", "debug");
    if (strncasecmp(lvl.c_str(), "trace", strlen("trace")) == 0) {
        level = SkgLogger::level::trace;
    } else if (strncasecmp(lvl.c_str(), "debug", strlen("debug")) == 0) {
        level = SkgLogger::level::debug;
    } else if (strncasecmp(lvl.c_str(), "info", strlen("info")) == 0) {
        level = SkgLogger::level::info;
    } else if (strncasecmp(lvl.c_str(), "warn", strlen("warn")) == 0) {
        level = SkgLogger::level::warn;
    } else if (strncasecmp(lvl.c_str(), "err", strlen("err")) == 0) {
        level = SkgLogger::level::err;
    }
    SkgLogger::GetInstance().SetLevel(level);
    if (verbose) Config::instance().PrintOpts(true);
}

} // End namespace


#endif


