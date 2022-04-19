#ifndef STARKNOWLEDGEGRAPH_STRINGUTILS_HPP
#define STARKNOWLEDGEGRAPH_STRINGUTILS_HPP


#include <string.h>
#include <assert.h>

#include <string>
#include <vector>
#include <regex>
#include "slice.h"
#include "skglogger.h"

#include "status.h"
#include "fmt/format.h"

class StringUtils {
public:
    /**
     * 字符串分割函数
     * @param str
     * @param pattern
     * @param result    存放结果的vector
     * @param max_split 默认为-1, 遇到每个pattern都进行分割
     *                   若为非负值, 则最多分割max_split个pattern, 即vector中最多有 max_split+1 个字符串
     *  eg. split("1,2,3,4", ",")    -> ("1", "2", "3", "4")
     *      split("1,2,3,4", ",", 0) -> ("1,2,3,4")
     *      split("1,2,3,4", ",", 1) -> ("1", "2,3,4")
     *      split("1,2,3,4", ",", 2) -> ("1", "2", "3,4")
     *      split("1,2,3,4", ",", 3) -> ("1", "2", "3", "4")
     *      split("1,2,3,4", ",", 4) -> ("1", "2", "3", "4")
     *      一些边界情况:
     *      split("1,2,3,4,", ",")   -> ("1", "2", "3", "4", "")
     *      split(",", ",")          -> ("", "")
     *      split("", " ")           -> ("", )
     * @return 0 正常
     */
    static
    int split(const std::string &str, const char pattern,
              std::vector<std::string> &result,
              const int max_split=-1);

    static
    std::vector<skg::Slice>
            split(const skg::Slice &str, const char pattern,
                  const int max_split=-1);

    // Code for trimming strings copied from + modified
    // http://stackoverflow.com/questions/479080/trim-is-not-part-of-the-standard-c-c-library
    static const std::string whiteSpaces;

    inline static
    bool IsValidChar(char ch) {
        return ('A' <= ch && ch <= 'Z') || ('a' <= ch && ch <= 'z') || ('0' <= ch && ch <= '9')
               || (ch == '_') || (ch == '.') || (ch == '-');
    }

    inline static
    bool IsValidName(const skg::Slice &slice) {
        if (slice.empty()) {
            return false;
        }
        for (size_t i = 0; i < slice.size(); ++i) {
            if (!IsValidChar(slice[i])) {
                return false;
            }
        }
        return true;
    }

    // Removes \r\n from the end of line
    inline static
    void FIXLINE(char * s) {
        size_t len = strlen(s) - 1;
        if(s[len] == '\n' || s[len] == '\r') s[len] = '\0'; // "\n" for *nix files
        if(s[len - 1] == '\r') s[len - 1] = '\0'; // "\r\n" for windows files
    }

    inline static
    void FIXBOM(char *s) {
        size_t len = strlen(s) - 1;
        if (len >= 3 && s[0] == '\xef' && s[1] == '\xbb' && s[2] == '\xbf') {
            for (size_t i = 0; i < len - 1; ++i) {
                s[i] = s[i + 3];
            }
        }
    }

    inline static
    std::string& trimRight(std::string &str,
                           const std::string& trimChars) {
        std::string::size_type pos = str.find_last_not_of(trimChars);
        str.erase( pos + 1 );
        return str;
    }

    inline static
    std::string& trimLeft(std::string &str,
                          const std::string& trimChars) {
        std::string::size_type pos = str.find_first_not_of(trimChars);
        str.erase( 0, pos );
        return str;
    }

    inline static
    std::string& trim(std::string &str) {
        trimRight(str, whiteSpaces);
        trimLeft(str, whiteSpaces);
        return str;
    }

    inline static
    std::string trim(const char *str) {
        std::string s(str);
        return trim(s);
    }

    inline static
    uint32_t ParseUint32(const std::string &value, skg::Status *s) {
        uint64_t num = 0;
        try {
            num = ParseUint64(value);
        } catch (std::exception const &e) {
            *s = skg::Status::InvalidArgument(fmt::format("invalid long: {}", value));
            return 0;
        }
        if ((num >> 32LL) == 0) {
            return static_cast<uint32_t>(num);
        } else {
            if (s != nullptr) {
                *s = skg::Status::InvalidArgument(fmt::format("out of range: {}", value));
            }
            // FIXME 解析错误, ignore, 返回 0
            return 0;
//            throw std::out_of_range(value);
        }
    }

    inline static
    int32_t ParseInt32(const std::string &value) {
        return static_cast<int32_t>(strtol(value.c_str(), nullptr, 10));
    }

    inline static
    uint32_t ParseUint32(const std::string& value) {
        return ParseUint32(value, nullptr);
    }

    inline static
    uint64_t ParseUint64(const std::string& value) {
        size_t endchar;
#ifndef CYGWIN
        uint64_t num = std::stoull(value, &endchar);
#else
        char* endptr;
        uint64_t num = std::strtoul(value.c_str(), &endptr, 0);
        endchar = endptr - value.c_str();
#endif

        if (endchar < value.length()) {
            char c = value[endchar];
            if (c == 'k' || c == 'K')
                num <<= 10LL;
            else if (c == 'm' || c == 'M')
                num <<= 20LL;
            else if (c == 'g' || c == 'G')
                num <<= 30LL;
            else if (c == 't' || c == 'T')
                num <<= 40LL;
        }

        return num;
    }

#if __cplusplus < 201103L
// before c++ 11, use private to delete construct functions
private:
    StringUtils();
    StringUtils(const StringUtils &);
    StringUtils& operator=(const StringUtils&);
#else
    StringUtils() = delete;
    StringUtils(const StringUtils &) = delete;
    StringUtils& operator=(const StringUtils&) = delete;
#endif
};

#endif //STARKNOWLEDGEGRAPH_STRINGUTILS_HPP
