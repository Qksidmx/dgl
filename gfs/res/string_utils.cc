
#include "string_utils.h"

const std::string StringUtils::whiteSpaces(" \f\n\r\t\v");

int
StringUtils::split(const std::string &str, const char pattern, std::vector<std::string> &result, const int max_split) {
    result.clear();

    assert(max_split == -1 || max_split >= 0);  // max_split 不应为-1以外的负值

    size_t numPattern = 0;

    size_t begPos = 0,
            endPos = 0;
    while ((endPos = str.find(pattern, begPos)) != std::string::npos) {
        if (max_split != -1 && numPattern == static_cast<size_t>(max_split)) {
            // printf("[Max] Push `%s`\n", str.substr(begPos).c_str());
            result.push_back(str.substr(begPos));
            return 0;
        }
        //printf("(%zu, %zu) `%s`\n",
        //         begPos, endPos,
        //         str.substr(begPos, endPos - begPos).c_str());
#if __cplusplus < 201103L
        result.push_back(str.substr(begPos, endPos - begPos));
#else
        result.emplace_back(str.substr(begPos, endPos - begPos));
#endif
        begPos = endPos + 1;
        numPattern += 1;
    }
#if __cplusplus < 201103L
    result.push_back(str.substr(begPos));
#else
    result.emplace_back(str.substr(begPos));
#endif
    return 0;
}

std::vector<Slice>
        StringUtils::split(const Slice &str, const char pattern, const int max_split) {
    std::vector<Slice> result;
    assert(max_split == -1 || max_split >= 0);  // max_split 不应为-1以外的负值

    size_t numPattern = 0;

    size_t begPos = 0,
            endPos = 0;
    while (true) {
        endPos = begPos;
        for (; endPos < str.size(); ++endPos) {
            if (str[endPos] == pattern) { break; }
        }

        if (endPos == str.size()) { break; }

        if (max_split != -1 && numPattern == static_cast<size_t>(max_split)) {
            // printf("[Max] Push `%s`\n", str.substr(begPos).c_str());
#if __cplusplus < 201103L
            result.push_back(Slice(str.data_ + begPos));
#else
            result.emplace_back(str.data_ + begPos);
#endif
            return result;
        }
        //printf("(%zu, %zu) `%s`\n",
        //         begPos, endPos,
        //         str.substr(begPos, endPos - begPos).c_str());
#if __cplusplus < 201103L
        result.push_back(Slice(str.data_ + begPos, endPos - begPos));
#else
        result.emplace_back(str.data_ + begPos, endPos - begPos);
#endif
        begPos = endPos + 1;
        numPattern += 1;
    }
#if __cplusplus < 201103L
    result.push_back(Slice(str.data_ + begPos));
#else
    result.emplace_back(str.data_ + begPos);
#endif
    return result;
}
// NOLINT
