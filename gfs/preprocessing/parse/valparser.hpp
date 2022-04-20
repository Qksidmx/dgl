#ifndef STARKNOWLEDGEGRAPHDATABASE_VALPARSER_HPP
#define STARKNOWLEDGEGRAPHDATABASE_VALPARSER_HPP

#include <cstdio>
#include "util/skglogger.h"

//#include "util/skglogger.h"
#include "util/types.h"
#include "util/internal_types.h"

/**
 * GNU COMPILER HACK TO PREVENT WARNINGS "Unused variable", if
 * the particular app being compiled does not use a function.
 */
#ifdef __GNUC__
#define VARIABLE_IS_NOT_USED __attribute__ ((unused))
#else
#define VARIABLE_IS_NOT_USED
#endif


namespace skg {

    /* Simple string to number parsers */

    static VARIABLE_IS_NOT_USED vid_t parseVid(const char * s) {
        return static_cast<vid_t>(strtol(s, nullptr, 10));
    }

    static VARIABLE_IS_NOT_USED int64_t parseTimestamp(const char * s) {
        return static_cast<int64_t>(strtoll(s, nullptr, 10));        
    }

    static VARIABLE_IS_NOT_USED void parse(int &x, const char * s) {
        x = static_cast<int>(strtol(s, nullptr, 10));
    }

    static VARIABLE_IS_NOT_USED void parse(unsigned int &x, const char * s) {
        x = static_cast<unsigned int>(strtoul(s, nullptr, 10));
    }

    static VARIABLE_IS_NOT_USED void parse(float &x, const char * s) {
        x = strtof(s, nullptr);
    }

    /**
     * Special templated parser for PairContainers.
     */
    template <typename T>
    void parse(PairContainer<T> &x, const char * s) {
        parse(x.left, s);
        parse(x.right, s);
    }

    static VARIABLE_IS_NOT_USED void parse(long &x, const char * s) {
        x = strtol(s, nullptr, 10);
    }

    static VARIABLE_IS_NOT_USED void parse(char &x, const char * s) {
        x = s[0];
    }

    static VARIABLE_IS_NOT_USED void parse(uint8_t &x, const char *s) {
        x = static_cast<uint8_t>(strtol(s, nullptr, 10));
    }

    static VARIABLE_IS_NOT_USED void parse(bool &x, const char * s) {
        x = strtol(s, nullptr, 10) == 1;
    }

    static VARIABLE_IS_NOT_USED void parse(double &x, const char * s) {
        x = strtod(s, nullptr);
    }

    static VARIABLE_IS_NOT_USED void parse(short &x, const char * s) {
        x = static_cast<short>(strtol(s, nullptr, 10));
    }

    // Catch all
    template <typename T>
    void parse(T &x, const char * s) {
        SKG_LOG_ERROR("You need to define parse<your-type>(your-type &x, const char *s) function"
                      " to support parsing the edge value.", "");
        assert(false);
    }

}

#endif //STARKNOWLEDGEGRAPHDATABASE_VALPARSER_HPP
