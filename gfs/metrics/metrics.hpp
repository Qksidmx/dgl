
/**
 * @file
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 
 *
 * @section DESCRIPTION
 *
 * Metrics. 
 */


#ifndef DEF_METRICS_HPP
#define DEF_METRICS_HPP

#include <cstring>
#include <map>
#include <vector>
#include <limits>
#include <assert.h>
#include <sys/time.h>

#include <chrono>

#include "util/pthread_tools.hpp"
#include "util/cmdopts.h"

namespace skg {


    enum class metrictype {
        REAL, INTEGER, TIME, STRING, VECTOR
    };

    enum class metric_duration_type {
        SECONDS,
        MILLISECONDS,
    };

    // Data structure for storing metric entries
    // NOTE: This data structure is not very optimal, should
    // of course use inheritance. But for this purpose,
    // it works fine as the number of metrics entry is small.
    struct metrics_entry {
        size_t count;
        double value;
        double minvalue;
        double maxvalue;
        double cumvalue;
        metrictype valtype;
        std::string stringval;
        std::vector<double> v;
        timeval start_time;
        double lasttime;
        metric_duration_type duration_type;

        metrics_entry()
                : count(0), value(0.0),
                  minvalue(std::numeric_limits<double>::max()),
                  maxvalue(std::numeric_limits<double>::min()),
                  cumvalue(0.0),
                  valtype(metrictype::INTEGER),
                  stringval(),
                  v(),
                  start_time(), lasttime(0.0), duration_type(metric_duration_type::SECONDS) {
        }

        inline metrics_entry(double firstvalue, metrictype _valtype)
                : count(1), value(firstvalue),
                  minvalue(firstvalue), maxvalue(firstvalue), cumvalue(value),
                  valtype(_valtype),
                  stringval(),
                  v(),
                  start_time(), lasttime(0.0), duration_type(metric_duration_type::SECONDS) {
            if (valtype == metrictype::VECTOR) v.push_back(firstvalue);
        }

        explicit
        inline metrics_entry(const std::string &svalue)
                : count(1), value(0.0),
                    minvalue(0.0), maxvalue(0.0), cumvalue(value),
                    valtype(metrictype::STRING),
                    stringval(svalue),
                    v(),
                    start_time(), lasttime(0.0), duration_type(metric_duration_type::SECONDS) {
        }

        explicit
        inline metrics_entry(
                const metrictype &_valtype,
                const metric_duration_type &_duration_type=metric_duration_type::SECONDS)
                : count(0), value(0.0),
                  minvalue(std::numeric_limits<double>::max()),
                  maxvalue(std::numeric_limits<double>::min()),
                  cumvalue(0.0),
                  valtype(_valtype),
                  stringval(),
                  v(),
                  start_time(), lasttime(0.0), duration_type(_duration_type) {
        }

        inline void adj(double v) {
            if (count == 0) {
                minvalue = v;
                maxvalue = v;
            } else {
                minvalue = std::min(v, minvalue);
                maxvalue = std::max(v, maxvalue);
            }
        }

        inline void add(double x) {
            adj(x);
            value += x;
            cumvalue += x;
            ++count;
            if (valtype == metrictype::VECTOR) {
                v.push_back(x);
            }
        }

        inline void set(double v) {
            adj(v);
            value = v;
            cumvalue += v;
        }

        inline void set(const std::string &s) {
            stringval = s;
        }

        inline void add_vector_entry(size_t i, double x) {
            if (v.size() < i + 1) v.resize(i + 1);
            count = v.size();
            value += x;
            cumvalue += x;
            v[i] += x;
            adj(v[i]);
        }

        inline void set_vector_entry(size_t i, double x) {
            if (v.size() < i + 1) v.resize(i + 1);
            count = v.size();
            value = value - v[i] + x;
            cumvalue = cumvalue - v[i] + x;
            v[i] = x;

            minvalue = x;
            maxvalue = x;
            for (size_t j = 0; j < v.size(); ++j) {
                adj(v[j]);
            }
        }

        inline void timer_start() {
            gettimeofday(&start_time, nullptr);
        }

        inline void timer_stop(double *duration_ = nullptr) {
            timeval end_time;
            gettimeofday(&end_time, nullptr);
            if (duration_type == metric_duration_type::SECONDS) {
                lasttime = end_time.tv_sec - start_time.tv_sec +
                           static_cast<double>(end_time.tv_usec - start_time.tv_usec) / 1.0E6;
            } else if (duration_type == metric_duration_type::MILLISECONDS) {
                lasttime = (end_time.tv_sec - start_time.tv_sec) * 1000 +
                           static_cast<double>(end_time.tv_usec - start_time.tv_usec) / 1.0E3;
            }
            add(lasttime);
            if (duration_ != nullptr) { *duration_ = lasttime; }
        }

    };

    class imetrics_reporter {

    public:
        virtual ~imetrics_reporter() = default;

        virtual
        void do_report(
                const std::string &name, const std::string &id,
                const std::map<std::string, metrics_entry> &entries) = 0;
    };

    /**
   * Metrics instance for logging metrics of a single object type.
   * Name of the metrics instance is set on construction.
   */
    class metrics {
    private:
        static metrics *m_instance;
    public:
        static void InitializeInstance(const std::string &name, const std::string &id="") {
            m_instance = new metrics(name, id);
        }

        static metrics* GetInstance() {
            if (m_instance == nullptr) {
                m_instance = new metrics("skg.default");
            }
            return m_instance;
        }

    private:
        std::string name, ident;
        std::map<std::string, metrics_entry> entries;
        mutex mlock;

    public:
        inline metrics(const std::string &_name = "", const std::string &_id = "") : name(_name), ident(_id) {
            this->set("app", _name);
        }

        inline void clear() {
            entries.clear();
        }

        inline std::string iterkey(const std::string &key, int iter) {
            char s[256];
            sprintf(s, "%s.%d", key.c_str(), iter);
            return std::string(s);
        }

        /**
         * Add to an existing value or create new.
         */
        inline void add(const std::string &key, double value, metrictype type = metrictype::REAL) {
            mlock.lock();
            if (entries.count(key) == 0) {
                entries[key] = metrics_entry(value, type);
            } else {
                entries[key].add(value);
            }
            mlock.unlock();
        }

        inline void add_to_vector(const std::string &key, double value) {
            if (entries.count(key) == 0) {
                entries[key] = metrics_entry(value, metrictype::VECTOR);
            } else {
                entries[key].add(value);
            }
        }

        inline void add_vector_entry(const std::string &key, size_t idx, double value) {
            if (entries.count(key) == 0) {
                entries[key] = metrics_entry(metrictype::VECTOR);
            }
            entries[key].add_vector_entry(idx, value);
        }

        inline void set(const std::string &key, size_t value) {
            set(key, (double) value, metrictype::INTEGER);
        }

        inline void set(const std::string &key, double value, metrictype type = metrictype::REAL) {
            if (entries.count(key) == 0) {
                entries[key] = metrics_entry(value, type);
            } else {
                entries[key].set(value);
            }
        }

        inline void set_integer(const std::string &key, size_t value) {
            if (entries.count(key) == 0) {
                entries[key] = metrics_entry((double) value, metrictype::INTEGER);
            } else {
                entries[key].set((double) value);
            }
        }

        inline void set(const std::string &key, const std::string &s) {
            if (entries.count(key) == 0) {
                entries[key] = metrics_entry(s);
            } else {
                entries[key].set(s);
            }
        }

        inline void set_vector_entry_integer(const std::string &key, size_t idx, size_t value) {
            set_vector_entry(key, idx, (double) (value));
        }

        inline void set_vector_entry(const std::string &key, size_t idx, double value) {
            mlock.lock();

            if (entries.count(key) == 0) {
                entries[key] = metrics_entry(metrictype::VECTOR);
            }
            entries[key].set_vector_entry(idx, value);
            mlock.unlock();
        }

        inline void start_time(
                const std::string &key,
                const metric_duration_type &duration_type=metric_duration_type::SECONDS) {
            mlock.lock();
            if (entries.count(key) == 0) {
                entries[key] = metrics_entry(metrictype::TIME, duration_type);
            }
            entries[key].timer_start();
            mlock.unlock();
        }

        metrics_entry start_time(
                const metric_duration_type &duration_type=metric_duration_type::SECONDS) {
            metrics_entry me(metrictype::TIME, duration_type);
            me.timer_start();
            return me;
        }

        inline void stop_time(metrics_entry me, const std::string &key, bool show = false) {
            me.timer_stop();
            mlock.lock();

            if (entries.count(key) == 0) {
                entries[key] = metrics_entry(metrictype::TIME);
            }
            entries[key].add(me.lasttime); // not thread safe
            if (show)
                std::cout << key << ": " << me.lasttime << " secs." << std::endl;
            mlock.unlock();

        }

        inline void stop_time(metrics_entry me, const std::string &key, uint32_t iternum, bool show = false) {
            me.timer_stop();
            mlock.lock();

            double t = me.lasttime;
            if (entries.count(key) == 0) {
                entries[key] = metrics_entry(metrictype::TIME);
            }
            entries[key].add(t); // not thread safe
            if (show)
                std::cout << key << ": " << me.lasttime << " secs." << std::endl;

            char s[256];
            sprintf(s, "%s.%d", key.c_str(), iternum);
            std::string ikey(s);
            if (entries.count(ikey) == 0) {
                entries[ikey] = metrics_entry(metrictype::TIME);
            }
            entries[ikey].add(t);

            mlock.unlock();
        }

        inline void stop_time(const std::string &key, bool show = false) {
            entries[key].timer_stop();
            /*
            if (show) {
                if (entries[key].duration_type == metric_duration_type::SECONDS) {
                    std::cout << key << ": " << entries[key].lasttime << " secs." << std::endl;
                } else {
                    std::cout << key << ": " << entries[key].lasttime << " ms." << std::endl;
                }
            }
            */
        }

        inline void set(const std::string &key, const metrics_entry &entry) {
            entries[key] = entry;
        }

        inline metrics_entry get(const std::string &key) {
            return entries[key];
        }

        void report(imetrics_reporter &reporter) {
            if (!name.empty()) {
                if (!ident.empty()) {
                    reporter.do_report(name, ident, entries);
                } else {
                    // ident 为空的情况下以 name 作为 ident
                    reporter.do_report(name, name, entries);
                }
            }
        }
    };

};


#endif

