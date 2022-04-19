
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
 * File metrics reporter.
 */


#ifndef DEF_GRAPHCHI_FILE_REPORTER
#define DEF_GRAPHCHI_FILE_REPORTER

#include <fstream>
#include <cstdio>

#include "metrics/metrics.hpp"
#include "util/cmdopts.h"


namespace skg {

    class file_reporter : public imetrics_reporter {
    private:
        std::string filename;
        FILE *f;
    public:

        explicit
        file_reporter(const std::string &fname) : filename(fname), f(nullptr) {
            // Create new file
            f = fopen(fname.c_str(), "w");
            assert(f != nullptr);
        }

        ~file_reporter() override {
            if (f != nullptr) {
                fclose(f);
            }
        }

        void report_integer(const std::string &ident,
                            std::map<std::string, metrics_entry>::const_iterator it) {
            const metrics_entry &ent = it->second;
            fprintf(f, "%s.%s=%ld\n", ident.c_str(), it->first.c_str(), (long int) (ent.value));
            fprintf(f, "%s.%s.count=%lu\n", ident.c_str(), it->first.c_str(), ent.count);
            fprintf(f, "%s.%s.min=%ld\n", ident.c_str(), it->first.c_str(), (long int) (ent.minvalue));
            fprintf(f, "%s.%s.max=%ld\n", ident.c_str(), it->first.c_str(), (long int) (ent.maxvalue));
            fprintf(f, "%s.%s.avg=%lf\n", ident.c_str(), it->first.c_str(), ent.cumvalue / ent.count);
        }

        void report_real(const std::string &ident,
                         std::map<std::string, metrics_entry>::const_iterator it) {
            const metrics_entry &ent = it->second;
            fprintf(f, "%s.%s=%lf\n", ident.c_str(), it->first.c_str(), (ent.value));
            fprintf(f, "%s.%s.count=%lu\n", ident.c_str(), it->first.c_str(), ent.count);
            if (ent.valtype == metrictype::TIME && ent.count > 1) { // 当计数>1时才输出最小/最大/平均值
                fprintf(f, "%s.%s.min=%lf\n", ident.c_str(), it->first.c_str(), (ent.minvalue));
                fprintf(f, "%s.%s.max=%lf\n", ident.c_str(), it->first.c_str(), (ent.maxvalue));
                fprintf(f, "%s.%s.avg=%lf\n", ident.c_str(), it->first.c_str(), ent.cumvalue / ent.count);
            }
        }

        void report_time(const std::string &ident,
                         std::map<std::string, metrics_entry>::const_iterator it) {
            const metrics_entry &ent = it->second;
            if (ent.valtype == metrictype::TIME) {
                // 打印时间单位
                if (ent.duration_type == metric_duration_type::SECONDS) {
                    fprintf(f, "%s.%s.duration_type=s\n", ident.c_str(), it->first.c_str());
                } else if (ent.duration_type == metric_duration_type::MILLISECONDS) {
                    fprintf(f, "%s.%s.duration_type=ms\n", ident.c_str(), it->first.c_str());
                }
            }
            report_real(ident, it);
        }

        void do_report(const std::string &name, const std::string &ident, const std::map<std::string, metrics_entry> &entries) override {
            if (ident != name) {
                fprintf(f, "[%s:%s]\n", name.c_str(), ident.c_str());
            } else {
                fprintf(f, "[%s]\n", name.c_str());
            }

            // First write numeral, then timings, then string entries
            for (size_t round = 0; round < 4; ++round) {
                bool isFirst = true;
                for (auto it = entries.begin(); it != entries.end(); ++it) {
                    const metrics_entry &ent = it->second;
                    switch (ent.valtype) {
                        case metrictype::INTEGER:
                            if (round == 0) {
                                if (isFirst) {
                                    fmt::print(f, "[Numeric]\n");
                                    isFirst = false;
                                }
                                report_integer(ident, it);
                            }
                            break;
                        case metrictype::REAL:
                            if (round == 1) {
                                if (isFirst) {
                                    fmt::print(f, "[Numeric]\n");
                                    isFirst = false;
                                }
                                report_real(ident, it);
                            }
                            break;
                        case metrictype::TIME:
                            if (round == 2) {
                                if (isFirst) {
                                    fmt::print(f, "[Timings]\n");
                                    isFirst = false;
                                }
                                report_time(ident, it);
                            }
                            break;
                        case metrictype::STRING:
                            if (round == 0) {
                                if (isFirst) {
                                    fmt::print(f, "[Others]\n");
                                    isFirst = false;
                                }
                                fprintf(f, "%s.%s=%s\n", ident.c_str(), it->first.c_str(), it->second.stringval.c_str());
                            }
                            break;
                        case metrictype::VECTOR:
                            if (round == 3) {
                                if (isFirst) {
                                    fmt::print(f, "[Numeric-array]\n");
                                    isFirst = false;
                                }
                                fprintf(f, "%s.%s.values=", ident.c_str(), it->first.c_str());
                                for (const double v : ent.v) fprintf(f, "%lf,", v);
                                fprintf(f, "\n");
                            }
                            break;
                    }
                }
            }

            fflush(f);
            fclose(f);
            f = nullptr;
        }

    };

};


#endif


