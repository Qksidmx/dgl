
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
 * Simple metrics reporter that dumps metrics to
 * standard output. 
 */



#ifndef GRAPHCHI_STRING_REPORTER
#define GRAPHCHI_STRING_REPORTER

#include <iostream>
#include <map>

#include "metrics/metrics.hpp"

/**
 * Simple metrics reporter that dumps metrics to
 * standard output.
 */

namespace skg {

    class string_reporter : public imetrics_reporter {

    public:
        string_reporter(std::string outputstr):outputstr(""){}
        ~string_reporter() override = default;

        void do_report(const std::string &name, const std::string &ident, const std::map<std::string, metrics_entry> &entries) override {
            std::ostringstream ostr;

            // TODO: use reporters
            if (ident != name) {
                ostr << std::endl << " === REPORT FOR " << name << "(" << ident << ") ===" << std::endl;
            } else {
                ostr << std::endl << " === REPORT FOR " << name << " ===" << std::endl;
            }

            // First write numeral, then timings, then string entries
            for (int round = 0; round < 4; round++) {
                int c = 0;

                for (auto it = entries.begin(); it != entries.end(); ++it) {
                    metrics_entry ent = it->second;
                    switch (ent.valtype) {
                        case metrictype::REAL:
                        case metrictype::INTEGER:
                            if (round == 0) {
                                if (c++ == 0) ostr << "[Numeric]" << std::endl;
                                ostr << it->first << ":\t\t";
                                if (ent.count > 1) {
                                    ostr << ent.value << "\t(count: " << ent.count << ", min: " << ent.minvalue <<
                                              ", max: " << ent.maxvalue << ", avg: "
                                              << ent.cumvalue / (double) ent.count << ")" << std::endl;
                                } else {
                                    ostr << ent.value << std::endl;
                                }
                            }
                            break;
                        case metrictype::TIME:
                            if (round == 1) {
                                if (c++ == 0) ostr << "[Timings]" << std::endl;
                                ostr << it->first << ":\t\t";
                                if (ent.count > 1) {
                                    if (ent.duration_type == metric_duration_type::SECONDS) {
                                        ostr<< fmt::format("{:.6f}s\t (count: {}, min: {:.6f}s, max: {:.6f}s, avg: {:.6f}s)\n",
                                                   ent.value, ent.count,
                                                   ent.minvalue, ent.maxvalue,
                                                   ent.cumvalue / (double) ent.count);
                                    } else if (ent.duration_type == metric_duration_type::MILLISECONDS) {
                                        ostr<<fmt::format("{:.6f}ms\t (count: {}, min: {:.6f}ms, max: {:.6f}ms, avg: {:.6f}ms)\n",
                                                   ent.value, ent.count,
                                                   ent.minvalue, ent.maxvalue,
                                                   ent.cumvalue / (double) ent.count);
                                    }
                                } else {
                                    if (ent.duration_type == metric_duration_type::SECONDS) {
                                        ostr<<fmt::format("{} s\n", ent.value);
                                    } else if (ent.duration_type == metric_duration_type::MILLISECONDS) {
                                        ostr<<fmt::format("{} ms\n", ent.value);
                                    }
                                }
                            }
                            break;
                        case metrictype::STRING:
                            if (round == 2) {
                                if (c++ == 0) ostr << "[Other]" << std::endl;
                                ostr << it->first << ":\t";
                                ostr << ent.stringval << std::endl;
                            }
                            break;
                        case metrictype::VECTOR:
                            if (round == 3) {
                                if (c++ == 0) ostr << "[Numeric]" << std::endl;
                                ostr << it->first << ":\t\t";
                                if (ent.count > 1) {
                                    ostr << ent.value << "\t(count: " << ent.count << ", min: " << ent.minvalue <<
                                              ", max: " << ent.maxvalue << ", avg: "
                                              << ent.cumvalue / (double) ent.count << ")" << std::endl;
                                } else {
                                    ostr << ent.value << std::endl;
                                }
                                ostr << it->first << ".values:\t\t";
                                for (const double v : ent.v) ostr << v << ",";
                                ostr << std::endl;
                            }
                            break;
                    }
                }
            }
            ostr << std::endl;
            outputstr = ostr.str();
        }
        std::string GetString(){return outputstr;}
    private:
        std::string outputstr;
    };

};


#endif

