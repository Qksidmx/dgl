
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



#ifndef GRAPHCHI_HTML_REPORTER
#define GRAPHCHI_HTML_REPORTER

#include <cstdio>

#include "metrics/metrics.hpp"


/**
  * Simple metrics reporter that dumps metrics to HTML
  */

namespace skg {

    class html_reporter : public imetrics_reporter {
    private:
        std::string filename;
        FILE *f;
    public:

        explicit html_reporter(const std::string &fname) : filename(fname), f(nullptr) {
            // Create new file
            f = fopen(fname.c_str(), "w");
            assert(f != nullptr);
            fprintf(f, "<html><head><title>GraphCHI Metrics Report</title>");
            fprintf(f, "<style>\n");
            fprintf(f,
                    "table {  border: 1px solid #999999; font: normal 80%%/140%% arial, helvetica, sans-serif; color: #555; background: #fff;}  td, th {border: 1px dotted #bbb; padding: .5em; width:100px}   ");
            fprintf(f, "</style></head><body>");
        }

        ~html_reporter() override {
            fprintf(f, "</body></html>");
            fclose(f);
            f = nullptr;
        }

        void do_report(const std::string &name, const std::string &ident, const std::map<std::string, metrics_entry> &entries) override {
            if (ident != name) {
                fprintf(f, "<h3>%s:%s</h3>\n", name.c_str(), ident.c_str());
            } else {
                fprintf(f, "<h3>%s</h3>\n", name.c_str());
            }

            // First write numeral, then timings, then string entries
            for (int round = 0; round < 4; round++) {
                int c = 0;
                fprintf(f, "<!-- Round %d -->\n", round);
                fprintf(f, "\n<p>");
                for (auto it = entries.begin(); it != entries.end(); ++it) {
                    metrics_entry ent = it->second;
                    switch (ent.valtype) {
                        case metrictype::INTEGER:
                            if (round == 0) {
                                if (c++ == 0)
                                    fprintf(f,
                                            "<table><tr><th>Key</th><th>Value</th><th>Count</th><th>Min</th><th>Max</th><th>Average</th></tr>");

                                fprintf(f, "<tr><td>%s</td>\n", it->first.c_str());

                                fprintf(f, "<td>%ld</td>\n", (long int) ent.value);
                                if (ent.count > 1) {
                                    fprintf(f, "<td>%ld</td>\n", (long int) ent.count);
                                    fprintf(f, "<td>%ld</td>\n", (long int) ent.minvalue);
                                    fprintf(f, "<td>%ld</td>\n", (long int) ent.maxvalue);
                                    fprintf(f, "<td>%.3lf</td>\n", ent.cumvalue / (double) ent.count);
                                } else fprintf(f, "<td colspan=4>&nbsp;</td>");
                                fprintf(f, "</tr>");
                            }
                            break;
                        case metrictype::REAL:
                            if (round == 0) {
                                if (c++ == 0)
                                    fprintf(f,
                                            "<table><tr><th>Key</th><th>Value</th><th>Count</th><th>Min</th><th>Max</th><th>Average</th></tr>");
                            }
                        case metrictype::TIME:
                            // TODO 打印时间单位
                            if (ent.valtype == metrictype::TIME && round == 1) {
                                if (c++ == 0)
                                    fprintf(f,
                                            "<table><tr><th>Key</th><th>Value (sec)</th><th>Count</th><th>Min (sec)</th><th>Max (sec)</th><th>Average (sec)</th></tr>\n");
                            }
                            if ((round == 0 && ent.valtype == metrictype::REAL) || (round == 1 && ent.valtype == metrictype::TIME)) {
                                fprintf(f, "<tr><td>%s</td>\n", it->first.c_str());

                                fprintf(f, "<td>%lf</td>\n", ent.value);
                                if (ent.count > 1) {
                                    fprintf(f, "<td>%ld</td>\n", (long int) ent.count);
                                    fprintf(f, "<td>%.3lf</td>\n", ent.minvalue);
                                    fprintf(f, "<td>%.3lf</td>\n", ent.maxvalue);
                                    fprintf(f, "<td>%.3lf</td>\n", ent.cumvalue / (double) ent.count);
                                } else fprintf(f, "<td colspan=4>&nbsp;</td>");
                                fprintf(f, "</tr>");
                            }
                            break;
                        case metrictype::STRING:
                            if (round == 2) {
                                if (c++ == 0)
                                    fprintf(f, "<table><tr><th>Key</th><th>Value</th></tr>\n");
                                fprintf(f, "<tr><td>%s</td><td width=400>%s</td>\n", it->first.c_str(),
                                        ent.stringval.c_str());
                                fprintf(f, "</tr>");
                            }

                            break;
                        case metrictype::VECTOR:
                            if (round == 3) {
                                // TODO
                            }
                            break;
                    }
                }
                if (c > 0) fprintf(f, "</table>");
                fprintf(f, " </p>");
            }

            fflush(f);
        }

    };

};


#endif



