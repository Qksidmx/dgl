

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
 * This header includes all the main headers needed for a GraphChi
 * program.
 */


#ifndef SKG_ALLBASIC_INCLUDES
#define SKG_ALLBASIC_INCLUDES

#include <omp.h>
#include <sstream>
#include "util/skglogger.h"

//#include "util/chifilenames.h"
#include "fs/skg_context.hpp"
#include "fs/skg_hetnet_program.hpp"
#include "fs/hetnet_edge.hpp"
#include "fs/hetnet_vertex.hpp"
#include "fs/hetnet_attrdata.hpp"
#include "util/ischeduler.hpp"
#include "fs/vertex_aggregator.hpp"

#include "fs/viewdef.hpp"
#include "fs/engine_dumper.hpp"
//#include "engine/hetnet_engine.hpp"

//#include "util/logger.h"

#include "metrics/metrics.hpp"
#include "metrics/reps/basic_reporter.hpp"
#include "metrics/reps/file_reporter.hpp"
#include "metrics/reps/html_reporter.hpp"

#include "util/cmdopts.h"


namespace skg {
        
    /**
      * Helper for metrics.
      */
    static VARIABLE_IS_NOT_USED void metrics_report(metrics &m);
    static VARIABLE_IS_NOT_USED void metrics_report(metrics &m) {
        std::string reporters = get_option_string("metrics.reporter", "console");
        char * creps = (char*)reporters.c_str();
        const char * delims = ",";
        char * t = strtok(creps, delims);

        while(t != NULL) {            
            std::string repname(t);
            if (repname == "basic" || repname == "console") {
                basic_reporter rep;
                m.report(rep);
            } else if (repname == "file") {
                file_reporter rep(get_option_string("metrics.reporter.filename", "metrics.txt"));
                m.report(rep);
            } else if (repname == "html") {
                html_reporter rep(get_option_string("metrics.reporter.htmlfile", "metrics.html"));
                m.report(rep);
            } else {
                SKG_LOG_WARNING("Could not find metrics reporter with name {}, ignoring", repname);
            }
            t = strtok(NULL, delims);
        }
        
        
    }
    
    
};

#endif
