

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
 * Scheduler interface.
 */

#ifndef DEF_GRAPHCHI_ISCHEDULER
#define DEF_GRAPHCHI_ISCHEDULER

#include "util/types.h"
#include "util/skglogger.h"

namespace skg {
    
    class ischeduler {
    public:
        virtual ~ischeduler() {} 
        virtual void add_task(vid_t vid, bool also_this_iteration=false) = 0;
        virtual void add_task_to_all()  = 0;
        virtual bool is_scheduled(vid_t vertex) = 0;
        virtual size_t num_tasks() = 0;
        virtual void new_iteration(int iteration) = 0;
        virtual void remove_tasks(vid_t fromvertex, vid_t tovertex) = 0;

        virtual void clear() = 0; // add by jayzonhuang
    };
    
    
    /** 
     * Implementation of the scheduler which actually does nothing.
     */
    class non_scheduler : public ischeduler {
        int nwarnings;
    public:
        non_scheduler() : nwarnings(0) {}
        virtual ~non_scheduler() {} 
        virtual void add_task(vid_t vid, bool also_this_iteration=false) {
            if (nwarnings++ % 10000 == 0) {
                SKG_LOG_WARNING("Tried to add task to scheduler, but scheduling was not enabled!", "");
            } 
        }
        virtual void add_task_to_all() { }
        virtual bool is_scheduled(vid_t vertex) { return true; }
        virtual size_t num_tasks() { return 0; }
        virtual void new_iteration(int iteration) {} 
        
        virtual void remove_tasks(vid_t fromvertex, vid_t tovertex) {}

        virtual void clear() {} // add by jayzonhuang
    };
    
}


#endif

