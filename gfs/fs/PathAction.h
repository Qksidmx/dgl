#ifndef _PATH_ACTION_H_
#define _PATH_ACTION_H_ 

#include "fmt/format.h"
#include "util/types.h"
#include "fs/skgfs.h"
#include "util/cmdopts.h"
#include <cstdio>
#include "PathAux.h"

namespace skg {
    class PathAction {
    private:
        std::string  basic_usage_str(char *);
        void queue_clear(std::vector<PathEdge*>& v_queue);
        const SkgDB* m_db;
    public:
        static const int check_freq = 20*1000;
        static const size_t max_mem_k = 10*1000*1000;

        PathAction(const SkgDB* db);
        ~PathAction();

        std::string shortest_path(const PathRequest& path_req);
        std::string all_path(const PathRequest& path_req);
        static int64_t cur_time(); 
    };
}
#endif
