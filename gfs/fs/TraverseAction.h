#ifndef _TRAVERSE_ACTION_H_
#define _TRAVERSE_ACTION_H_ 

#include "fmt/format.h"
#include "util/types.h"
#include "fs/skgfs.h"
#include "util/cmdopts.h"
#include <cstdio>
#include "fs/PathAux.h"

namespace skg {
    class TraverseAction {
    private:
        std::string  basic_usage_str(char *);
        void queue_clear(std::vector<PathEdge*>& v_queue);
        const SkgDB* m_db;
    public:
        TraverseAction(const SkgDB* db);
        ~TraverseAction();
        Status get_neighbors(VertexQueryResult* vqr, std::string id,
                std::string label, const std::vector<std::string>& qcols, char direction);

        Status get_edges(EdgesQueryResult* eqr, std::string id, 
                std::string label, const std::vector<std::string> &qcols, char direction);

        Status get_vattr(VertexQueryResult* vqr_ptr, std::string id, 
            std::string label, const std::vector<std::string> &qcols);

        Status get_vattr_str(std::string& ret_str, std::string id, 
            std::string label, const std::vector<std::string> &qcols);
        
        Status k_neighbor(const TraverseRequest& traverse_req,
                std::set<PathVertex> *visited);

        Status k_out(const TraverseRequest& traverse_req,
                std::vector<PVpair> *e_visited);

        Status k_out_size(const TraverseRequest& traverse_req,
                size_t *v_size, size_t *e_size);
    };
}
#endif
