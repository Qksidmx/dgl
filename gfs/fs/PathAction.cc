#include "PathAction.h"
#include "ColumnDescriptorUtils.h"

namespace skg {

PathAction::PathAction(const SkgDB* db) {
    assert(db != nullptr);
    m_db = db;
}

PathAction::~PathAction() {
    m_db = nullptr;
}

std::string PathAction::shortest_path(const PathRequest& path_req) {
    std::string src_id = path_req.src_id;
    std::string src_label = path_req.src_label;
    std::string dst_id = path_req.dst_id;
    std::string dst_label = path_req.dst_label;
    int max_depth = path_req.max_depth;
    int nlimit = path_req.nlimit;
    if (nlimit <= 0) {
        PathResult pr(1, fmt::format("nlimit = {}", nlimit));
        return pr.to_str();
    }
    int mseclimit = path_req.mseclimit;
    if (mseclimit <= 0) {
        PathResult pr(1, fmt::format("time limit = {} ms", mseclimit));
        return pr.to_str();
    }
    const std::vector<std::string>& label_cons = path_req.label_constraint;
    std::set<std::string> label_set;
    label_set.insert(label_cons.begin(), label_cons.end());

    std::vector<PathEdge*> v_queue;
    std::set<PathVertex> visited;
    PathEdge* pe = new PathEdge(src_id, nullptr, src_label, "");
    v_queue.push_back(pe);
    size_t vq_front = 0;
    std::vector<PathEdge*> dst_pe_vec;
    int front_depth=-1;//first vertex is depth=0
    size_t next_level_first = 0; // the leftmost index of visited ones in next level;
    Status s;
    std::vector<std::string> empty_vec;
    int distance = -1;
    size_t max_queue_size = PathAction::max_mem_k*1000;
    int64_t t_begin = PathAction::cur_time();
    size_t queue_mem = pe->size();
    int prev_freq_batch = 0;
    while (vq_front != v_queue.size() && 
            max_queue_size > queue_mem &&
            dst_pe_vec.size() < (size_t)nlimit) {
        PathEdge* cur_pe = v_queue[vq_front];
        if (next_level_first == vq_front) {
            if (distance != -1) {
                break; // no need for next level
            }
            next_level_first = v_queue.size();
            front_depth ++;
            if (front_depth == max_depth)
                break;
        }
        if ((int)(v_queue.size()/PathAction::check_freq) 
                > prev_freq_batch) {
            int64_t t_delta = PathAction::cur_time() - t_begin;
            if ((int)t_delta >= mseclimit) {
                PathResult pr(2, "time out");
                queue_clear(v_queue);
                queue_clear(dst_pe_vec);
                return pr.to_str();
            }
        }
        vq_front ++;
        std::string cur_id = cur_pe->id;
        std::string cur_label = cur_pe->label; 

        EdgesQueryResult eqr;
        VertexRequest v_req;
        v_req.SetVertex(cur_label, cur_id);
        s = m_db->GetOutEdges(v_req, &eqr);
        if (!s.ok()) {
            queue_clear(v_queue);
            queue_clear(dst_pe_vec);
            PathResult pr(1, s.ToString());
            return pr.to_str(); 
        }

        while (eqr.HasNext()) {
            eqr.MoveNext();
            std::string elabel = eqr.GetLabel(&s);
            if (label_set.size() > 0 && 
                    label_set.find(elabel) == label_set.end()) {
                continue;
            }
            std::string target_id = eqr.GetDstVertex(&s);
            std::string target_label = eqr.GetDstVertexLabel(&s);
            PathVertex pv(target_label, target_id);
            if (nlimit == 1) {
                if (visited.find(pv) != visited.end()) {
                    continue;
                } else {
                    visited.insert(pv);
                }
            }

            if (target_id == dst_id &&
                target_label == dst_label) {
                PathEdge* new_pe = new PathEdge(target_id, cur_pe,
                    target_label, elabel);
                dst_pe_vec.push_back(new_pe);
                queue_mem += new_pe->size();
                if (queue_mem > max_queue_size){
                  std::cout<< "Out of memory.\n";
                  break;
                }
                if (dst_pe_vec.size() >= (size_t)nlimit) {
                    break;
                }
                if (distance == -1) {
                    distance = 0;
                    PathEdge* tmppe = new_pe;
                    while (tmppe->father != nullptr) {
                        distance ++;
                        tmppe = tmppe->father;
                    }
                }
            } else if (distance == -1) {//need deeper
                PathEdge* new_pe = new PathEdge(target_id, cur_pe,
                    target_label, elabel);
                queue_mem += new_pe->size();
                if (queue_mem > max_queue_size){
                  std::cout<< "Out of memory.\n";
                  break;
                }
                v_queue.push_back(new_pe);
            }
        }
        if (dst_pe_vec.size() > (size_t)nlimit) {
            break;
        }
    }

    std::string ret = "";
    if (dst_pe_vec.empty()) {
        PathResult pr(0, "Not found");
        ret = pr.to_str();
    } else {
        int path_len = 0;
        std::string path_str = PathEdge::path_str(dst_pe_vec[0], path_len);
        PathResult pr(0, fmt::format("{} shortest path(s) of length {}",
                    dst_pe_vec.size(), path_len) );
        pr.add_data(path_str);

        for (size_t i  = 1; i < dst_pe_vec.size(); i ++) {
            path_str = PathEdge::path_str(dst_pe_vec[i], path_len);
            pr.add_data(path_str);
        }
        ret = pr.to_str();
    }
    queue_clear(v_queue);
    queue_clear(dst_pe_vec);
    return ret;
}

std::string PathAction::all_path(const PathRequest& path_req) {
    std::string src_id = path_req.src_id;
    std::string src_label = path_req.src_label;
    std::string dst_id = path_req.dst_id;
    std::string dst_label = path_req.dst_label;
    int max_depth = path_req.max_depth;
    int nlimit = path_req.nlimit;
    if (nlimit <= 0) {
        PathResult pr(1, fmt::format("nlimit = {}", nlimit));
        return pr.to_str();
    }
    int mseclimit = path_req.mseclimit;
    if (mseclimit <= 0) {
        PathResult pr(1, fmt::format("time limit = {} ms", mseclimit));
        return pr.to_str();
    }
    const std::vector<std::string>& label_cons = path_req.label_constraint;
    std::set<std::string> label_set;
    label_set.insert(label_cons.begin(), label_cons.end());

    std::vector<PathEdge*> v_queue;
    std::set<PathVertex> visited;
    PathEdge* pe = new PathEdge(src_id, nullptr, src_label, "");
    v_queue.push_back(pe);
    size_t vq_front = 0;
    std::vector<PathEdge*> dst_pe_vec;
    int front_depth=-1;//first vertex is depth=0
    size_t next_level_first = 0; // the leftmost index of visited ones in next level;
    Status s;
    std::vector<std::string> empty_vec;
    size_t max_queue_size = PathAction::max_mem_k*1000;
    int64_t t_begin = PathAction::cur_time();
    size_t queue_mem = pe->size();
    int prev_freq_batch = 0;
    while (vq_front != v_queue.size() && 
            max_queue_size > queue_mem &&
            dst_pe_vec.size() < (size_t)nlimit) {
        PathEdge* cur_pe = v_queue[vq_front];
        if (next_level_first == vq_front) {
            next_level_first = v_queue.size();
            front_depth ++;
            if (front_depth == max_depth)
                break;
        }
        if ((int)(v_queue.size()/PathAction::check_freq) 
                > prev_freq_batch) {
            int64_t t_delta = PathAction::cur_time() - t_begin;
            if ((int)t_delta >= mseclimit) {
                PathResult pr(2, "time out");
                queue_clear(v_queue);
                queue_clear(dst_pe_vec);
                return pr.to_str();
            }
            prev_freq_batch = v_queue.size()/PathAction::check_freq;
        }
        vq_front ++;
        std::string cur_id = cur_pe->id;
        std::string cur_label = cur_pe->label; 

        EdgesQueryResult eqr;
        VertexRequest v_req;
        v_req.SetVertex(cur_label, cur_id);
        s = m_db->GetOutEdges(v_req, &eqr);
        if (!s.ok()) {
            queue_clear(v_queue);
            queue_clear(dst_pe_vec);
            PathResult pr(1, s.ToString());
            return pr.to_str(); 
        }

        while (eqr.HasNext()) {
            eqr.MoveNext();
            std::string elabel = eqr.GetLabel(&s);
            if (label_set.size() > 0 && 
                    label_set.find(elabel) == label_set.end()) {
                continue;
            }
            std::string target_id = eqr.GetDstVertex(&s);
            std::string target_label = eqr.GetDstVertexLabel(&s);
            if (cur_pe->check_cycle(target_id, target_label)) {
                continue;
            }
            if (nlimit == 1) {
                PathVertex pv(target_label, target_id);
                if (visited.find(pv) != visited.end()) {
                    continue;
                } else {
                    visited.insert(pv);
                }
            }

            if (target_id == dst_id &&
                target_label == dst_label) {
                PathEdge* new_pe = new PathEdge(target_id, cur_pe,
                    target_label, elabel);
                dst_pe_vec.push_back(new_pe);
                queue_mem += new_pe->size();
                if (dst_pe_vec.size() >= (size_t)nlimit) {
                    break;
                }
            } else {
                PathEdge* new_pe = new PathEdge(target_id, cur_pe,
                    target_label, elabel);
                v_queue.push_back(new_pe);
                queue_mem += new_pe->size();
            }
        }
        if (dst_pe_vec.size() > (size_t)nlimit) {
            break;
        }
    }

    std::string ret = "";
    if (dst_pe_vec.empty()) {
        PathResult pr(0, "Not found");
        ret = pr.to_str();
    } else {
        int path_len = 0;
        std::string path_str = PathEdge::path_str(dst_pe_vec[0], path_len);
        PathResult pr(0, fmt::format("{}  path(s)",
                    dst_pe_vec.size()) );
        pr.add_data(path_str);

        for (size_t i  = 1; i < dst_pe_vec.size(); i ++) {
            path_str = PathEdge::path_str(dst_pe_vec[i], path_len);
            pr.add_data(path_str);
        }
        ret = pr.to_str();
    }
    queue_clear(v_queue);
    queue_clear(dst_pe_vec);
    return ret;
}

int64_t PathAction::cur_time() {
    timeval tv;
    gettimeofday(&tv, nullptr);
    int64_t ret = ((int64_t)tv.tv_sec) * 1000 + tv.tv_usec / 1000;
    return ret;
}

std::string PathAction::basic_usage_str(char* prog) {
    std::string usage_str = "Usage: {}";
    usage_str += " --db-name=<db-name>";
    usage_str += " --src-vtx=<source-vertex>";         
    usage_str += " --dst-vtx=<destination-vertex>";     
    usage_str += " --src-label=<label-of-src-vertex>";    
    usage_str += " --dst-label=<label-of-dst-vertex>";     
    usage_str += " --db_dir=<directory-of-db>";     
    return usage_str;
}

void PathAction::queue_clear(std::vector<PathEdge*>& v_queue) {
    for (auto vitr = v_queue.begin(); vitr != v_queue.end(); vitr ++) {
        delete (*vitr);
    }
}
} // namesapce
