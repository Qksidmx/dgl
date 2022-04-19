#include "fs/TraverseAction.h"
#include "fs/ColumnDescriptorUtils.h"

namespace skg {

TraverseAction::TraverseAction(const SkgDB* db) {
    assert(db != nullptr);
    m_db = db;
}

TraverseAction::~TraverseAction() {
    m_db = nullptr;
}

Status TraverseAction::get_neighbors(VertexQueryResult* vqr_ptr, std::string id,
        std::string label, const std::vector<std::string> &qcols, char direction) {
    VertexRequest vreq;
    vreq.SetVertex(label, id);
    vreq.SetQueryColumnNames(qcols);
    Status s;
    if (direction == 'i') {
        s = m_db->GetInVertices(vreq, vqr_ptr);
    } else if (direction == 'o') {
        s = m_db->GetOutVertices(vreq, vqr_ptr);
    } else if (direction == 'b') {
        s = m_db->GetBothVertices(vreq, vqr_ptr);
    }
    return s;
}

Status TraverseAction::get_edges(EdgesQueryResult* eqr, std::string id, 
        std::string label, const std::vector<std::string> &qcols, char direction) {
    VertexRequest vreq;
    vreq.SetVertex(label, id); 
    vreq.SetQueryColumnNames(qcols);
    Status s;
    if (direction == 'i') {
        s = m_db->GetInEdges(vreq, eqr);
    } else if (direction == 'o') {
        s = m_db->GetOutEdges(vreq, eqr);
    } else if (direction == 'b') {
        s = m_db->GetBothEdges(vreq, eqr);
    }
    return s; 
}

Status TraverseAction::get_vattr(VertexQueryResult* vqr_ptr, std::string id, 
        std::string label, const std::vector<std::string> &qcols) {
    VertexRequest vreq;
    vreq.SetVertex(label, id);
    vreq.SetQueryColumnNames(qcols);
    Status s;
    s = m_db->GetVertexAttr(vreq, vqr_ptr);
    return s;
}

Status TraverseAction::get_vattr_str(std::string& ret_str, std::string id, std::string label,
        const std::vector<std::string> &qcols) {
    VertexQueryResult vqr;
    Status s = get_vattr(&vqr, id, label, qcols);
    if (!s.ok()) {
        ret_str = "";
        return s; 
    }
    if (vqr.HasNext()) {
        vqr.MoveNext();
        ret_str = ColumnDescriptorUtils::SerializePropList(vqr);
    } else {
        ret_str = "";
    }
    return s;
}

Status TraverseAction::k_neighbor(const TraverseRequest& traverse_req,
                std::set<PathVertex> *visited) {
    std::string src_id  = traverse_req.id;
    std::string src_label = traverse_req.label;
    int k = traverse_req.k;
    char direction = traverse_req.direction;
    int nlimit = traverse_req.nlimit;
    const std::vector<std::string> qcols =
        traverse_req.qcols;
    const std::vector<std::string> label_cons = 
        traverse_req.label_constraint;
    std::set<std::string> label_set;
    label_set.insert(label_cons.begin(), label_cons.end());
    if (nlimit <= 0) 
        return Status::OK();
    std::vector<PathVertex> pv_level[2];
    int n_reserve = (nlimit > 100000) ? 100000 : nlimit;
    if ( k > 1) {
        pv_level[0].reserve(n_reserve);
    }
    pv_level[1].reserve(n_reserve);
    std::string vattr_str = "";
    if (qcols.size() > 0) {
        get_vattr_str(vattr_str, src_id, src_label, qcols); //ref vattr_str
    }
    PathVertex pv_src(src_label, src_id, vattr_str);
    pv_level[0].push_back(pv_src);
    visited->insert(pv_src);
    Status s;

    if (nlimit == 1) {
        return Status::OK();
    }
    for (int i = 0; i < k; i ++) {
        int cur = i % 2;
        int next = 1 - cur;    
        pv_level[next].clear();
        for (size_t j = 0; j < pv_level[cur].size(); j ++) {
            EdgesQueryResult eqr;
            std::string cur_label = pv_level[cur][j].label;
            std::string cur_id = pv_level[cur][j].id;
            s = this->get_edges(&eqr, cur_id, cur_label, qcols, direction);
            if (!s.ok()) {
                return s;
            }

            while (eqr.HasNext()) {
                eqr.MoveNext(); 
                if (label_set.size() > 0 &&
                        label_set.find(eqr.GetLabel(&s)) ==
                            label_set.end()) {
                    continue;
                }

                std::string dst_id = eqr.GetDstVertex(&s);
                std::string dst_label = eqr.GetDstVertexLabel(&s);
                vattr_str = "";
                if (qcols.size() > 0) {
                    get_vattr_str(vattr_str, dst_id, dst_label, qcols); //ref vattr_str
                }

                PathVertex pv(dst_label, dst_id, vattr_str);
                if (visited->find(pv) != visited->end()) {
                    continue;
                } else {
                    visited->insert(pv);
                    if (visited->size() >= (size_t) nlimit) {
                        return Status::OK();
                    }
                    if (i < k-1) {
                        pv_level[next].push_back(pv);
                    }
                }
            }  
        } 
    }
    return Status::OK();
}

Status TraverseAction::k_out(const TraverseRequest& traverse_req,
                std::vector<PVpair> *e_visited) {
    std::string src_id  = traverse_req.id;
    std::string src_label = traverse_req.label;
    int k = traverse_req.k;
    char direction = traverse_req.direction;
    int nlimit = traverse_req.nlimit;
    const std::vector<std::string> qcols =
        traverse_req.qcols;
    const std::vector<std::string> label_cons = 
        traverse_req.label_constraint;
    std::set<std::string> label_set;
    label_set.insert(label_cons.begin(), label_cons.end());
    if (nlimit <= 0) 
        return Status::OK();
    std::vector<PathVertex> pv_level[2];
    int n_reserve = (nlimit > 100000) ? 100000 : nlimit;
    if ( k > 1) {
        pv_level[0].reserve(n_reserve);
    }
    pv_level[1].reserve(n_reserve);
    PathVertex pv_src(src_label, src_id);
    pv_level[0].push_back(pv_src);
    std::set<PathVertex> visited;
    visited.insert(pv_src);
    Status s;
    for (int i = 0; i < k; i ++) {
        int cur = i % 2;
        int next = 1 - cur;    
        pv_level[next].clear();
        for (size_t j = 0; j < pv_level[cur].size(); j ++) {
            EdgesQueryResult eqr;
            std::string cur_label = pv_level[cur][j].label;
            std::string cur_id = pv_level[cur][j].id;
            s = this->get_edges(&eqr, cur_id, cur_label, qcols, direction);
            if (!s.ok()) {
                return s;
            }

            while (eqr.HasNext()) {
                eqr.MoveNext(); 
                if (label_set.size() > 0 &&
                        label_set.find(eqr.GetLabel(&s)) ==
                            label_set.end()) {
                    continue;
                }
                PathVertex pv(eqr.GetDstVertexLabel(&s),eqr.GetDstVertex(&s));
                e_visited->push_back(
                        PVpair(pv_level[cur][j], pv, eqr.GetLabel(&s), ColumnDescriptorUtils::SerializePropList(eqr))
                        );
                if (e_visited->size() >= (size_t) nlimit)
                    return Status::OK();

                if (visited.find(pv) != visited.end()) {
                    continue;
                } else {
                    visited.insert(pv);
                    if (i < k-1) {
                        pv_level[next].push_back(pv);
                    }
                }
            }  
        } 
    }
    return Status::OK();
}

Status TraverseAction::k_out_size(const TraverseRequest& traverse_req,
                size_t *v_size, size_t *e_size) {
    std::string src_id  = traverse_req.id;
    std::string src_label = traverse_req.label;
    int k = traverse_req.k;
    char direction = traverse_req.direction;
    const std::vector<std::string> label_cons = 
        traverse_req.label_constraint;
    std::set<std::string> label_set;
    label_set.insert(label_cons.begin(), label_cons.end());
    std::vector<std::string> qcols; //need no set
    std::vector<PathVertex> pv_level[2];
    int n_reserve = 100000;
    if ( k > 1) {
        pv_level[0].reserve(n_reserve);
    }
    pv_level[1].reserve(n_reserve);
    PathVertex pv_src(src_label, src_id);
    pv_level[0].push_back(pv_src);
    std::set<PathVertex> visited;
    visited.insert(pv_src);
    *e_size = 0;
    *v_size = 1;
    Status s;
    for (int i = 0; i < k; i ++) {
        int cur = i % 2;
        int next = 1 - cur;    
        pv_level[next].clear();
        for (size_t j = 0; j < pv_level[cur].size(); j ++) {
            EdgesQueryResult eqr;
            std::string cur_label = pv_level[cur][j].label;
            std::string cur_id = pv_level[cur][j].id;
            s = this->get_edges(&eqr, cur_id, cur_label, qcols, direction);
            //*e_size += eqr.Size();
            if (!s.ok()) {
                return s;
            }

            while (eqr.HasNext()) {
                eqr.MoveNext(); 
                if (label_set.size() > 0 &&
                        label_set.find(eqr.GetLabel(&s)) ==
                            label_set.end()) {
                    continue;
                }
                *e_size = *e_size + 1;
                PathVertex pv(eqr.GetDstVertexLabel(&s),eqr.GetDstVertex(&s));
                if (visited.find(pv) != visited.end()) {
                    continue;
                } else {
                    visited.insert(pv);
                    if (i < k-1) {
                        pv_level[next].push_back(pv);
                    }
                }
            }  
        } 
    }
    *v_size = visited.size();
    return Status::OK();
}

std::string TraverseAction::basic_usage_str(char* prog) {
    std::string usage_str = "Usage: {}";
    usage_str += " --db-name=<db-name>";
    usage_str += " --src-vtx=<source-vertex>";         
    usage_str += " --dst-vtx=<destination-vertex>";     
    usage_str += " --src-label=<label-of-src-vertex>";    
    usage_str += " --dst-label=<label-of-dst-vertex>";     
    usage_str += " --db_dir=<directory-of-db>";     
    return usage_str;
}

void TraverseAction::queue_clear(std::vector<PathEdge*>& v_queue) {
    for (auto vitr = v_queue.begin(); vitr != v_queue.end(); vitr ++) {
        delete (*vitr);
    }
}
} // namesapce
