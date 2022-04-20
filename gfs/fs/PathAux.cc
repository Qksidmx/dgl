#include "PathAux.h"
#include "util/types.h"
#include "fmt/time.h"
#include "fmt/format.h"

namespace skg {
PathRequest::PathRequest() {
    max_depth=5;
    src_label = "";
    src_id = "";
    dst_label = "";
    dst_id = "";
    nlimit = 1 << 30;
    min_time = 0;
    max_time = INT_MAX;
    direction = 'o';
    mseclimit = 1 << 30;
}

bool PathRequest::any_label() const {
    return label_constraint.empty();
}

TraverseRequest::TraverseRequest() {
    label = "";
    id = "";
    direction = 'o';
    k = 3;
    nlimit = 1 << 30;
    mseclimit = 1 << 30;
}

PathResult::PathResult(int _code, std::string _msg) {
    code = _code;
    msg = _msg;
    data_vec.clear();
}

std::string PathResult::to_str() {
    std::string code_str = fmt::format("\"code\":{}", code);
    std::string msg_str = fmt::format("\"message\":\"{}\"",
            msg);
    std::string data_str = "\"data\": [";
    for (size_t i = 0; i < data_vec.size(); i ++) {
        data_str += "\n\"" + data_vec[i] + "\"";
        if (i+1 < data_vec.size()) {
            data_str += ",";
        }
    }
    data_str += "]";

    return fmt::format("{{{},{},{}}}", code_str, msg_str, data_str);
}

void PathResult::add_data(std::string& data_str) {
    data_vec.push_back(data_str);
    return;
}


PathVertex::PathVertex(std::string _label, std::string _id) {
    label = _label;
    id = _id;
    data = "";
}

PathVertex::PathVertex(std::string _label, std::string _id,
                       std::string _data) {
    label = _label;
    id = _id;
    data = _data;
}

PathVertex::PathVertex() {
    label = "";
    id = "";
    data = "";
}

PathVertex& PathVertex::operator=(const PathVertex &pv) {
    label = pv.label;
    id = pv.id;
    data = pv.data;
    return *this;
}

bool PathVertex::operator < (const PathVertex &pv) const {
    int label_cmp = label.compare(pv.label);
    if (label_cmp != 0) {
        return label_cmp < 0;
    }
    int id_cmp = id.compare(pv.id);
    if (id_cmp != 0) {
        return id_cmp < 0;
    }
      
    return false;  
    //return data.compare(pv.data) < 0;
}

bool PathVertex::operator == (const PathVertex &pv) const {
    return label.compare(pv.label) == 0 &&
           id.compare(pv.id) == 0; // &&
           //data.compare(pv.data) == 0;
}

std::string PathVertex::to_str() const {
    if (data == "") {
        return label+":"+id;
    }
    else {
        return label+":"+id+""+data+"";   
    }
}

PathEdge::PathEdge(std::string _str_id, PathEdge* _father, std::string _label, std::string _elabel) {
    id = _str_id;
    father = _father;
    label = _label;
    elabel = _elabel;
}

size_t PathEdge::size() {
    size_t ret = 0;
    ret += id.size() + label.size() + elabel.size();
    ret += sizeof(PathEdge);
    return ret;
}

bool PathEdge::check_cycle(const std::string& vid, const std::string& vlabel) {
    if (vid == id && vlabel == label)
        return true;
    if (father == nullptr)
        return false;
    return father->check_cycle(vid, vlabel);  
}

std::string Edge::to_str() const {
    return label+":"+id;
}

PathEdge** PathEdge::to_patharray(int &path_len) {
    path_len = 0;
    PathEdge* cur_ite_ptr = this;
    while (cur_ite_ptr != nullptr) {
        path_len ++;
        cur_ite_ptr = cur_ite_ptr->father;
    }
    cur_ite_ptr = this;
    PathEdge** array = new PathEdge*[path_len];
    int i_edge = path_len-1;
    while (cur_ite_ptr != nullptr) {
        array[i_edge] = cur_ite_ptr;
        i_edge --;
        cur_ite_ptr = cur_ite_ptr->father;
    }
    return array;
}

std::string PathEdge::path_str(PathEdge* dst_ptr, int &path_len) {
    if (dst_ptr == nullptr) {
        return "";
    }
    path_len = 0;
    PathEdge** array = dst_ptr->to_patharray(path_len);
    std::string ret;
    ret = array[0]->to_str();
    for (int i = 1; i < path_len; i ++) {
        ret += " -" + array[i]->elabel + "-> ";
        ret += array[i]->to_str();
    }
    delete[] array;
    return ret;
}

TimeEdge::TimeEdge(std::string _str_id, TimeEdge* _father, std::string _label, std::string _elabel, int _timestamp) {
    id = _str_id;
    father = _father;
    label = _label;
    elabel = _elabel;
    timestamp = _timestamp;
}

#if 0
bool TimeEdge::check_cycle(const std::string& vid, const std::string& vlabel) {
    if (vid == id && vlabel == label)
        return true;
    if (father == nullptr)
        return false;
    return father->check_cycle(vid, vlabel);  
}
#endif

TimeEdge** TimeEdge::to_tpatharray(int &path_len) {
    path_len = 0;
    TimeEdge* cur_ite_ptr = this;
    while (cur_ite_ptr != nullptr) {
        path_len ++;
        cur_ite_ptr = cur_ite_ptr->father;
    }
    cur_ite_ptr = this;
    TimeEdge** array = new TimeEdge*[path_len];
    int i_edge = path_len-1;
    while (cur_ite_ptr != nullptr) {
        array[i_edge] = cur_ite_ptr;
        i_edge --;
        cur_ite_ptr = cur_ite_ptr->father;
    }
    return array;
}

size_t TimeEdge::size() {
    size_t ret = 0;
    ret += id.size() + label.size() + elabel.size();
    ret += sizeof(TimeEdge);
    return ret;
}

std::string TimeEdge::tpath_str(TimeEdge* dst_ptr, int &path_len) {
    if (dst_ptr == nullptr) {
        return "";
    }
    path_len = 0;
    TimeEdge** array = dst_ptr->to_tpatharray(path_len);
    std::string ret;
    ret = array[0]->to_str();
    for (int i = 1; i < path_len; i ++) {
        std::string tstr = tformat((std::time_t)array[i]->timestamp);
        ret += fmt::format(" -{}@{}-> ", 
                array[i]->elabel, tstr);
        ret += array[i]->to_str();
    }
    delete[] array;
    return ret;
}

std::string TimeEdge::tformat(int _timestamp) {
    std::time_t t = (std::time_t)_timestamp;
    return  fmt::format("{:%Y-%m-%d %H:%M:%S}", *std::localtime(&t)); 
}

PVpair::PVpair() {

}

PVpair::PVpair(const PathVertex &pv1, const PathVertex &pv2, const std::string _elabel, const std::string _data) {
    first = pv1;
    second = pv2;
    data = _data;
    elabel = _elabel;
}

PVpair& PVpair::operator=(const PVpair & pvp) {
    first = pvp.first;
    second = pvp.second;
    data = pvp.data;
    return *this;
}

std::string PVpair::to_str() const {
    std::string ret = "";
    if (elabel != "") ret += elabel + ",";
    ret += first.to_str() + "," + second.to_str();
    if (data != "") ret += data;
    return ret;
}
} // namespace
