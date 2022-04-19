#ifndef _PATH_AUX_HPP_
#define _PATH_AUX_HPP_

#include <stdlib.h>
#include <vector>
#include <set>
#include <queue>
#include <stdio.h>
#include <string>

namespace skg {

    class PathRequest {
    public:
        std::string src_label;
        std::string src_id;
        std::string dst_label;
        std::string dst_id;
        char direction;
        int max_depth;
        int nlimit;
        int mseclimit;
        int min_time;
        int max_time;
        std::string time_col;
        std::vector<std::string> label_constraint;

        PathRequest();
        bool any_label() const;
    };

    class TraverseRequest {
    public:
        std::string label;
        std::string id;
        std::vector<std::string> qcols;
        char direction;
        int k;
        int mseclimit;
        int nlimit;
        std::vector<std::string> label_constraint;

        TraverseRequest();
    };

    class PathResult {
    public:
        int code;
        std::string msg;
        std::vector<std::string> data_vec;

        PathResult(int _code, std::string _msg);
        std::string to_str();
        void add_data(std::string& data_str);
    };

    class PathVertex {
    public:
        std::string label;
        std::string id;
        std::string data;
        PathVertex(std::string _label, std::string _id);
        PathVertex(std::string _label, std::string _id, std::string data);
        PathVertex();
        
        PathVertex& operator=(const PathVertex &pv);
        bool operator < (const PathVertex &pv) const;
        bool operator == (const PathVertex &pv) const;
        std::string to_str() const;
    };

    class Edge {
    public:
        std::string elabel;
        std::string label;
        std::string id;

        Edge(){}
        std::string to_str() const;
    };
    
    class PathEdge : public Edge {
    public:
        PathEdge* father;

        PathEdge(std::string _str_id, PathEdge* _father, std::string _label, std::string _elabel);
        bool check_cycle(const std::string& vid, const std::string& vlabel);
        PathEdge** to_patharray(int &path_len);
        size_t size();
        static std::string path_str(PathEdge* dst_ptr, int &path_len);
    };

    class TimeEdge : public Edge {
    public:
        int timestamp;
        TimeEdge* father;
    
        TimeEdge(std::string _id, TimeEdge* _father, std::string _label, std::string _elabel, int _timestamp);
        TimeEdge** to_tpatharray(int &path_len);
        size_t size();
        static std::string tpath_str(TimeEdge* dst_ptr, int &path_len);
        static std::string tformat(int _timestamp);
    };

    class PVpair {
    public:
        PathVertex first;
        PathVertex second;
        std::string elabel;
        std::string data;

        PVpair();
        PVpair(const PathVertex &pv1, const PathVertex &pv2, const std::string elabel, const std::string _data);
        PVpair& operator=(const PVpair & pvp);
        std::string to_str() const;
    };
}

#endif
