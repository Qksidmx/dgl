#ifndef STARKNOWLEDGEGRAPHDATABASE_RESULTVERTEX_H
#define STARKNOWLEDGEGRAPHDATABASE_RESULTVERTEX_H

#include <string>

#include "util/types.h"
#include "ResultProperties.h"

namespace skg {
class ResultVertex {
public:
    // 节点的类型
    EdgeTag_t tag;
    // 节点的 id
    vid_t m_vertex;
    // 节点的 string-id
    std::string m_s_vertex;
    // 节点的属性数据
    ResultProperties m_properties;

    ResultVertex(): ResultVertex(0, 0, "", ResultProperties(0)){
    }

    ResultVertex(EdgeTag_t tag, vid_t vid, const std::string &s_vertex,
                 const ResultProperties &prop):
        tag(tag), m_vertex(vid), m_s_vertex(s_vertex), m_properties(prop) {
    }

};
}

#endif //STARKNOWLEDGEGRAPHDATABASE_RESULTVERTEX_H
