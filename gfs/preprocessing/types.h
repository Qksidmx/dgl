#ifndef STARKNOWLEDGEGRAPHDATABASE_TYPES_HPP
#define STARKNOWLEDGEGRAPHDATABASE_TYPES_HPP

#include "util/types.h"
#include "util/internal_types.h"

namespace skg { namespace preprocess {

#ifdef SKG_PREPROCESS_FIX_EDGE
#pragma pack(push)
#pragma pack(1)  // 设定为1字节对齐
    class PreprocessEdge {
    public:
        vid_t src;
        vid_t dst;
        uint8_t type;
        uint32_t weight;
        PreprocessEdge(vid_t src_, vid_t dst_, uint8_t type_, uint32_t weight_)
                : src(src_), dst(dst_), type(type_), weight(weight_) {
        }
        PreprocessEdge(vid_t src_, vid_t dst_, uint8_t type_)
                : PreprocessEdge(src_, dst_, type_, 0) {
        }
        PreprocessEdge(vid_t src_, vid_t dst_)
                : PreprocessEdge(src_, dst_, 0, 0) {
        }
        PreprocessEdge() : PreprocessEdge(0, 0, 0, 0) {}
    };
#pragma pack(pop)
#elif defined SKG_PREPROCESS_DYNAMIC_EDGE
    class PreprocessEdge {
    public:
        vid_t src;
        vid_t dst;
        const char *cols_data;
        PreprocessEdge(vid_t src_, vid_t dst_, const char *data_)
                : src(src_), dst(dst_), cols_data(data_) {
        }
    };
#endif

    template <typename E, typename Compare>
    void sorted_indexes(const E *edges, Compare cmp, std::vector<idx_t> *pIdx) {
        std::sort(pIdx->begin(), pIdx->end(),
                  [&edges, &cmp](const idx_t &i1, const idx_t &i2) -> bool {return cmp(edges[i1], edges[i2]);}
        );
    };

}}
#endif //STARKNOWLEDGEGRAPHDATABASE_TYPES_HPP
