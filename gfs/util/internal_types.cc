#include "internal_types.h"

#include <stdint.h>

#include "fmt/format.h"

namespace skg {

// ==== helper functions of interval_t ====//

void format_arg(
        fmt::BasicFormatter<char> &f,
        const char *&format_str,
        const interval_t &i) {
    i.write(f.writer());
}

// ==== helper functions of interval_t ====//

void PersistentEdge::CopyFrom(const MemoryEdge &edge) {
    src = edge.src;
    dst = edge.dst;
    weight = edge.weight;
    tag = edge.tag;
    memcpy(m_properties_bitset.m_bitset, edge.m_properties_bitset.m_bitset, sizeof(m_properties_bitset.m_bitset));
}

std::string EdgeLabel::ToString() const {
    return fmt::format("{}--{}->{}", src_label, edge_label, dst_label);
}

}
