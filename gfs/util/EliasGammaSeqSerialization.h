#ifndef STARKNOWLEDGEGRAPHDATABASE_ELIASGAMMASEQSERIALIZATION_H
#define STARKNOWLEDGEGRAPHDATABASE_ELIASGAMMASEQSERIALIZATION_H

#include <util/skglogger.h>
#include "util/EliasGammaSeq.h"
//#include "elias-gamma.pb.h"

namespace skg {

class EliasGammaSeqSerialization {
public:
	/*
    static
    std::string Encode(const EliasGammaSeq &seq) {
        std::string str;
        skg::proto::eliasgamma::PbEliasGammaSeq pb;
        pb.set_length(seq.length());
        pb.set_index_interval(seq.m_index_interval);
        pb.set_bits(seq.m_bytes.data(), seq.m_bytes.size());
        auto pb_index_bit_idx = pb.mutable_index_bit_idx();
        for (auto i : seq.m_index_bit_idx) {
            pb_index_bit_idx->Add(i);
        }
        auto pb_index_values = pb.mutable_index_values();
        for (auto m_index_value : seq.m_index_values) {
            pb_index_values->Add(m_index_value);
        }
        if (pb.SerializeToString(&str)) {
            return str;
        } else {
            SKG_LOG_ERROR("while serialize elias-gamma-sequence", "");
            return "";
        }
    }

    static
    bool Decode(const std::string &pb_bytes, EliasGammaSeq *seq) {
        assert(seq != nullptr);
        skg::proto::eliasgamma::PbEliasGammaSeq pb;
        if (!pb_bytes.empty() && pb.ParseFromString(pb_bytes)) {
            seq->m_length = pb.length();
            seq->m_index_interval = pb.index_interval();
            seq->m_bytes.resize(pb.bits().size());
            memcpy(seq->m_bytes.data(), pb.bits().data(), pb.bits().size());
            seq->m_index_bit_idx.resize(pb.index_bit_idx_size());
            for (int i = 0; i < pb.index_bit_idx_size(); ++i) {
                seq->m_index_bit_idx[i] = pb.index_bit_idx(i);
            }
            seq->m_index_values.resize(pb.index_values_size());
            for (int i = 0; i < pb.index_values_size(); ++i) {
                seq->m_index_values[i] = pb.index_values(i);
            }
            return true;
        } else {
            return false;
        }
    }
    */
public:
    EliasGammaSeqSerialization() = delete;
    EliasGammaSeqSerialization(const EliasGammaSeqSerialization &) = delete;
    EliasGammaSeqSerialization& operator=(const EliasGammaSeqSerialization &) = delete;
};

}

#endif //STARKNOWLEDGEGRAPHDATABASE_ELIASGAMMASEQSERIALIZATION_H
