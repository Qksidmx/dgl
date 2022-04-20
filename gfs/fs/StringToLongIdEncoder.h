#ifndef STARKNOWLEDGEGRAPHDATABASE_STRINGTOLONGIDENCODER_H
#define STARKNOWLEDGEGRAPHDATABASE_STRINGTOLONGIDENCODER_H

#include <fmt/format.h>
#include "IDEncoder.h"
#include "util/types.h"

#include "util/StringUtils.h"

namespace skg {
    class StringToLongIdEncoder: public IDEncoder {
    public:
        StringToLongIdEncoder() {
        }

        Status Close() override {
            return Status::OK();
        }

        Status Open(const std::string &, OpenMode) override {
            return Status::OK();
        }

        Status Flush() override {
            return Status::OK();
        }

        Status Put(const std::string &label, const std::string &vertex, vid_t vid) override {
            return Status::OK();
        }

        Status PutBatch(
                const std::vector<std::tuple<std::string, std::string, vid_t>> &batch) override {
            return Status::OK();
        }

        Status GetIDByVertex(const std::string &label, const std::string &vertex, vid_t *vid) override {
            assert(vid != nullptr);
            // FIXME vid_t -> uint64 的时候, 需要切换为 ParseUint64
            Status s;
            *vid = StringUtils::ParseUint32(vertex, &s);
            return s;
        }

        Status GetIDByVertexBatch(
                const std::vector<std::tuple<std::string, std::string>> &batch,
                std::vector<vid_t> *vid_batch) override {
            assert(vid_batch != nullptr);
            Status s;
            vid_batch->resize(batch.size());
            for (size_t i = 0; i < batch.size(); ++i) {
                vid_batch->operator[](i) = StringUtils::ParseUint32(std::get<1>(batch[i]), &s);
            }
            return s;
        }

        Status GetVertexByID(const vid_t vid, std::string *label, std::string *vertex) override {
            assert(vertex != nullptr);
            *vertex = fmt::format("{}", vid);
            return Status::OK();
        }

        Status GetVertexByIDBatch(
                const std::vector<vid_t> &batch,
                std::vector<std::tuple<std::string, std::string>> *vertex_batch) override {
            assert(vertex_batch != nullptr);
            vertex_batch->resize(batch.size());
            for (size_t i = 0; i < batch.size(); ++i) {
                vertex_batch->operator[](i) = std::make_tuple(0, fmt::format("{}", batch[i]));
            }
            return Status::OK();
        }

        Status DeleteVertex(const std::string &label, const std::string &vertex, vid_t vid) override {
            return Status::OK();
        }
    };
}

#endif //STARKNOWLEDGEGRAPHDATABASE_STRINGTOLONGIDENCODER_H
