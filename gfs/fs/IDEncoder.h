#ifndef STARKNOWLEDGEGRAPHDATABASE_IDENCODER_H
#define STARKNOWLEDGEGRAPHDATABASE_IDENCODER_H

#include <string>

#include "util/status.h"
#include "util/types.h"
#include <map>
namespace skg {

    class IDEncoder {
    public:
        enum class OpenMode {
            READ_WRITE,
            READ_ONLY,
            BULK_LOAD,
        };
    public:

        virtual ~IDEncoder() = default;

        inline Status Open(const std::string &dirname) {
            return Open(dirname, OpenMode::READ_WRITE);
        }

        virtual
        Status Open(const std::string &dirname, OpenMode mode) = 0;

        virtual
        Status Close() = 0;

        virtual
        Status Flush() = 0;

        virtual
        Status Put(const std::string &label, const std::string &vertex, vid_t vid) = 0;

        virtual
        Status PutBatch(
                const std::vector<std::tuple<std::string, std::string, vid_t>> &batch) = 0;

        virtual
        Status GetIDByVertex(const std::string &label, const std::string &vertex, vid_t *vid) = 0;

        virtual
        Status GetIDByVertexBatch(
                const std::vector<std::tuple<std::string, std::string>> &batch,
                std::vector<vid_t> *vid_batch) = 0;

        virtual
        Status GetVertexByID(const vid_t vid, std::string *label, std::string *vertex) = 0;

        virtual
        Status GetVertexByIDBatch(
                const std::vector<vid_t> &batch,
                std::vector<std::tuple<std::string, std::string>> *vertex_batch) = 0;

        virtual
        Status DeleteVertex(
                const std::string &label, const std::string &vertex,
                vid_t vid) = 0;
    };
}

#endif //STARKNOWLEDGEGRAPHDATABASE_IDMAPPING_H
