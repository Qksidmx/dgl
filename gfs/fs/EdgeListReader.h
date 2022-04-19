#ifndef STARKNOWLEDGEGRAPHDATABASE_EDGELISTREADER_H
#define STARKNOWLEDGEGRAPHDATABASE_EDGELISTREADER_H

#include <sys/mman.h>

#include "util/skgfilenames.h"

namespace skg {

class EdgeListReader {
public:
    virtual ~EdgeListReader() {
    }

    virtual Status Open() = 0;

    virtual Status Flush() = 0;

    virtual void Close() = 0;

    virtual const PersistentEdge &GetImmutableEdge(const idx_t idx, char *buf) const = 0;

    virtual PersistentEdge *GetMutableEdge(const idx_t idx, char *buf) = 0;

    virtual Status Set(const idx_t idx, const PersistentEdge *const pEdge) = 0;

    virtual idx_t num_edges() const = 0;

    virtual const std::string &filename() const = 0;
};
}

#endif //STARKNOWLEDGEGRAPHDATABASE_EDGELISTREADER_H
