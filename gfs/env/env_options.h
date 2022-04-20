#ifndef STARKNOWLEDGEGRAPH_ENV_OPTIONS_H
#define STARKNOWLEDGEGRAPH_ENV_OPTIONS_H

#include <cstdint>
#include <cstdio>

namespace skg {

class RateLimiter;

// Options while opening a file to read/write
struct EnvOptions {

    // Construct with default Options
    EnvOptions() {}

    // Construct from Options
//    explicit EnvOptions(const DBOptions& options);

    // If true, then use mmap to read data
    bool use_mmap_reads = false;

    // If true, then use FILE* to write data. Added by jayzonhuang
    bool use_clib_writes = true;

    // If true, then use mmap to write data
    bool use_mmap_writes = true;

    // If true, then use O_DIRECT for reading data
    bool use_direct_reads = false;

    // If true, then use O_DIRECT for writing data
    bool use_direct_writes = false;

    // If false, fallocate() calls are bypassed
    bool allow_fallocate = true;

    // If true, set the FD_CLOEXEC on open fd.
    bool set_fd_cloexec = true;

    // Allows OS to incrementally sync files to disk while they are being
    // written, in the background. Issue one request for every bytes_per_sync
    // written. 0 turns it off.
    // Default: 0
    uint64_t bytes_per_sync = 0;

    // If true, we will preallocate the file with FALLOC_FL_KEEP_SIZE flag, which
    // means that file size won't change as part of preallocation.
    // If false, preallocation will also change the file size. This option will
    // improve the performance in workloads where you sync the data on every
    // write. By default, we set it to true for MANIFEST writes and false for
    // WAL writes
    bool fallocate_with_keep_size = true;

    // See DBOptions doc
    size_t compaction_readahead_size;

    // See DBOptions doc
    size_t random_access_max_buffer_size;

    // See DBOptions doc
    size_t writable_file_max_buffer_size = 1024 * 1024;

    // If not nullptr, write rate limiting is enabled for flush and compaction
    RateLimiter *rate_limiter = nullptr;
};
}
#endif //STARKNOWLEDGEGRAPH_ENV_OPTIONS_H
