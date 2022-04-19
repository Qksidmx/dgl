#ifndef C_FILENAMES_DEF
#define C_FILENAMES_DEF

#include <fstream>
#include <fcntl.h>
#include <string>
#include <sstream>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <vector>
#include <sys/stat.h>

#include "internal_types.h"
#include "pathutils.h"
#include "types.h"
#include "logger.h"
#include "skglogger.h"
#include "fmt/format.h"
#include "cmdopts.h"
#include "skgfilenames.h"

namespace skg {


    /****************************************************
     *           deprecated api.                        *
     *           TODO remove old deprecated api         *
     ****************************************************/

    static VARIABLE_IS_NOT_USED std::string dirname_shard_edge_columns (const std::string &shardfile) {
        return fmt::format("{}_col", shardfile);
    }

    static VARIABLE_IS_NOT_USED std::string filename_shard_edge_column(
            const std::string &shardfile, const std::string &colname) {
        return fmt::format("{}/{}", dirname_shard_edge_columns(shardfile), colname);
    }

    static VARIABLE_IS_NOT_USED std::string filename_shard_edge_column_size(
            const std::string &column_dir, const std::string &colname) {
        return fmt::format("{}/{}.col.sz", column_dir, colname);
    }

    static
    std::string VARIABLE_IS_NOT_USED gshovel_filename(const std::string &basefilename, int idx) {
        return fmt::format("{}.{:03d}.shovel", basefilename, idx);
    }

    // === beg block 形式存储的边属性 === //
    static VARIABLE_IS_NOT_USED std::string dirname_shard_edge_column_block(
            const std::string &shardfile, const std::string &colname) {
        return fmt::format("{}/{}", dirname_shard_edge_columns(shardfile), colname);
    }

    static VARIABLE_IS_NOT_USED std::string filename_shard_edge_column_block(const std::string &column_dir, size_t blockid) {
        return fmt::format("{}/{}", column_dir, blockid);
    }

    static VARIABLE_IS_NOT_USED std::string filename_shard_edge_column_size(const std::string &column_dir) {
        return fmt::format("{}/col.sz", column_dir);
    }
    // === end block 形式存储的边属性 === //

}

#endif

