#ifndef STARKNOWLEDGEGRAPHDATABASE_IPREPROCESSOR_HPP
#define STARKNOWLEDGEGRAPHDATABASE_IPREPROCESSOR_HPP

#include "skgdb/types.h"
#include "preprocessing/filter.h"

namespace skg { namespace preprocess {

    template <typename EdgeDataType, typename FinalEdgeDataType=EdgeDataType>
    class Preprocessor {
    public:
        virtual void set_duplicate_filter(DuplicateEdgeFilter<EdgeDataType> *filter) = 0;
        virtual void end_preprocessing() = 0;
        virtual void start_preprocessing() = 0;
        virtual void preprocessing_add_edge(const vid_t from, const vid_t to, const EdgeDataType &val) = 0;
    };

}}

#endif //STARKNOWLEDGEGRAPHDATABASE_IPREPROCESSOR_HPP
