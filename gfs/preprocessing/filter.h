#ifndef STARKNOWLEDGEGRAPHDATABASE_FILTER_H
#define STARKNOWLEDGEGRAPHDATABASE_FILTER_H
namespace skg{ namespace preprocess {
    template <typename EdgeDataType>
    class DuplicateEdgeFilter {
    public:
        virtual
        bool acceptFirst(const EdgeDataType& first,
                         const EdgeDataType& second) = 0;
    };
}}
#endif //STARKNOWLEDGEGRAPHDATABASE_FILTER_H
