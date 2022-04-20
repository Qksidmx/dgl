#ifndef STARKNOWLEDGEGRAPH_SHOVELER_HPP
#define STARKNOWLEDGEGRAPH_SHOVELER_HPP

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>

#include "skgdb/Options.h"
#include "skgdb/idencoder/IDEncoder.h"
#include "preprocessing/filter.h"
#include "preprocessing/types.h"
#include "preprocessing/ipreprocessor.hpp"
#include "metrics/metrics.hpp"
#include "metrics/reps/basic_reporter.hpp"
#include "logger/logger.hpp"
#include "util/radixSort.hpp"
#include "util/ioutil.hpp"

namespace skg { namespace preprocess {

    struct shovel_flushinfo {
        std::string shovelname;
        vid_t max_vertex;
#ifdef SKG_PREPROCESS_FIX_EDGE
        // 使用固定字段的边进行预处理, 编译后, 预处理时无法支持tag/weight以外的边属性
        std::vector<PreprocessEdge> buffer;
#elif defined SKG_PREPROCESS_DYNAMIC_EDGE
        // 预处理时支持任意的边属性列
        // 预先开边属性的buffer, 避免下面方法在预处理过程中大量申请小块内存
        std::vector<PreprocessEdge> buffer;
        char *cols_buffer;
        size_t value_bytes;
#else
        // 支持预处理时增加任意的边属性列, 但效率十分低
        // 因为在预处理过程中会有很多小块内存的申请
        std::vector<MemoryEdge> buffer;
#endif

#ifdef SKG_PREPROCESS_FIX_EDGE
        shovel_flushinfo(const std::string &shovelname, vid_t max_vertex,
                          std::vector<PreprocessEdge> &&edges)
                : shovelname(shovelname), max_vertex(max_vertex), buffer(edges) {
        }
        void flush() {
            /* Sort */
            SKG_LOG_INFO("Sorting shovel: {}, max: {}", shovelname, max_vertex);
            // 按照dst排序
            std::sort(
                    buffer.begin(), buffer.end(),
                    [](const PreprocessEdge &lhs, const PreprocessEdge &rhs){
                        return lhs.dst < rhs.dst;
                    });
            SKG_LOG_INFO("Sort done. {}", shovelname);
            FILE *f = fopen(shovelname.c_str(), "wb");
            fwrite(buffer.data(), sizeof(PreprocessEdge), buffer.size(), f);
            fclose(f);
            // 清空buffer
            buffer.clear();
            buffer.shrink_to_fit();
        }
#elif defined SKG_PREPROCESS_DYNAMIC_EDGE
        shovel_flushinfo(const std::string &shovelname, vid_t max_vertex,
                         std::vector<PreprocessEdge> &&edges,
                         char *cols_buffer_, size_t value_bytes)
                : shovelname(shovelname), max_vertex(max_vertex),
                  buffer(edges), cols_buffer(cols_buffer_), value_bytes(value_bytes) {
        }
        void flush() {
            /* Sort */
            SKG_LOG_INFO("Sorting shovel: {}, max: {}", shovelname, max_vertex);
            // 按照dst排序
            std::sort(
                    buffer.begin(), buffer.end(),
                    [](const PreprocessEdge &lhs, const PreprocessEdge &rhs) {
                        return lhs.dst < rhs.dst;
                    });
            SKG_LOG_INFO("Sort done. {}", shovelname);
            FILE *f = fopen(shovelname.c_str(), "wb");
            for (size_t i = 0; i < buffer.size(); ++i) {
                fwrite(&buffer[i].src, sizeof(vid_t), 1, f);
                fwrite(&buffer[i].dst, sizeof(vid_t), 1, f);
                fwrite(buffer[i].cols_data, value_bytes, 1, f);
            }
            fclose(f);
            free(cols_buffer);
            buffer.clear();
            buffer.shrink_to_fit();
        }
#else
        shovel_flushinfo(const std::string &shovelname, vid_t max_vertex,
                          std::vector<MemoryEdge> &&edges)
                : shovelname(shovelname), max_vertex(max_vertex), buffer(edges) {
        }
        void flush() {
            /* Sort */
            SKG_LOG_INFO("Sorting shovel: {}, max: {}", shovelname, max_vertex);
            // 按照dst排序
            std::sort(buffer.begin(), buffer.end(), MemoryEdgeDstSortedFunc());
            SKG_LOG_INFO("Sort done. {}", shovelname);
            FILE *f = fopen(shovelname.c_str(), "wb");
            for (const MemoryEdge &edge: buffer) {
                fwrite(&edge.src, sizeof(vid_t), 1, f);
                fwrite(&edge.dst, sizeof(vid_t), 1, f);
                fwrite(edge.GetColsData().data(), sizeof(u_char), edge.GetColsData().size(), f);
            }
            fclose(f);
            // 清空buffer
            buffer.clear();
            buffer.shrink_to_fit();
        }
#endif
    };

    // Run in a thread
    static void *shelter_flush_run(void *_info) {
        auto *task = static_cast<shovel_flushinfo*>(_info);
        task->flush();
        return nullptr;
    }


    class Shoveler {
    public:
        explicit Shoveler(const std::string &outPrefix,
                          const std::vector<IEdgeColumnPtr> &cols,
                          const size_t edgeColsBytes,
                          std::shared_ptr<IDEncoder> encoder,
                          const Options &options)
                : m("shelter"), m_basefilename(outPrefix),
                  max_vertex_id(0), num_shelters(0), m_num_edges(0),
                  curshovel_buffer(),
#ifdef SKG_PREPROCESS_DYNAMIC_EDGE
                  cols_buffer(nullptr), cols_buffer_ptr(nullptr),
#endif
                  shovelsize(1024),
                  shelter_threads(), shoveltasks(),
                  m_cols(cols), m_parseColPos(), m_edge_cols_bytes(edgeColsBytes),
                  m_encoder(encoder)
#ifdef SKG_PREPROCESS_FIX_EDGE
                  ,m_tag_col(), m_weight_col() {
#else
                {
#endif
        }

        virtual ~Shoveler() {
            for (size_t i = 0; i < shoveltasks.size(); ++i) {
                delete shoveltasks[i];
                shoveltasks[i] = nullptr;
            }
        }

        const std::vector<IEdgeColumnPtr>& cols() const {
            return m_cols;
        }

        std::vector<IEdgeColumnPtr> GetParseCols() const {
            std::vector<IEdgeColumnPtr> cols;
            for (size_t i = 0; i < m_parseColPos.size(); ++i) {
                cols.emplace_back(m_cols[m_parseColPos[i]]);
            }
            return cols;
        }

        size_t edge_cols_size() const {
            return m_edge_cols_bytes;
        }

        void start_preprocessing() {
            m.start_time("preprocessing");

            num_shelters = 0;
            const size_t membudget_b = static_cast<const size_t>(get_option_int("membudget_mb", 1024)) * 1024 * 1024;
            const size_t edge_size = sizeof(vid_t) + sizeof(vid_t) + m_edge_cols_bytes;
            shovelsize = (membudget_b / 4 / edge_size);  // FIXME 为什么需要/4?

            SKG_LOG_INFO("Starting preprocess, shovel size: {}", shovelsize);
#ifdef SKG_PREPROCESS_FIX_EDGE
            for (size_t i = 0; i < m_cols.size(); ++i) {
                const IEdgeColumnPtr &col = m_cols[i];
                if (col->name() == "tag" && col->edgeColType() == ColumnType::BYTE) {
                    m_tag_col = col;
                } else if (col->name() == "weight" && col->edgeColType() == ColumnType::INT32) {
                    m_weight_col = col;
                } else {
                    assert(false);
                    SKG_LOG_FATAL("Can NOT preprocess with col: " << col->name());
                }
            }
#elif defined SKG_PREPROCESS_DYNAMIC_EDGE
            // 调整边的属性列位置
            this->AdjustColPosition();
            curshovel_buffer.reserve(shovelsize);
            cols_buffer = static_cast<char *>(malloc(m_edge_cols_bytes * shovelsize));
            cols_buffer_ptr = cols_buffer;
#endif

            shelter_threads.clear();

            /* Write the maximum vertex id place holder - to be filled later */
            max_vertex_id = 0;
        }

        void end_preprocessing() {
            this->flush_shovel(false);
            m_encoder.reset();
            m.stop_time("preprocessing");

            basic_reporter rep;
            m.report(rep);
        }

        Status preprocessing_add_edge(
                const std::string &src, const std::string &dst, const Bytes &val) {
            Status s;
            vid_t srcid = 0, dstid = 0;
            s = m_encoder->GetIDByVertex("", src, &srcid);
            if (!s.ok()) { return s; }
            s = m_encoder->GetIDByVertex("", dst, &dstid);
            if (!s.ok()) { return s; }
            return preprocessing_add_edge(srcid, dstid, val);
        }

        Status preprocessing_add_edge(
                const vid_t from, const vid_t to, const Bytes &val) {
            if (from == to) {
                // Do not allow self-edges
                return Status::InvalidArgument();
            }
#ifdef SKG_PREPROCESS_FIX_EDGE
            // 插入到buffer
            uint8_t tag = 0;
            if (m_tag_col != nullptr) {
                tag = *(val.data() + m_tag_col->GetOffset());
            }
            uint32_t weight = 0;
            if (m_weight_col != nullptr) {
                weight = *reinterpret_cast<const uint32_t *>(val.data() + m_weight_col->GetOffset());
            }
            PreprocessEdge edge(from, to, tag, weight);
            curshovel_buffer.emplace_back(edge);
            if (curshovel_buffer.size() == shovelsize) { // buffer满了, 生成子线程刷磁盘
                this->flush_shovel();
            }
#elif defined SKG_PREPROCESS_DYNAMIC_EDGE
            memcpy(cols_buffer_ptr, val.data(), m_edge_cols_bytes);  // copy 边属性数据
            PreprocessEdge edge(from, to, cols_buffer_ptr);
            cols_buffer_ptr += m_edge_cols_bytes;
            curshovel_buffer.emplace_back(edge);
            if (curshovel_buffer.size() == shovelsize) { // buffer满了, 生成子线程刷盘
                this->flush_shovel();
            }
#else
            MemoryEdge edge(from, to, m_edge_cols_bytes);
            edge.SetData(val.data(), 0, val.size(), val.size());
            curshovel_buffer.emplace_back(edge);
            if (curshovel_buffer.size() == shovelsize) { // buffer满了, 生成子线程刷磁盘
                this->flush_shovel();
            }
#endif

            // update max vertex id
            max_vertex_id = std::max(std::max(from, to), max_vertex_id);
            return Status::OK();
        }

        size_t GetNumSheltersEdge() const {
            return m_num_edges;
        }

        uint32_t GetNumShelter() const {
            return num_shelters;
        }

        vid_t GetMaxVertexID() const {
            return max_vertex_id;
        }

    protected:

        /**
         * 调整属性列的位置
         */
        void AdjustColPosition() {
            SKG_LOG_DEBUG("adjusting edge cols position", "");
            std::vector<IEdgeColumnPtr> newCols;
            std::vector<size_t> parsePosition(m_cols.size(), 0);
            for (size_t round = 0; round < 3; ++round) {
                if (round == 0) {
                    // 如果有标签列, 提到前面
                    for (size_t i = 0; i < m_cols.size(); ++i) {
                        if (m_cols[i] == nullptr) { continue; }
                        if (m_cols[i]->edgeColType() == ColumnType::TAG) {
                            parsePosition[i] = newCols.size();
                            newCols.emplace_back(std::move(m_cols[i]));
                            break;
                        }
                    }
                } else if (round == 1) {
                    // 如果有权重列, 提到前面
                    for (size_t i = 0; i < m_cols.size(); ++i) {
                        if (m_cols[i] == nullptr) { continue; }
                        if (m_cols[i]->edgeColType() == ColumnType::WEIGHT) {
                            parsePosition[i] = newCols.size();
                            newCols.emplace_back(std::move(m_cols[i]));
                            break;
                        }
                    }
                } else {
                    for (size_t i = 0; i < m_cols.size(); ++i) {
                        if (m_cols[i] == nullptr) { continue; }
                        parsePosition[i] = newCols.size();
                        newCols.emplace_back(std::move(m_cols[i]));
                    }
                }
            }
            size_t offset = 0;
            for (auto &col: newCols) {
                col->SetOffset(offset);
                offset += col->value_size();
            }
            std::swap(m_cols, newCols);
            std::swap(m_parseColPos, parsePosition);
        }

        void flush_shovel(bool async = true) {
            /* Flush in separate thread unless the last one */
            // 当前边缓冲区的指针,id等信息生成flushinfo
            m_num_edges += curshovel_buffer.size();
#ifdef SKG_PREPROCESS_FIX_EDGE
            shovel_flushinfo *flushinfo = new shovel_flushinfo(
                    gshovel_filename(m_basefilename, num_shelters),
                    max_vertex_id, std::move(curshovel_buffer)
            );
            curshovel_buffer.clear();
#elif defined SKG_PREPROCESS_DYNAMIC_EDGE
            shovel_flushinfo *flushinfo = new shovel_flushinfo(
                    gshovel_filename(m_basefilename, num_shelters),
                    max_vertex_id,
                    std::move(curshovel_buffer), cols_buffer, m_edge_cols_bytes
            );
            curshovel_buffer.clear();
            curshovel_buffer.reserve(shovelsize);
            cols_buffer = static_cast<char *>(malloc(m_edge_cols_bytes * shovelsize));
            cols_buffer_ptr = cols_buffer;
#else
            shovel_flushinfo *flushinfo = new shovel_flushinfo(
                    gshovel_filename(m_basefilename, num_shelters),
                    max_vertex_id, std::move(curshovel_buffer)
            );
            curshovel_buffer.clear();
#endif
            shoveltasks.push_back(flushinfo);
            num_shelters++;

            if (!async) {
                // 同步等待所有任务结束
                flushinfo->flush();

                /* Wait for threads to finish 等待之前异步刷磁盘的线程结束*/
                SKG_LOG_INFO("Waiting shoveling threads...", "");
                for (size_t i = 0; i < shelter_threads.size(); i++) {
                    pthread_join(shelter_threads[i], NULL);
                }
                SKG_LOG_INFO("All shoveling threads done.", "");
            } else {
                // 维持最多同时只有两个shovel处理线程 TODO 在 Options类 中指定
                if (shelter_threads.size() > static_cast<size_t>(get_option_int("shelter_threads", 2))) {
                    SKG_LOG_INFO("Too many outstanding shoveling threads: {} ...", shelter_threads.size());

                    for (size_t i = 0; i < shelter_threads.size(); i++) {
                        pthread_join(shelter_threads[i], NULL);
                    }
                    shelter_threads.clear();
                    SKG_LOG_INFO("All shoveling threads done.", "");
                }
                // 创建新线程
                pthread_t t;
                int ret = pthread_create(&t, NULL, shelter_flush_run, (void *) flushinfo);
                shelter_threads.push_back(t);
                assert(ret >= 0);
            }
        }

    private:

        metrics m;
        std::string m_basefilename;

        // the id of max vertex
        vid_t max_vertex_id;
        // num of shovel files
        uint32_t num_shelters;
        // 总共处理的边数
        size_t m_num_edges;

        // 缓冲区
#ifdef SKG_PREPROCESS_FIX_EDGE
        std::vector<PreprocessEdge> curshovel_buffer;
#elif defined SKG_PREPROCESS_DYNAMIC_EDGE
        std::vector<PreprocessEdge> curshovel_buffer;
        char * cols_buffer;
        char * cols_buffer_ptr;
#else
        std::vector<MemoryEdge> curshovel_buffer;
#endif
        size_t shovelsize;  // 缓冲区最大边数

        std::vector<pthread_t> shelter_threads;
        std::vector<shovel_flushinfo*> shoveltasks;

        std::vector<IEdgeColumnPtr> m_cols;
        std::vector<size_t> m_parseColPos;
        size_t m_edge_cols_bytes;
#ifdef SKG_PREPROCESS_FIX_EDGE
        IEdgeColumnPtr m_tag_col;
        IEdgeColumnPtr m_weight_col;
#endif

        std::shared_ptr<IDEncoder> m_encoder;
    };
}}

#endif //STARKNOWLEDGEGRAPH_SHOVELER_HPP
