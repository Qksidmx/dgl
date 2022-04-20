#ifndef STARKNOWLEDGEGRAPHDATABASE_BLOCKSCACHE_H
#define STARKNOWLEDGEGRAPHDATABASE_BLOCKSCACHE_H

#include <cstdlib>
#include <memory>
#include <set>

#include "util/types.h"
#include "util/status.h"
#include "util/LRUCache.h"
#include "util/internal_types.h"
#include "util/skglogger.h"
#include "util/skgfilenames.h"
#include "metrics/metrics.hpp"

namespace skg {

    struct CachedBlock {
        const std::string fname;
        size_t len;
        bool isDirty;
        char *data;

        CachedBlock(const std::string &fname_, size_t len_, char *data_)
                : fname(fname_), len(len_), isDirty(false), data(data_) {
        }

        ~CachedBlock() {
            delete[] data;
        }

        // disable copying
        CachedBlock(const CachedBlock &) = delete;
        CachedBlock& operator=(const CachedBlock &) = delete;
    };

    struct CachedBlock2 {
        FILE *const f;
        const size_t off;
        size_t len;
        bool isDirty;
        char *data;

        CachedBlock2(FILE *f_, size_t off_, size_t len_, char *data_)
                : f(f_), off(off_), len(len_), isDirty(false), data(data_) {
        }

        ~CachedBlock2() {
            delete [] data;
        }

        // disable copying
        CachedBlock2(const CachedBlock2 &) = delete;
        CachedBlock2& operator=(const CachedBlock2 &) = delete;
    };

    class BlocksCacheManager {
        static BlocksCacheManager *m_instance;
    public:
        static void InitializeInstance(size_t budget_mb, metrics *m) {
            m_instance = new BlocksCacheManager(budget_mb, m);
        }
        static BlocksCacheManager* GetInstance() {
            return m_instance;
        }
    private:
        size_t m_budget_mb;
        LruCache<std::string, std::shared_ptr<CachedBlock>> m_cached_blocks;
        std::map<interval_t, std::set<std::string> > m_shards_blocks;
        std::mutex m_mutex;
        metrics *m_metrics;
    private:
        explicit
        BlocksCacheManager(size_t budget_mb, metrics *m)
                : m_budget_mb(budget_mb),
                  m_cached_blocks(), m_shards_blocks(),
                  m_mutex(),
                  m_metrics(m) {
            m_cached_blocks.SetCapacity(budget_mb * 1024 * 1024 / BASIC_BLOCK_SIZE);
        }

    public:
        Status SetMetrics(metrics *m) {
            m_metrics = m;
            return Status::OK();
        }

        Status Get(const interval_t &interval, const std::string &blockfile,
                   std::shared_ptr<CachedBlock> *block) {
            if (m_cached_blocks.Get(blockfile, block)) {
                // ====== cache hit ====== //
                if (m_metrics != nullptr) { m_metrics->add("BlocksCache.hit", 1.0); }
//                SKG_LOG_DEBUG(fmt::format("{} hit cache.", blockfile));
                return Status::OK();
            } else {
                // ====== cache miss ====== //
                if (m_metrics != nullptr) { m_metrics->add("BlocksCache.miss", 1.0); }
//                SKG_LOG_DEBUG(fmt::format("{} cache miss. Loading..", blockfile));

                std::lock_guard<std::mutex> lock(m_mutex);

                // read block into memory
                if (m_metrics != nullptr) { m_metrics->start_time("BlocksCache.miss.load"); }
                const size_t size = PathUtils::getsize(blockfile);
                FILE *f = fopen(blockfile.c_str(), "rb");
                if (f == nullptr) {
                    return Status::FileNotFound(fmt::format("Block: {}", blockfile));
                }
                std::shared_ptr<CachedBlock> newBlock = std::make_shared<CachedBlock>(
                        blockfile, size, new char[size]
                );
                size_t nread = fread(newBlock->data, sizeof(char), newBlock->len, f);
                if (nread != newBlock->len) {
                    fclose(f);
                    return Status::IOError(fmt::format("Can NOT read {} bytes from {}.",
                                                       newBlock->len, blockfile));
                }
                fclose(f);
                if (m_metrics != nullptr) { m_metrics->stop_time("BlocksCache.miss.load"); }

                Status s;
                if (m_metrics != nullptr) { m_metrics->start_time("BlocksCache.miss.push"); }
                // push block to cache
                std::shared_ptr<CachedBlock> eliminatedBlock;
                m_cached_blocks.Set(blockfile, newBlock, &eliminatedBlock);
                m_shards_blocks[interval].insert(blockfile);
//                SKG_LOG_DEBUG("Block {}({}) loaded.", newBlock->fname, newBlock->len);
                *block = std::move(newBlock);
                if (m_metrics != nullptr) { m_metrics->stop_time("BlocksCache.miss.push"); }

                // 如果有被淘汰的block, 刷到磁盘上
                if (eliminatedBlock != nullptr) {
                    if (m_metrics != nullptr) { m_metrics->start_time("BlocksCache.miss.eliminated"); }
//                    SKG_LOG_DEBUG("Block {}({}) eliminated.", eliminatedBlock->fname, eliminatedBlock->len);
                    s = this->FlushBlock(std::move(eliminatedBlock));
                    if (m_metrics != nullptr) { m_metrics->stop_time("BlocksCache.miss.eliminated"); }
                    if (!s.ok()) { return s; }
                }
            }
            return Status::OK();
        }

        // 把该interval对应的shard的属性所有block刷到磁盘
        Status Flush(const interval_t &interval) {
            std::lock_guard<std::mutex> lock(m_mutex);

            const auto &iter = m_shards_blocks.find(interval);
            if (iter == m_shards_blocks.end()) {
                return Status::OK();
            } else {
                SKG_LOG_DEBUG("Flushing all blocks of {}", interval);
                Status s;
                const std::set<std::string> cachedBlockNames = iter->second;
                std::shared_ptr<CachedBlock> eliminatedBlock;
                // 清空 interval 对应的所有已缓存的block
                for (const auto &name: cachedBlockNames) {
                    if (m_cached_blocks.isExist(name)) {
                        m_cached_blocks.Erase(name, &eliminatedBlock);
                        if (eliminatedBlock != nullptr) {
                            s = FlushBlock(std::move(eliminatedBlock));
                            if (!s.ok()) { return s; }
                        }
                        eliminatedBlock.reset();
                    }
                }
                // 清空旧的记录
                m_shards_blocks.erase(iter);
                return s;
            }
        }

    private:
        Status FlushBlock(std::shared_ptr<CachedBlock> block) {
            if (!block->isDirty) {
                return Status::OK();
            }
            FILE *f = fopen(block->fname.c_str(), "wb");
            if (f == nullptr) {
                return Status::IOError(fmt::format("Can not flush edge block: {}",
                                                   block->fname));
            }
            const size_t nwrite = fwrite(block->data, sizeof(char), block->len, f);
            if (nwrite != block->len) {
                fclose(f);
                return Status::IOError(
                        fmt::format("Error while flush block {}, "
                                    "expect write {} bytes but {} in actual.",
                                    block->fname, block->len, nwrite));
            }
            fclose(f);
            // 清空该block的缓存
            block.reset();
            return Status::OK();
        }

    public:
        BlocksCacheManager(const BlocksCacheManager &) = delete;
        BlocksCacheManager& operator=(const BlocksCacheManager&) = delete;
    };

    class BlocksCacheManager2 {
        static BlocksCacheManager2 *m_instance;
    public:
        static void InitializeInstance(size_t budget_mb, metrics *m) {
            m_instance = new BlocksCacheManager2(budget_mb, m);
        }
        static BlocksCacheManager2* GetInstance() {
            return m_instance;
        }
    private:
        size_t m_budget_mb;
        LruCache<std::string, std::shared_ptr<CachedBlock2>> m_cached_blocks;
        std::map<interval_t, std::set<std::string> > m_shards_blocks;
        std::mutex m_mutex;
        metrics *m_metrics;
    public:
        explicit
        BlocksCacheManager2(size_t budget_mb, metrics *m)
                : m_budget_mb(budget_mb),
                  m_cached_blocks(), m_shards_blocks(),
                  m_mutex(),
                  m_metrics(m) {
            m_cached_blocks.SetCapacity(budget_mb * 1024 * 1024 / BASIC_BLOCK_SIZE);
        }

        Status SetMetrics(metrics *m) {
            m_metrics = m;
            return Status::OK();
        }

        Status Get(const interval_t &interval,
                   const std::string &filename,
                   FILE *const f, size_t offset, size_t len,
                   std::shared_ptr<CachedBlock2> *block) {
            if (m_cached_blocks.Get(filename, block)) {
                // ====== cache hit ====== //
                if (m_metrics != nullptr) { m_metrics->add("BlocksCache.hit", 1.0); }
//                SKG_LOG_DEBUG(fmt::format("{} hit cache.", blockfile));
                return Status::OK();
            } else {
                // ====== cache miss ====== //
                if (m_metrics != nullptr) { m_metrics->add("BlocksCache.miss", 1.0); }
//                SKG_LOG_DEBUG(fmt::format("{} cache miss. Loading..", blockfile));

                std::lock_guard<std::mutex> lock(m_mutex);

                // read block into memory
                if (m_metrics != nullptr) { m_metrics->start_time("BlocksCache.miss.load"); }
                std::shared_ptr<CachedBlock2> newBlock = std::make_shared<CachedBlock2>(
                        f, offset, len, new char[len]
                );
                if (m_metrics != nullptr) { m_metrics->start_time("BlocksCache.miss.load.seek"); }
                if (fseek(f, offset, SEEK_SET) != 0) {
                    return Status::IOError(fmt::format("Can NOT fseek file:off {}:{}", filename, offset));
                }
                if (m_metrics != nullptr) { m_metrics->stop_time("BlocksCache.miss.load.seek"); }
                if (m_metrics != nullptr) { m_metrics->start_time("BlocksCache.miss.load.read"); }
                size_t nread = fread(newBlock->data, sizeof(char), newBlock->len, f);
                if (nread != newBlock->len) {
//                    return Status::IOError(fmt::format("Can NOT read {} bytes from file:off {}:{}.",
//                                                       newBlock->len, filename, offset));
                    // 两种情况: 1. offset 开始, 未读到 len, 文件已经结束; 2. fread 因为其他原因出错未能一次性读完?
                    // 针对情况1, 暂时先把 block->len 设置为读入的大小
                    newBlock->len = sizeof(char) * nread;
                }
                if (m_metrics != nullptr) { m_metrics->stop_time("BlocksCache.miss.load.read"); }
                if (m_metrics != nullptr) { m_metrics->stop_time("BlocksCache.miss.load"); }

                Status s;
                if (m_metrics != nullptr) { m_metrics->start_time("BlocksCache.miss.push"); }
                // push block to cache
                std::shared_ptr<CachedBlock2> eliminatedBlock;
                m_cached_blocks.Set(filename, newBlock, &eliminatedBlock);
                m_shards_blocks[interval].insert(filename);
//                SKG_LOG_DEBUG("Block {}({}) loaded.", newBlock->fname, newBlock->len);
                *block = std::move(newBlock);
                if (m_metrics != nullptr) { m_metrics->stop_time("BlocksCache.miss.push"); }

                // 如果有被淘汰的block, 刷到磁盘上
                if (eliminatedBlock != nullptr) {
                    if (m_metrics != nullptr) { m_metrics->start_time("BlocksCache.miss.eliminated"); }
//                    SKG_LOG_DEBUG("Block {}({}) eliminated.", eliminatedBlock->fname, eliminatedBlock->len);
                    s = this->FlushBlock(std::move(eliminatedBlock));
                    if (m_metrics != nullptr) { m_metrics->stop_time("BlocksCache.miss.eliminated"); }
                    if (!s.ok()) { return s; }
                }
            }
            return Status::OK();
        }

        // 把该interval对应的shard的属性所有block刷到磁盘
        Status Flush(const interval_t &interval) {
            std::lock_guard<std::mutex> lock(m_mutex);

            const auto &iter = m_shards_blocks.find(interval);
            if (iter == m_shards_blocks.end()) {
                return Status::OK();
            } else {
                SKG_LOG_TRACE("Flushing all blocks of {}", interval);
                Status s;
                const std::set<std::string> cachedBlockKeys = iter->second;
                std::shared_ptr<CachedBlock2> eliminatedBlock;
                // 清空 interval 对应的所有已缓存的block
                for (const auto &name: cachedBlockKeys) {
                    if (m_cached_blocks.isExist(name)) {
                        m_cached_blocks.Erase(name, &eliminatedBlock);
                        if (eliminatedBlock != nullptr) {
                            s = FlushBlock(std::move(eliminatedBlock));
                            if (!s.ok()) { return s; }
                        }
                        eliminatedBlock.reset();
                    }
                }
                // 清空旧的记录
                m_shards_blocks.erase(iter);
                return s;
            }
        }

    private:
        Status FlushBlock(std::shared_ptr<CachedBlock2> block) {
            if (!block->isDirty) {
                return Status::OK();
            }
            if (fseek(block->f, block->off, SEEK_SET) != 0) {
                return Status::IOError(fmt::format(
                        "Can NOT fseek file:off:len {}:{}",
                        block->off, block->len));
            }
            const size_t nwrite = fwrite(block->data, sizeof(char), block->len, block->f);
            if (nwrite != block->len) {
                return Status::IOError(
                        fmt::format("Error while flush block, "
                                    "expect write {} bytes but {} in actual.",
                                    block->len, nwrite));
            }
            // 清空该block的缓存
            block.reset();
            return Status::OK();
        }

    public:
        BlocksCacheManager2(const BlocksCacheManager2 &) = delete;
        BlocksCacheManager2& operator=(const BlocksCacheManager2&) = delete;
    };
}

#endif //STARKNOWLEDGEGRAPHDATABASE_BLOCKSCACHE_H
