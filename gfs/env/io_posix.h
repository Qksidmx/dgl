//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include "env.h"
#include "fmt/format.h"

namespace skg {

    // file_name can be left empty if it is not unkown.
    static Status IOError(const std::string &context, const std::string &file_name,
                          int err_number) {
        switch (err_number) {
            case ENOSPC:
                return Status::NoSpace(fmt::format("{}: {}", context, file_name));
            case ESTALE:
                return Status::IOError(strerror(err_number));
            default:
                return Status::IOError(fmt::format("{}: {}", context, file_name));
        }
    }

    class PosixHelper {
    public:
        static size_t GetUniqueIdFromFile(int fd, char *id, size_t max_size);
    };

    class PosixSequentialFile : public SequentialFile {
    private:
        std::string filename_;
        FILE *file_;
        int fd_;
        bool use_direct_io_;
        size_t logical_sector_size_;

    public:
        PosixSequentialFile(const std::string &fname, FILE *file, int fd,
                            const EnvOptions &options);

        virtual ~PosixSequentialFile();

        virtual Status Read(size_t n, Slice *result, char *scratch) override;

        virtual Status PositionedRead(uint64_t offset, size_t n, Slice *result,
                                      char *scratch) override;

        virtual Status Skip(uint64_t n) override;

        virtual Status InvalidateCache(size_t offset, size_t length) override;

        virtual bool use_direct_io() const override { return use_direct_io_; }

        virtual size_t GetRequiredBufferAlignment() const override {
            return logical_sector_size_;
        }
    };

    class PosixRandomAccessFile : public RandomAccessFile {
    protected:
        std::string filename_;
        int fd_;
        bool use_direct_io_;
        size_t logical_sector_size_;

    public:
        PosixRandomAccessFile(const std::string &fname, int fd,
                              const EnvOptions &options);

        virtual ~PosixRandomAccessFile();

        virtual Status Read(uint64_t offset, size_t n, Slice *result,
                            char *scratch) const override;

        virtual Status Prefetch(uint64_t offset, size_t n) override;

#if defined(OS_LINUX) || defined(OS_MACOSX) || defined(OS_AIX)

        virtual size_t GetUniqueId(char *id, size_t max_size) const override;

#endif

        virtual void Hint(AccessPattern pattern) override;

        virtual Status InvalidateCache(size_t offset, size_t length) override;

        virtual bool use_direct_io() const override { return use_direct_io_; }

        virtual size_t GetRequiredBufferAlignment() const override {
            return logical_sector_size_;
        }
    };

    class PosixWritableFile : public WritableFile {
    protected:
        const std::string filename_;
        const bool use_direct_io_;
        int fd_;
        uint64_t filesize_;
        size_t logical_sector_size_;
#ifdef SKG_FALLOCATE_PRESENT
        bool allow_fallocate_;
        bool fallocate_with_keep_size_;
#endif

    public:
        explicit PosixWritableFile(const std::string &fname, int fd,
                                   const EnvOptions &options);

        virtual ~PosixWritableFile();

        // Need to implement this so the file is truncated correctly
        // with direct I/O
        virtual Status Truncate(uint64_t size) override;

        virtual Status Close() override;

        virtual Status Append(const Slice &data) override;

        virtual Status PositionedAppend(const Slice &data, uint64_t offset) override;

        virtual Status Flush() override;

        virtual Status Sync() override;

        virtual Status Fsync() override;

        virtual bool IsSyncThreadSafe() const override;

        virtual bool use_direct_io() const override { return use_direct_io_; }

        virtual void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override;

        virtual uint64_t GetFileSize() override;

        virtual Status InvalidateCache(size_t offset, size_t length) override;

        virtual size_t GetRequiredBufferAlignment() const override {
            return logical_sector_size_;
        }

#ifdef SKG_FALLOCATE_PRESENT
        virtual Status Allocate(uint64_t offset, uint64_t len) override;
#endif
#ifdef ROCKSDB_RANGESYNC_PRESENT
        virtual Status RangeSync(uint64_t offset, uint64_t nbytes) override;
#endif
#ifdef OS_LINUX
        virtual size_t GetUniqueId(char *id, size_t max_size) const override;
#endif
    };

    class CWritableFile: public WritableFile {
    protected:
        const std::string filename_;
        FILE *fd_;
        uint64_t filesize_;

    public:
        explicit CWritableFile(const std::string &fname, FILE *fd,
                               const EnvOptions &options);

        ~CWritableFile() override;

        // Need to implement this so the file is truncated correctly
        // with direct I/O
        Status Truncate(uint64_t size) override;

        Status Close() override;

        Status Append(const Slice &data) override;

        Status Flush() override;

        Status Sync() override;

        Status Fsync() override;

        uint64_t GetFileSize() override;

#ifdef OS_LINUX
    size_t GetUniqueId(char *id, size_t max_size) const override;
#endif
    };

    // mmap() based random-access
    class PosixMmapReadableFile : public RandomAccessFile {
    private:
        int fd_;
        std::string filename_;
        void *mmapped_region_;
        size_t length_;

    public:
        PosixMmapReadableFile(const int fd, const std::string &fname, void *base,
                              size_t length, const EnvOptions &options);

        ~PosixMmapReadableFile() override;

        Status Read(uint64_t offset, size_t n, Slice *result,
                    char *scratch) const override;

        Status InvalidateCache(size_t offset, size_t length) override;
    };

    class PosixMmapFile : public WritableFile {
    private:
        std::string filename_;
        int fd_;
        size_t page_size_;
        size_t map_size_;       // How much extra memory to map at a time
        char *base_;            // The mapped region
        char *limit_;           // Limit of the mapped region
        char *dst_;             // Where to write next  (in range [base_,limit_])
        char *last_sync_;       // Where have we synced up to
        uint64_t file_offset_;  // Offset of base_ in file
#ifdef SKG_FALLOCATE_PRESENT
        bool allow_fallocate_;  // If false, fallocate calls are bypassed
      bool fallocate_with_keep_size_;
#endif

        // Roundup x to a multiple of y
        static size_t Roundup(size_t x, size_t y) { return ((x + y - 1) / y) * y; }

        size_t TruncateToPageBoundary(size_t s) {
            s -= (s & (page_size_ - 1));
            assert((s % page_size_) == 0);
            return s;
        }

        Status MapNewRegion();

        Status UnmapCurrentRegion();

        Status Msync();

    public:
        PosixMmapFile(const std::string &fname, int fd, size_t page_size,
                      const EnvOptions &options);

        ~PosixMmapFile() override;

        // Means Close() will properly take care of truncate
        // and it does not need any additional information
        Status Truncate(uint64_t /*size*/) override { return Status::OK(); }

        Status Close() override;

        Status Append(const Slice &data) override;

        Status Flush() override;

        Status Sync() override;

        Status Fsync() override;

        uint64_t GetFileSize() override;

        Status InvalidateCache(size_t offset, size_t length) override;

#ifdef SKG_FALLOCATE_PRESENT
        virtual Status Allocate(uint64_t offset, uint64_t len) override;
#endif
    };

    class PosixRandomRWFile : public RandomRWFile {
    public:
        explicit PosixRandomRWFile(const std::string &fname, int fd,
                                   const EnvOptions &options);

        ~PosixRandomRWFile() override;

        Status Write(uint64_t offset, const Slice &data) override;

        Status Read(uint64_t offset, size_t n, Slice *result,
                    char *scratch) const override;

        Status Flush() override;

        Status Sync() override;

        Status Fsync() override;

        Status Close() override;

    private:
        const std::string filename_;
        int fd_;
    };
}
