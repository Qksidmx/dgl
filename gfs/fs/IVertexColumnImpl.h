#ifndef STARKNOWLEDGEGRAPHDATABASE_IVERTEXCOLUMNIMPL_H
#define STARKNOWLEDGEGRAPHDATABASE_IVERTEXCOLUMNIMPL_H

#include <stdio.h>
#include <cstdint>
#include <string>
#include <sys/mman.h>

#include "fmt/format.h"

#include "util/types.h"
//#include "fs/skgfs.h"
#include "util/status.h"

#include "IVertexColumn.h"
//#include "util/chifilenames.h"
#include "preprocessing/parse/valparser.hpp"
#include "preprocessing/parse/fileparse/fileparser.hpp"

#include "util/skglogger.h"
#include "util/internal_types.h"
#include "env/env.h"

#ifdef SKG_WITH_ROCKSDB
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/cache.h"
#include "rocksdb/status.h"
#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/slice.h"
#endif SKG_WITH_ROCKSDB


namespace skg {
    class FixedBytesVertexColumn : public IVertexColumn {
    protected:
        std::string m_filename;
        size_t m_mapped_size;
        char *m_mapped_vattrs;
        bool m_modified;
        const size_t m_value_size;

    public:
        FixedBytesVertexColumn(
                const std::string &basefile,
                const std::string &label,
                const ColumnDescriptor &desc)
            : IVertexColumn(desc),
              m_filename(FILENAME::vertex_attr_data(basefile, label, name())),
              m_mapped_size(0), m_mapped_vattrs(nullptr), m_modified(false),
              m_value_size(desc.value_size()) {
            assert(m_value_size != 0);
        }

        ~FixedBytesVertexColumn() override {
            this->Close();
        }

        Status Open() override {
            // 防止多次Open导致内存泄露
            if (m_mapped_vattrs == nullptr) {
                int fd = open(m_filename.c_str(), O_RDWR);
                if (fd < 0) {
                    return Status::IOError(fmt::format("vertex-property file:`{}`, error: {}({})",
                                                       m_filename, strerror(errno), errno));
                }
                m_mapped_size = PathUtils::getsize(m_filename);
                m_mapped_vattrs = static_cast<char *>(
                        mmap(nullptr, m_mapped_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
                close(fd);
                if (m_mapped_vattrs == MAP_FAILED) {
                    return Status::IOError(fmt::format("Can NOT load {}, error: {}({})",
                                                       m_filename, strerror(errno), errno));
                }
                m_modified = false;
            }
            return Status::OK();
        }

        Status Close() override {
            if (m_mapped_vattrs == nullptr || m_mapped_vattrs == MAP_FAILED) {
                return Status::OK();
            }
            Status s = this->Flush();
            if (!s.ok()) { return s; }
            munmap(m_mapped_vattrs, m_mapped_size);
            m_mapped_vattrs = nullptr;
            return Status::OK();
        }

        Status Drop() override {
            Status s = this->Close();
            if (!s.ok()) {
                s = PathUtils::RemoveFile(m_filename);
            }
            return s;
        }

        Status Flush() override {
            Status s;
            if (m_modified) {
                int iRet = msync(m_mapped_vattrs, m_mapped_size, MS_SYNC);
                if (iRet < 0) {
                    s = Status::IOError(fmt::format("msync fail! file: {}", m_filename));
                }
                m_modified = false;
            }
            return s;
        }

        Status EnsureStorage(const vid_t storageCapacity) override {
            Status s;
            // 足够存储
            if (storageCapacity * this->value_size() < m_mapped_size) {
                return s;
            }
            // flush to disk && close handler
            s = this->Close();
            if (!s.ok()) { return s; }
            // extend vertex room to store more
            s = PathUtils::TruncateFile(
                    filename(), storageCapacity  * this->value_size()
            );
            if (!s.ok()) { return s; }
            // re-open handler
            s = this->Open();
            return s;
        }

        Status Get(const vid_t vtx, ResultProperties *properties, size_t offset) const override {
            // debug模式下,边界检查
            assert(vtx < num_vertices());
            if (id() == ColumnDescriptor::ID_VERTICES_BITSET
                || id() == ColumnDescriptor::ID_VERTICES_TAG
                || !properties->is_null(id())) { // 非空, 取属性
                return properties->putBytes(
                        Slice(m_mapped_vattrs + (vtx * m_value_size), m_value_size),
                        offset, id());
            } else {
                return Status::OK();
            }
        }

        Status Set(const vid_t vtx, const Slice &value) override {
            // debug模式下,边界检查
            assert(vtx < num_vertices());
            m_modified = true;
            // 存字符串, 最终需要有'\0'结束符, 先把区域清零 TODO 字符串存取都通过 VarChar 进行存储, 这里可去除覆盖值为 '\0' 的操作
            memset(m_mapped_vattrs + (vtx * m_value_size), 0, m_value_size);
            // 再copy值
            memcpy(m_mapped_vattrs + (vtx * m_value_size), value.data(), std::min(value.size(), m_value_size));
            return Status::OK();
        }

        const char *filename() const override {
            return m_filename.c_str();
        }

        ColumnType vertexColType() const override {
            return ColumnType::FIXED_BYTES;
        }

        size_t value_size() const override {
            return m_value_size;
        }

    private:
        size_t num_vertices() const {
            assert(m_value_size != 0);
            return m_mapped_size / value_size();
        }

    };

    class Int32VertexColumn : public FixedBytesVertexColumn {
    public:
        typedef int32_t ValueType;

        Int32VertexColumn(
                const std::string &basefile,
                const std::string &label, const ColumnDescriptor &desc)
                : FixedBytesVertexColumn(basefile, label, desc) {
        }

        ColumnType vertexColType() const override {
            return ColumnType::INT32;
        }

    };

    class Int64VertexColumn : public FixedBytesVertexColumn {
    public:
        typedef int64_t ValueType;

        Int64VertexColumn(
                const std::string &basefile,
                const std::string &label, const ColumnDescriptor &desc)
                : FixedBytesVertexColumn(basefile, label, desc) {
        }

        ColumnType vertexColType() const override {
            return ColumnType::INT64;
        }

    };

    class Float32VertexColumn : public FixedBytesVertexColumn {
    public:
        typedef float ValueType;

        Float32VertexColumn(
                const std::string &basefile,
                const std::string &label, const ColumnDescriptor &desc)
                : FixedBytesVertexColumn(basefile, label, desc) {
        }

        ColumnType vertexColType() const override {
            return ColumnType::FLOAT;
        }

    };

    class Float64VertexColumn :public FixedBytesVertexColumn {
    public:
        typedef double ValueType;

        Float64VertexColumn(
                const std::string &basefile,
                const std::string &label, const ColumnDescriptor &desc)
                : FixedBytesVertexColumn(basefile, label, desc) {
        }

        ColumnType vertexColType() const override {
            return ColumnType::FLOAT64;
        }
    };

    class TimeVertexColumn: public FixedBytesVertexColumn {
    private:
        std::string m_format;
    public:
        typedef time_t ValueType;

        TimeVertexColumn(
                const std::string &basefile,
                const std::string &label,  const ColumnDescriptor &desc)
                : FixedBytesVertexColumn(basefile, label, desc),
                  m_format(desc.GetTimeFormat()) {
        }

        ColumnType vertexColType() const override {
            return ColumnType::TIME;
        }
    };

    class TagVertexColumn: public FixedBytesVertexColumn {
    public:
        typedef uint8_t ValueType;

        TagVertexColumn(
                const std::string &basefile,
                const std::string &label, const ColumnDescriptor &desc)
                : FixedBytesVertexColumn(basefile, label, desc) {
        }

        ColumnType vertexColType() const override {
            return ColumnType::TAG;
        }
    };

#ifdef SKG_WITH_ROCKSDB
    class VarCharVertexColumn: public IVertexColumn {
    
    public:
        typedef std::string ValueType;

    public:
        std::string m_db_path;
        rocksdb::DB *m_db;
        rocksdb::Options options;
    public:
                     
        VarCharVertexColumn(
                const std::string &db_file,
                const std::string &label,
                const ColumnDescriptor &desc)
                : IVertexColumn(desc), m_db_path(), m_db(nullptr) {
            options.create_if_missing = true;
            m_db_path = FILENAME::vertex_attr_varchar_data(db_file, label, name());
        }
     
        ~VarCharVertexColumn() override {
            this->Close();
        }

        Status Open() override {
            if (m_db == nullptr) {
                rocksdb::Status s = rocksdb::DB::Open(options, m_db_path, &m_db);
                if(s.ok()) {
                    return Status::OK();
                } else {
                    return Status::IOError(fmt::format(
                            "open vertex-col: {}, error: {}",
                            m_db_path, s.ToString()));
                }        
            }
            return Status::OK();
        }
        
        Status Close() override {
            if (m_db!= nullptr){
                delete m_db;
                m_db = nullptr;
            }
            return Status::OK();
        }

        Status Drop() override {
            Status s = this->Close();
            if (!s.ok()) {
                s = Env::Default()->DeleteDir(m_db_path, true);
            }
            return s;
        }

        Status EnsureStorage(const vid_t storageCapacity) override {
            return Status::OK();
        }

        Status Get(const vid_t vtx, ResultProperties *properties, size_t offset) const override {
            assert(m_db != nullptr);
            if (properties->is_null(id())) {
                return Status::OK();
            }
			std::string key = std::to_string(vtx);
			std::string val;
			rocksdb::Status s = m_db->Get(rocksdb::ReadOptions(), key, &val);
            if(s.ok()){
                return properties->putVar(val, offset, id());
            } else {
                return Status::NotExist("vtx not exist!");
            }
        }

		Status Set(const vid_t vtx, const Slice &value) override {
            assert(m_db != nullptr);
            const std::string skey = std::to_string(vtx);
			const std::string val = value.ToString();
			rocksdb::Status s = m_db->Put(rocksdb::WriteOptions(), skey, val);
            if(s.ok()){
                return Status::OK();
            } else{
                return Status::IOError(fmt::format("vtx: {}, set error: {}", vtx, s.ToString()));
            }   
		}

        Status Flush() override {
            assert(m_db != nullptr);
            rocksdb::Status s = m_db->Flush(rocksdb::FlushOptions());
            if (s.ok()) {
                return Status::OK();
            } else {
                return Status::IOError(s.ToString());
            }
        }

        const char *filename() const override{
            return NULL;
        }

        size_t value_size() const override{
            return sizeof(uint32_t);
        }
        
        ColumnType vertexColType() const override {
            return ColumnType::VARCHAR;
        }
    
    };

#endif SKG_WITH_ROCKSDB
}
#endif //STARKNOWLEDGEGRAPHDATABASE_IVERTEXCOLUMNIMPL_H
