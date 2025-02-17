#include "env.h"

#include <thread>

namespace skg {
    Env::~Env() {
    }

    uint64_t Env::GetThreadID() const {
        std::hash<std::thread::id> hasher;
        return hasher(std::this_thread::get_id());
    }

    Status Env::ReuseWritableFile(const std::string &fname,
                                  const std::string &old_fname,
                                  unique_ptr<WritableFile> *result,
                                  const EnvOptions &options) {
        Status s = RenameFile(old_fname, fname);
        if (!s.ok()) {
            return s;
        }
        return NewWritableFile(fname, result, options);
    }

    Status Env::GetChildrenFileAttributes(const std::string &dir,
                                          std::vector<FileAttributes> *result) {
        assert(result != nullptr);
        std::vector<std::string> child_fnames;
        Status s = GetChildren(dir, &child_fnames);
        if (!s.ok()) {
            return s;
        }
        result->resize(child_fnames.size());
        size_t result_size = 0;
        for (size_t i = 0; i < child_fnames.size(); ++i) {
            const std::string path = dir + "/" + child_fnames[i];
            if (!(s = GetFileSize(path, &(*result)[result_size].size_bytes)).ok()) {
                if (FileExists(path).IsFileNotFound()) {
                    // The file may have been deleted since we listed the directory
                    continue;
                }
                return s;
            }
            (*result)[result_size].name = std::move(child_fnames[i]);
            result_size++;
        }
        result->resize(result_size);
        return Status::OK();
    }

    FileLock::~FileLock() {
    }

    SequentialFile::~SequentialFile() = default;

    RandomAccessFile::~RandomAccessFile() = default;

    WritableFile::~WritableFile() = default;

    Status WriteStringToFile(Env *env, const Slice &data, const std::string &fname,
                             bool should_sync) {
        unique_ptr<WritableFile> file;
        EnvOptions soptions;
        Status s = env->NewWritableFile(fname, &file, soptions);
        if (!s.ok()) {
            return s;
        }
        s = file->Append(data);
        if (s.ok() && should_sync) {
            s = file->Sync();
        }
        if (!s.ok()) {
            env->DeleteFile(fname);
        }
        return s;
    }

    Status ReadFileToString(Env *env, const std::string &fname, std::string *data) {
        EnvOptions soptions;
        data->clear();
        unique_ptr<SequentialFile> file;
        Status s = env->NewSequentialFile(fname, &file, soptions);
        if (!s.ok()) {
            return s;
        }
        static const int kBufferSize = 8192;
        char *space = new char[kBufferSize];
        while (true) {
            Slice fragment;
            s = file->Read(kBufferSize, &fragment, space);
            if (!s.ok()) {
                break;
            }
            data->append(fragment.data(), fragment.size());
            if (fragment.empty()) {
                break;
            }
        }
        delete[] space;
        return s;
    }

    EnvWrapper::~EnvWrapper() {
    }
}
