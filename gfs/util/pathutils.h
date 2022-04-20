#include <stdio.h>
#include <libgen.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifdef ENV_MAC
// for macro PATH_MAX
#include <sys/syslimits.h>
#endif
#include <cassert>
#include <string>
#include <iostream>

#include "status.h"
#include "fmt/format.h"

#ifndef DEF_GRAPHCHI_PATH_UTILS
#define DEF_GRAPHCHI_PATH_UTILS

class PathUtils {
public:
    static
    std::string os_path_basename(const std::string &fn) {
        const size_t alloc_sz = fn.size() + 1;
        char *dup = new char[alloc_sz];
        memset(dup, 0, alloc_sz);
        memcpy(dup, fn.c_str(), fn.size());
        std::string basefn(basename(dup));
        delete[] dup;
        return basefn;
    }

    // TODO duplicated with os_path_basename
    static
    std::string get_filename(std::string arg) {
        size_t a = arg.find_last_of('/');
        if (a != arg.npos) {
            std::string f = arg.substr(a + 1);
            return f;
        } else {
            assert(false);
            return "n/a";
        }
    }

    static
    std::string os_path_dirname(const std::string &fn) {
        const size_t alloc_sz = fn.size() + 1;
        char *dup = new char[alloc_sz];
        memset(dup, 0, alloc_sz);
        memcpy(dup, fn.c_str(), fn.size());
        std::string basedir(dirname(dup));
        delete[] dup;
        return basedir;
    }

    // TODO duplicated with os_path_dirname
    static
    std::string get_dirname(std::string arg) {
        size_t a = arg.find_last_of('/');
        if (a != arg.npos) {
            std::string dir = arg.substr(0, a);
            return dir;
        } else {
            assert(false);
            return "n/a";
        }
    }

    static
    bool FileExists(const std::string &sname) {
        int res = access(sname.c_str(), F_OK);
        if (res == 0) {
            return true;
        } else {
            return false;
        }
    }

    static
    bool DirExists(const std::string& dname) {
        struct stat statbuf;
        if (stat(dname.c_str(), &statbuf) == 0) {
            return S_ISDIR(statbuf.st_mode);
        }
        return false; // stat() failed return false
    }

    // http://www.linuxquestions.org/questions/programming-9/c-list-files-in-directory-379323/
    static
    size_t getsize(const std::string &filename) {
        struct stat statbuf;
        if (stat(filename.c_str(), &statbuf) != 0) {
            fprintf(stderr, "File `%s` not exists. err: %s\n", filename.c_str(), strerror(errno));
            assert(false);
        }
        return static_cast<size_t>(statbuf.st_size);
    }

    /**
     * takes a given directory and returns the contents of the directory in an array
     * http://www.linuxquestions.org/questions/programming-9/c-list-files-in-directory-379323/
     * @param dir
     * @param files
     * @return
     */
    static
    int listdir(const std::string &dir, std::vector<std::string> &files) {
        DIR *dp;
        if((dp = opendir(dir.c_str())) == nullptr) {
            fprintf(stderr, "Error(%i) opening %s\n", errno, dir.c_str());
            return errno;
        }

        struct dirent *dirp;
        while ((dirp = readdir(dp)) != nullptr) {
            // exclude '.' '..' from result
            if (strncmp(dirp->d_name, ".", PATH_MAX) != 0
                && strncmp(dirp->d_name, "..", PATH_MAX) != 0) {
                files.push_back(std::string(dirp->d_name));
            }
        }
        closedir(dp);
        return 0;
    }

    static
    skg::Status CreateDirIfMissing(const std::string &dir) {
        skg::Status s;
        char tmp[PATH_MAX];
        snprintf(tmp, sizeof(tmp), "%s", dir.c_str());
        size_t len = strnlen(tmp, PATH_MAX);
        if(tmp[len - 1] == '/')
            tmp[len - 1] = '\0';

        // 0755 权限, 用户, 用户组可读可写, 其他人可读可进入
        const int default_mode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH;
        char *p = nullptr;
        for(p = tmp + 1; *p; p++) {
            if (*p == '/') {
                *p = '\0';
                if (mkdir(tmp, default_mode) != 0) {
                    if (errno != EEXIST) {
                        s = skg::Status::IOError(fmt::format(
                                "while mkdir if missing, {}. err: {}", tmp, strerror(errno)));
                        return s;
                    } else if (!DirExists(tmp)) {
                        // check that name is actually a directory
                        s = skg::Status::IOError(fmt::format("`{}' exists but is not a directory", tmp));
                        return s;
                    }
                }
                *p = '/';
            }
        }
        if (mkdir(tmp, default_mode) != 0) {
            if (errno != EEXIST) {
                s = skg::Status::IOError(fmt::format(
                        "while mkdir if missing, {}. err: {}", tmp, strerror(errno)));
                return s;
            } else if (!DirExists(tmp)) {
                // check that name is actually a directory
                s = skg::Status::IOError(fmt::format("`{}' exists but is not a directory", tmp));
                return s;
            }
        }
        return s;
    }


    static
    skg::Status CreateFile(const std::string &fname) {
        int f = open(fname.c_str(), O_WRONLY | O_CREAT, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
        if (f < 0) {
            return skg::Status::IOError(fmt::format("Can NOT create file: {}, error: {}",
                                                    fname, strerror(errno)));
        }
        close(f);
        return skg::Status::OK();
    }

    static
    skg::Status CreateFileIfMissing(const std::string &fname) {
        if (!FileExists(fname)) {
            return CreateFile(fname);
        } else {
            return skg::Status::OK();
        }
    }

    static
    inline
    skg::Status TruncateFile(const std::string &fname, off_t length) {
        const int iRet = truncate(fname.c_str(), length);
        if (iRet == 0) {
            return skg::Status::OK();
        } else {
            return skg::Status::IOError(fmt::format("Can NOT truncate file: {}, error: {}",
                                                    fname, strerror(errno)));
        }
    }

    static
    inline
    skg::Status RemoveFile(const std::string &fname) {
        struct stat buf;
        if (lstat(fname.c_str(), &buf) < 0) {
            return skg::Status::IOError(fmt::format("Fail to get stat of {}", fname));
        }
        // 如果是目录, 先删除目录下的所有文件
        if (S_ISDIR(buf.st_mode)) {
            std::vector<std::string> files;
            PathUtils::listdir(fname, files);
            skg::Status s;
            for (const auto &file: files) {
                const std::string filename = fmt::format("{}/{}", fname, file);
                s = PathUtils::RemoveFile(filename);
                if (!s.ok()) { // 删除错误
                    return s;
                }
            }
        }
        const int iRet = remove(fname.c_str());
        if (iRet == 0) {
            return skg::Status::OK();
        } else {
            return skg::Status::IOError(fmt::format("Can NOT remove file: {}", fname));
        }
    }

    static
    inline
    skg::Status RenameFile(const std::string &oldfile, const std::string &newfile) {
        const int iRet = rename(oldfile.c_str(), newfile.c_str());
        if (iRet < 0) {
            return skg::Status::IOError(fmt::format("Fail rename file: {} -> {}, error: {}",
                                                    oldfile, newfile,
                                                    strerror(errno)));
        } else {
            return skg::Status::OK();
        }
    }
};

#endif
