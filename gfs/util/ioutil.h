
/**
 * @file
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 
 *
 * @section DESCRIPTION
 *
 * I/O Utils.
 */
#ifndef DEF_IOUTIL_HPP
#define DEF_IOUTIL_HPP

#include <unistd.h>
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <cstring>
#include <zlib.h>
#include <fcntl.h>

#include "status.h"

#include "skglogger.h"

#define SKG_LIMIT_DEF_MAX_READ_BYTES
#ifdef SKG_LIMIT_DEF_MAX_READ_BYTES
static const size_t MAX_BYTES_PER_READ = 1024UL * 1024UL * 1024UL; // 1 GB
#endif
// Reads given number of bytes to a buffer
// TODO return error by skg::Status
template <typename T>
skg::Status preada(int f, T * tbuf, size_t nbytes, size_t off) {
    size_t nread = 0;
    char * buf = (char*)tbuf;
    while(nread<nbytes) {
#ifdef SKG_LIMIT_DEF_MAX_READ_BYTES
        ssize_t a = pread(f, buf, std::min(nbytes - nread, MAX_BYTES_PER_READ), off + nread);
#else
        ssize_t a = pread(f, buf, nbytes - nread, off + nread);
#endif
        if (a == (-1)) {
            const std::string err = fmt::format("Could not read, error: `{}'. f: {} nbytes: {} off: {}",
                    strerror(errno), f, nbytes, off);
            SKG_LOG_ERROR("{}", err);
            assert(a != -1);
            return skg::Status::IOError(err);
        }
        assert(a>0);
        buf += a;
        nread += a;
    }
    assert(nread <= nbytes);
    return skg::Status::OK();
}

template <typename T>
void preada_trunc(int f, T * tbuf, size_t nbytes, size_t off) {
    size_t nread = 0;
    char * buf = (char*)tbuf;
    while(nread<nbytes) {
        size_t a = pread(f, buf, nbytes-nread, off+nread);
        if (a == 0) {
            // set rest to 0
     //       std::cout << "WARNING: file was not long enough - filled with zeros. " << std::endl;
            memset(buf, 0, nbytes-nread);
            return;
        }
        buf += a;
        nread += a;
    }

} 

template <typename T>
size_t readfull(int f, T ** buf) {
     off_t sz = lseek(f, 0, SEEK_END);
     lseek(f, 0, SEEK_SET);
     *buf = (T*)malloc(sz);
    preada(f, *buf, sz, 0);
    return sz;
}

template <typename T>
skg::Status pwritea(int f, T * tbuf, size_t nbytes, size_t off) {
    size_t nwritten = 0;
    assert(f>0);
    char * buf = (char*)tbuf;
    while(nwritten<nbytes) {
        ssize_t a = pwrite(f, buf, nbytes-nwritten, off+nwritten);
        if (a == -1) {
            const std::string err = fmt::format("f:{} nbytes: {} written: {} off: {} error: {}",
                                                f, nbytes, nwritten, off, strerror(errno));
            SKG_LOG_ERROR("{}", err);
            assert(false);
            return skg::Status::IOError(err);
        }
        assert(a>0);
        buf += a;
        nwritten += a;
    }
    return skg::Status::OK();
}




template <typename T>
void checkarray_filesize(std::string fname, size_t nelements) {
    // Check the vertex file is correct size
    int f = open(fname.c_str(),  O_RDWR | O_CREAT, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
    if (f < 1) {
        SKG_LOG_ERROR("Error initializing the data-file: {} error:", fname, strerror(errno));
    }
    assert(f>0);
    int err = ftruncate(f, nelements * sizeof(T));
    if (err != 0) {
        SKG_LOG_ERROR("Error in adjusting file size: {} to size: {} error: {}",
                      fname, nelements * sizeof(T), strerror(errno));
    }
    assert(err == 0);
    close(f);
}

template <typename T>
skg::Status writea(int f, T * tbuf, size_t nbytes) {
    size_t nwritten = 0;
    char * buf = (char*)tbuf;
    while(nwritten<nbytes) {
        ssize_t a = write(f, buf, nbytes - nwritten);
        assert(a>0);
        if (a == -1) {
            const std::string err = fmt::format("Could not write {} bytes! f: {} error: {}",
                    (nbytes-nwritten), f, strerror(errno));
            SKG_LOG_ERROR("{}", err);
            assert(false);
            return skg::Status::IOError(err);
        }
        buf += a;
        nwritten += a;
    }
    return skg::Status::OK();
}

/*
 * COMPRESSED
 */



template <typename T>
size_t write_compressed(int f, T * tbuf, size_t nbytes) {
#ifndef SKG_DISABLE_COMPRESSION
    unsigned char * buf = (unsigned char*)tbuf;
    int ret;
    unsigned have;
    z_stream strm;
    int CHUNK = (int) std::max((size_t)1024 * 1024, nbytes);
    unsigned char * out = (unsigned char *) malloc(CHUNK);
    lseek(f, 0, SEEK_SET);

    /* allocate deflate state */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = deflateInit(&strm, Z_BEST_SPEED);
    if (ret != Z_OK)
        assert(false);
    
    /* compress until end of file */
    strm.avail_in = (int) nbytes;
    strm.next_in = buf;
    
    int trerr = ftruncate(f, 0);
    assert (trerr == 0);
    size_t totwritten = 0;
    
   /* run deflate() on input until output buffer not full, finish
     compression if all of source has been read in */
    do {
        strm.avail_out = CHUNK;
        strm.next_out = out;
        ret = deflate(&strm, Z_FINISH);    /* no bad return value */
        assert(ret != Z_STREAM_ERROR);  /* state not clobbered */
        have = CHUNK - strm.avail_out;
        if (write(f, out, have) != have) {
            (void)deflateEnd(&strm);
            assert(false);
        }
        totwritten += have;
    } while (strm.avail_out == 0);
    assert(strm.avail_in == 0);     /* all input will be used */
        
    assert(ret == Z_STREAM_END);        /* stream will be complete */
    
    /* clean up and return */
    (void)deflateEnd(&strm);
    free(out);
    return totwritten;
#else
    writea(f, tbuf, nbytes);
    return nbytes;
#endif 

}

/* Zlib-inflated read. Assume tbuf is correctly sized memory block. */
template <typename T>
void read_compressed(int f, T * tbuf, size_t nbytes) {
#ifndef SKG_DISABLE_COMPRESSION
    unsigned char * buf = (unsigned char*)tbuf;
    int ret;
    unsigned have;
    z_stream strm;
    int CHUNK = (int) std::max((size_t)1024 * 1024, nbytes);

    size_t fsize = lseek(f, 0, SEEK_END);
    
    unsigned char * in = (unsigned char *) malloc(fsize);
    lseek(f, 0, SEEK_SET);

    /* allocate inflate state */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;
    ret = inflateInit(&strm);
    if (ret != Z_OK)
        assert(false);
    
    /* decompress until deflate stream ends or end of file */
    do {
        ssize_t a = 0;
        do {
            a = read(f, in + strm.avail_in, fsize - strm.avail_in); //fread(in, 1, CHUNK, source);
            strm.avail_in += (int) a;
            assert(a != (ssize_t)(-1));
        } while (a > 0);
       
        if (strm.avail_in == 0)
            break;
        strm.next_in = in;
        
        /* run inflate() on input until output buffer not full */
        do {
            strm.avail_out = CHUNK;
            strm.next_out = buf;
            ret = inflate(&strm, Z_NO_FLUSH);
            assert(ret != Z_STREAM_ERROR);  /* state not clobbered */
            switch (ret) {
                case Z_NEED_DICT:
                    ret = Z_DATA_ERROR;     /* and fall through */
                case Z_DATA_ERROR:
                case Z_MEM_ERROR:
                    assert(false);
            }
            have = CHUNK - strm.avail_out;
            buf += have;
        } while (strm.avail_out == 0);
        
        /* done when inflate() says it's done */
    } while (ret != Z_STREAM_END);
   // std::cout << "Read: " << (buf - (unsigned char*)tbuf) << std::endl;
    /* clean up and return */
    (void)inflateEnd(&strm);
    free(in);
#else
    preada(f, tbuf, nbytes, 0);
#endif
}



#endif


