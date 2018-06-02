/**
 * @ cloud_storage_prototypes.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * 
 * @section DESCRIPTION
 *
 * Function pointer definitions for all the Cloud functionality used.
 *
 */

#ifndef __CLOUD_STORAGE_PROTOTYPES_H__
#define  __CLOUD_STORAGE_PROTOTYPES_H__

#include <errno.h> /* for EINTERNAL, etc. */
#include <fcntl.h> /* for O_RDONLY, O_WRONLY */
#include <stdint.h> /* for uint64_t, etc. */
#include <time.h> /* for time_t */

extern int load_hdfs_library();

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_DLL_OK                                                  0
#define TILEDB_DLL_ERR                                                -1
/**@}*/

/** Default error message. */
#define TILEDB_DLL_ERRMSG std::string("[TileDB::DynamicLibaryLoad] Error: ")


#ifdef __cplusplus
extern  "C" {
#endif

#ifdef LOAD_LIBRARY_IMPL
  #define LIBHDFS_EXTERNAL __attribute__((visibility("default")))
#else
  #define LIBHDFS_EXTERNAL extern
#endif

#ifndef O_RDONLY
#define O_RDONLY 1
#endif

#ifndef O_WRONLY
#define O_WRONLY 2
#endif

struct hdfsBuilder;
typedef int32_t   tSize; /// size of data for read/write io ops
typedef time_t    tTime; /// time type in seconds
typedef int64_t   tOffset;/// offset within the file
typedef uint16_t  tPort; /// port
typedef enum tObjectKind {
  kObjectKindFile = 'F',
  kObjectKindDirectory = 'D',
} tObjectKind;

struct hdfs_internal;
typedef struct hdfs_internal* hdfsFS;

struct hdfsFile_internal;
typedef struct hdfsFile_internal* hdfsFile;

typedef struct  {
  tObjectKind mKind;   /* file or directory */
  char *mName;         /* the name of the file */
  tTime mLastMod;      /* the last modification time for the file in seconds */
  tOffset mSize;       /* the size of the file in bytes */
  short mReplication;    /* the count of replicas */
  tOffset mBlockSize;  /* the block size for the file */
  char *mOwner;        /* the owner of the file */
  char *mGroup;        /* the group associated with the file */
  short mPermissions;  /* the permissions associated with the file */
  tTime mLastAccess;    /* the last access time for the file in seconds */
} hdfsFileInfo;

// HDFS Connection configuration
LIBHDFS_EXTERNAL struct hdfsBuilder *(*hdfsNewBuilder)(void);
LIBHDFS_EXTERNAL void (*hdfsBuilderSetForceNewInstance)(struct hdfsBuilder *bld);
LIBHDFS_EXTERNAL void (*hdfsBuilderSetNameNode)(struct hdfsBuilder *bld, const char *nn);
LIBHDFS_EXTERNAL void (*hdfsBuilderSetNameNodePort)(struct hdfsBuilder *bld, tPort port);
LIBHDFS_EXTERNAL int (*hdfsBuilderConfSetStr)(struct hdfsBuilder *bld, const char *key, const char *val);
LIBHDFS_EXTERNAL hdfsFS (*hdfsBuilderConnect)(struct hdfsBuilder *bld);
LIBHDFS_EXTERNAL int (*hdfsDisconnect)(hdfsFS fs);

// Directory related functions
LIBHDFS_EXTERNAL char* (*hdfsGetWorkingDirectory)(hdfsFS fs, char *buffer, size_t bufferSize);
LIBHDFS_EXTERNAL int (*hdfsSetWorkingDirectory)(hdfsFS fs, const char* path);
LIBHDFS_EXTERNAL int (*hdfsCreateDirectory)(hdfsFS fs, const char* path);

// Path related functions
LIBHDFS_EXTERNAL hdfsFileInfo *(*hdfsGetPathInfo)(hdfsFS fs, const char* path);
LIBHDFS_EXTERNAL hdfsFileInfo *(*hdfsListDirectory)(hdfsFS fs, const char* path, int *numEntries);
LIBHDFS_EXTERNAL void (*hdfsFreeFileInfo)(hdfsFileInfo *hdfsFileInfo, int numEntries);
LIBHDFS_EXTERNAL int (*hdfsExists)(hdfsFS fs, const char *path);

// File related functions
LIBHDFS_EXTERNAL hdfsFile (*hdfsOpenFile)(hdfsFS fs, const char* path, int flags, int bufferSize, short replication, tSize blocksize);
LIBHDFS_EXTERNAL int (*hdfsCloseFile)(hdfsFS fs, hdfsFile file);
LIBHDFS_EXTERNAL int (*hdfsSeek)(hdfsFS fs, hdfsFile file, tOffset desiredPos); 

LIBHDFS_EXTERNAL tSize (*hdfsRead)(hdfsFS fs, hdfsFile file, void* buffer, tSize length);
LIBHDFS_EXTERNAL tSize (*hdfsWrite)(hdfsFS fs, hdfsFile file, const void* buffer, tSize length);
  
LIBHDFS_EXTERNAL int (*hdfsFlush)(hdfsFS fs, hdfsFile file);
LIBHDFS_EXTERNAL int (*hdfsHFlush)(hdfsFS fs, hdfsFile file);
LIBHDFS_EXTERNAL int (*hdfsHSync)(hdfsFS fs, hdfsFile file);

LIBHDFS_EXTERNAL int (*hdfsCopy)(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);
LIBHDFS_EXTERNAL int (*hdfsMove)(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);
LIBHDFS_EXTERNAL int (*hdfsDelete)(hdfsFS fs, const char* path, int recursive);
LIBHDFS_EXTERNAL int (*hdfsRename)(hdfsFS fs, const char* oldPath, const char* newPath);

#undef LIBHDFS_EXTERNAL

#ifdef __cplusplus
}
#endif

#endif /*__CLOUD_STORAGE_PROTOTPYES_H__*/
