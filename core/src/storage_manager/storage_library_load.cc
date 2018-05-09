/**
 * @file storage_library_load.cc
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
 * Dynamic load of libraries.
 */

#define LOAD_LIBRARY_IMPL
#include "cloud_storage_prototypes.h"
#include <assert.h>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <mutex>

bool loaded = false;
std::mutex loading;

#define BIND_SYMBOL(X, Y, Z)  X = Z dlsym(handle, Y); if (!X) std::cerr << Y << " HDFS Symbol not found"<< std::endl << std::flush; assert(X && "HDFS Symbol not found")

void load_hdfs_library() {
  if (!loaded) {
    loading.lock();
    if (!loaded) {
      void *handle = dlopen("libhdfs.so", RTLD_LAZY);
      if (!handle) { // Try again
	handle = dlopen("libhdfs.dylib", RTLD_LAZY);
      }
      assert(handle && "No support for HDFS; libhdfs could not be loaded");
      
      BIND_SYMBOL(hdfsNewBuilder, "hdfsNewBuilder", (hdfsBuilder* (*)()));
      BIND_SYMBOL(hdfsBuilderSetForceNewInstance, "hdfsBuilderSetForceNewInstance", (void (*)(hdfsBuilder*)));
      BIND_SYMBOL(hdfsBuilderSetNameNode, "hdfsBuilderSetNameNode", (void (*)(hdfsBuilder*, const char*)));
      BIND_SYMBOL(hdfsBuilderSetNameNodePort, "hdfsBuilderSetNameNodePort", (void (*)(hdfsBuilder*, tPort)));
      BIND_SYMBOL(hdfsBuilderConfSetStr, "hdfsBuilderConfSetStr", (int (*)(hdfsBuilder*, const char*, const char*)));
      BIND_SYMBOL(hdfsBuilderConnect, "hdfsBuilderConnect", (hdfs_internal* (*)(hdfsBuilder*)));
      BIND_SYMBOL(hdfsDisconnect, "hdfsDisconnect", (int (*)(hdfsFS)));
      
      BIND_SYMBOL(hdfsGetWorkingDirectory, "hdfsGetWorkingDirectory",(char* (*)(hdfsFS, char*, size_t)));
      BIND_SYMBOL(hdfsSetWorkingDirectory, "hdfsSetWorkingDirectory", (int (*)(hdfsFS, const char*)));
      BIND_SYMBOL(hdfsCreateDirectory, "hdfsCreateDirectory", (int (*)(hdfsFS, const char*)));

      BIND_SYMBOL(hdfsGetPathInfo, "hdfsGetPathInfo", (hdfsFileInfo* (*)(hdfsFS, const char*)));
      BIND_SYMBOL(hdfsListDirectory, "hdfsListDirectory", (hdfsFileInfo* (*)(hdfsFS, const char*, int*)));
      BIND_SYMBOL(hdfsFreeFileInfo, "hdfsFreeFileInfo", (void (*)(hdfsFileInfo*, int)));
      BIND_SYMBOL(hdfsExists, "hdfsExists", (int (*)(hdfsFS, const char*)));

      BIND_SYMBOL(hdfsOpenFile, "hdfsOpenFile", (hdfsFile_internal* (*)(hdfsFS, const char*, int, int, short int, tSize)));
      BIND_SYMBOL(hdfsCloseFile, "hdfsCloseFile", (int (*)(hdfsFS, hdfsFile)));
      BIND_SYMBOL(hdfsSeek, "hdfsSeek", (int (*)(hdfsFS, hdfsFile, tOffset)));
      BIND_SYMBOL(hdfsRead, "hdfsRead", (tSize (*)(hdfsFS, hdfsFile, void*, tSize)));
      BIND_SYMBOL(hdfsWrite, "hdfsWrite", (tSize (*)(hdfsFS, hdfsFile, const void*, tSize)));
      
      BIND_SYMBOL(hdfsFlush, "hdfsFlush", (int (*)(hdfsFS, hdfsFile)));
      BIND_SYMBOL(hdfsHFlush, "hdfsHFlush", (int (*)(hdfsFS, hdfsFile)));
      BIND_SYMBOL(hdfsHSync, "hdfsHSync", (int (*)(hdfsFS, hdfsFile)));

      BIND_SYMBOL(hdfsCopy, "hdfsCopy", (int (*)(hdfsFS, const char*, hdfsFS, const char*)));
      BIND_SYMBOL(hdfsMove, "hdfsMove", (int (*)(hdfsFS, const char*, hdfsFS, const char*)));
      BIND_SYMBOL(hdfsDelete, "hdfsDelete", (int (*)(hdfsFS, const char*, int)));
      BIND_SYMBOL(hdfsRename, "hdfsRename", (int (*)(hdfsFS, const char*, const char*))); 
    }
     
    loading.unlock();
  } 
}
