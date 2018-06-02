/**
 * @file   trace.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 University of California, Los Angeles and Intel Corporation
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
 * Trace Macros if TILEDB_TRACE is defined
 */

#ifndef __TDB_TRACE_H__
#define __TDB_TRACE_H__

#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef TILEDB_TRACE
#  define TRACE_FN std::cerr << "Trace - Function:" << __func__ << " File:" << __FILE__ << ":" << __LINE__ << " tid=" << syscall(SYS_gettid) << std::endl << std::flush
#  define TRACE_FN_ARG(X) std::cerr << "Trace - Function:" << __func__ <<  " File:" << __FILE__ << ":" << __LINE__ << " " << X << " tid=" << syscall(SYS_gettid) << std::endl << std::flush
#else
#  define TRACE_FN
#  define TRACE_FN_ARG(X)
#endif

#endif /*__TDB_TRACE_H__*/
