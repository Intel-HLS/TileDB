/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 MIT and Intel Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */


#ifndef TILEDB_TESTS_H
#define TILEDB_TESTS_H

#include <assert.h>
#include <cstdio>
#include <cstdlib>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>

#define DATASETNAME 						"Int32Array"
#define DATASETNAME_COMPRESSED	"Int32Array_Compressed"
#define RANK 										2
#define ONE_MILLION 						1000000

#define GETTIME(t) gettimeofday(&t, NULL)
#define DIFF_TIME_SECS(b,e) (float)((e.tv_sec - b.tv_sec)*1000000 + (e.tv_usec - b.tv_usec))/1000000

#define FAIL -1
#define	FILENAMESIZE 10240

#endif // TILEDB_TESTS_H
