/**
 * The MIT License
 * 
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * It shows how to consolidate arrays.
 */

#include "c_api.h"
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <tiledb_tests.h>
#include <sys/types.h>
#include <sys/sysinfo.h>

using namespace std;

char *tiledb_arrayname = NULL;

int parseLine(char* line);
int getValue();
int parse_opts(int argc, char** argv);

int main(
	int argc,
	char **argv) {

	parse_opts(argc, argv);

  // Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
	TileDB_Config config = {};
	config.read_method_ = TILEDB_IO_MMAP;
  tiledb_ctx_init(&tiledb_ctx, &config);
	
	struct timeval start, end;
	GETTIME(start);
  // Consolidate the dense array
  tiledb_array_consolidate(tiledb_ctx, tiledb_arrayname);
	GETTIME(end);
	printf("%.3f\n", DIFF_TIME_SECS(start, end));

	long long physMemUsed = getValue();
	printf("mem used: %lld KB\n", physMemUsed);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}

int parseLine(char* line){
	int i = strlen(line);
	while (*line < '0' || *line > '9') line++;
	line[i-3] = '\0';
	i = atoi(line);
	return i;
}

/**
 * Get total memory used from /proc/
 */
int getValue(){ //Note: this value is in KB!
	FILE* file = fopen("/proc/self/status", "r");
	int result = -1;
	char line[128];


	while (fgets(line, 128, file) != NULL){
		if (strncmp(line, "VmRSS:", 6) == 0){
			result = parseLine(line);
			break;
		}
	}
	fclose(file);
	return result;
}

/**
 * Parse command line parameters
 */
int parse_opts(
	int argc,
	char **argv) {

  int c, help = 0;
  opterr = 0;

	while ((c = getopt (argc, argv, "a:h")) != -1) {
		switch (c)
		{
			case 'a':
				{
					tiledb_arrayname = new char [FILENAMESIZE];
					strcpy(tiledb_arrayname, optarg);
					break;
				}
			case 'h':
				{
					help = 1;
					break;
				}
			default:
				abort ();
		}
	}	// end of while

	int error = 0;
	error = (!tiledb_arrayname) ? 1 : error;

	if (error || help) {
		cout << "\n Usage: " << argv[0]
			<< ":\n\n\t-a arrayname\t\tTileDB Array name/directory\n";
		exit(EXIT_FAILURE);
	}
	return EXIT_SUCCESS;
}
