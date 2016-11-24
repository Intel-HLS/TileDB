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
 *
 * @description: Removes duplicate entries from a sorted AIS Broadcast file.
 * The file contents has the schema lat, long, SOG, COG, Heading, ROT, Status,
 * VoyageID, MMSI. First sort the data using GNU Bash sort command line tool
 * and then run this script to remove the duplicate entries
 */
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <sys/types.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <unistd.h>

using namespace std;

int main(int argc, char **argv) {
	if (argc < 3) {
		cout << "Usage: " << argv[0] << " input-file output-file\n";
		cout << "\tNote that input-file must be sorted "
			   << "on coordinates X,Y (first two columns)\n";
		exit(EXIT_FAILURE);
	}
	char *ifilename = new char [10240];
	strcpy(ifilename, argv[1]);
	char *ofilename = new char [10240];
	strcpy(ofilename, argv[2]);
	
	FILE *fp = fopen(ifilename,"r");
	FILE *ofp = fopen(ofilename, "w");
	if (!fp || !ofp) {
		cerr << "file open error\n";
		exit(EXIT_FAILURE);
	}
	uint64_t x,y;
	int a,b,c,d,e,f,g;
	uint64_t prev_x = 0,prev_y = 0;
	uint64_t lines_written=0L, lines_read=0L;
	while (fscanf(fp,
			"%ld %ld %d %d %d %d %d %d %d", &x,&y,&a,&b,&c,&d,&e,&f,&g)!=EOF) {
		if (!(x==prev_x && y==prev_y)) {
			fprintf(ofp, "%ld %ld %d %d %d %d %d %d %d\n", x,y,a,b,c,d,e,f,g);
			lines_written++;
		}
		prev_x = x;
		prev_y = y;
		lines_read++;
	}
	fclose(fp);
	fclose(ofp);
	cout << "Lines read: " << lines_read << "\n";
	cout << "Lines written: " << lines_written << "\n";
	return EXIT_SUCCESS;
}
