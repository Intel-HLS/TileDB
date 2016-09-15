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
 * Updates an existing TileDB sparse array created with the AIS vessel traffic
 * data. The update values are taken from another AIS month data file. How
 * these data files are created from raw AIS data are explained in detail in the
 * "datagen" directory README and scripts.
 */

#include "c_api.h"
#include <fstream>
#include <iostream>
#include <vector>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <tiledb_tests.h>
#include <map>

using namespace std;

int64_t *buffer_SOG;
int64_t *buffer_COG;
int64_t *buffer_Heading;
int64_t *buffer_ROT;
int64_t *buffer_Status;
int64_t *buffer_VoyageID;
int64_t *buffer_MMSI;
uint64_t *buffer_coords;

void freeall();

size_t lineCount(char *filename) {
	ifstream aFile(filename);
	size_t lines_count = 0;
	string line;
  while (getline(aFile , line)) ++lines_count;
	aFile.close();
	return lines_count;
}

size_t fileToBuffer(char *filename) {

	size_t lines = lineCount(filename);
	//cout << "Reading " << lines << " from " << filename;
	long lineindex = 0L;
	buffer_SOG = new int64_t [lines];
	buffer_COG = new int64_t [lines];
	buffer_Heading = new int64_t [lines];
	buffer_ROT = new int64_t [lines];
	buffer_Status = new int64_t [lines];
	buffer_VoyageID = new int64_t [lines];
	buffer_MMSI = new int64_t [lines];
	buffer_coords = new uint64_t [2*lines];

	long coord_index = 0L;

	FILE *fp = fopen(filename, "r");
	if (fp==NULL) {
		cerr << "error opening file: " << filename << "\n";
		exit(EXIT_FAILURE);
	}
	uint64_t x,y;
	int64_t a,b,c,d,e,f,g;
	while (fscanf(fp,"%ld %ld %ld %ld %ld %ld %ld %ld %ld", &x,&y,&a,&b,&c,&d,&e,&f,&g)!=EOF) {
		buffer_coords[coord_index++] = x;
		buffer_coords[coord_index++] = y;
		buffer_SOG[lineindex] = a;
		buffer_COG[lineindex] = b;
		buffer_Heading[lineindex] = c;
		buffer_ROT[lineindex] = d;
		buffer_Status[lineindex] = e;
		buffer_VoyageID[lineindex] = f;
		buffer_MMSI[lineindex] = g;
		++lineindex;
	}
	fclose(fp);
	return lines;
}

int main(int argc, char **argv) {

  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

	if (argc < 6) {
		cout << "Usage " << argv[0] << " arrayname datadir nfrags update-month srand-key\n";
		exit(EXIT_FAILURE);
	}

	char *arrayname = new char [10240];
	strcpy(arrayname, argv[1]);
	char *datadir = new char [10240];
	strcpy(datadir, argv[2]);
	const int nfrags = atoi(argv[3]);
	const int update_month = atoi(argv[4]);
	const int srand_key = atoi(argv[5]);
	const int cells_per_frag = 1000;

  struct timeval start, end;

  /* Initialize the array in WRITE mode. */
  TileDB_Array* tiledb_array;

	float rt = 0.0, wt = 0.0, initt = 0.0, ft = 0.0;
  size_t linecount = 0L;
	char filename[10240];
	sprintf(filename, "%s/sorted_tsv_deduped_%02d-Broadcast-2009", datadir, update_month);
	GETTIME(start);
	linecount = fileToBuffer(filename);
	GETTIME(end);
	rt += DIFF_TIME_SECS(start, end);
	cout << "File: " << filename << " read in " << DIFF_TIME_SECS(start, end) << " secs. Done\n";

	int l;
	int64_t *buffer_SOG_w = new int64_t [cells_per_frag];
	int64_t *buffer_COG_w = new int64_t [cells_per_frag];
	int64_t *buffer_Heading_w = new int64_t [cells_per_frag];
	int64_t *buffer_ROT_w = new int64_t [cells_per_frag];
	int64_t *buffer_Status_w = new int64_t [cells_per_frag];
	int64_t *buffer_VoyageID_w = new int64_t [cells_per_frag];
	int64_t *buffer_MMSI_w = new int64_t [cells_per_frag];
	uint64_t *buffer_coords_w = new uint64_t [2*cells_per_frag];

	srand(srand_key);

	for (int f = 0; f < nfrags; f++) {
		std::map<int64_t, int64_t> myMap;
		std::map<int64_t, int64_t>::iterator it; 
		myMap.clear();

		GETTIME(start);
		tiledb_array_init(
				tiledb_ctx,
				&tiledb_array,
				arrayname,
				TILEDB_ARRAY_WRITE_UNSORTED,
				NULL,            // No range - entire domain
				NULL,            // No projection - all attributes
				0);              // Meaningless when "attributes" is NULL
		GETTIME(end);
		initt += DIFF_TIME_SECS(start, end);
		for (int i = 0; i < cells_per_frag; ++i) {
			do {
				l = rand() % linecount;
				it = myMap.find(l);
			} while (it != myMap.end());
			myMap[l] = l;
			buffer_SOG_w[i] = buffer_SOG[l];
			buffer_COG_w[i] = buffer_COG[l];
			buffer_Heading_w[i] = buffer_Heading[l];
			buffer_ROT_w[i] = buffer_ROT[l];
			buffer_Status_w[i] = buffer_Status[l];
			buffer_VoyageID_w[i] = buffer_VoyageID[l];
			buffer_MMSI_w[i] = buffer_MMSI[l];
			buffer_coords_w[2*i] = buffer_coords[2*l];
			buffer_coords_w[2*i+1] = buffer_coords[2*l+1];
		}
		const void* buffers[] = { buffer_SOG_w, buffer_COG_w, buffer_Heading_w, buffer_ROT_w,	
			buffer_Status_w, buffer_VoyageID_w, buffer_MMSI_w, buffer_coords_w };
		size_t buffer_sizes[8] = { cells_per_frag*sizeof(int64_t), cells_per_frag*sizeof(int64_t),
			cells_per_frag*sizeof(int64_t), cells_per_frag*sizeof(int64_t),
			cells_per_frag*sizeof(int64_t), cells_per_frag*sizeof(int64_t),
			cells_per_frag*sizeof(int64_t), 2*cells_per_frag*sizeof(uint64_t) };
		/* Write to array. */
		GETTIME(start);
		tiledb_array_write(tiledb_array, buffers, buffer_sizes);
		GETTIME(end);
		wt += DIFF_TIME_SECS(start, end);
		/* Finalize the array. */
		GETTIME(start);
		tiledb_array_finalize(tiledb_array);
		system("sync");
		GETTIME(end);

		ft += DIFF_TIME_SECS(start, end);	
	}

	freeall();

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

	printf("%.3f\n", initt+ft+wt);
  return 0;
}

void freeall() {
	delete buffer_SOG;
	delete buffer_COG;
	delete buffer_Heading;
	delete buffer_ROT;
	delete buffer_Status;
	delete buffer_VoyageID;
	delete buffer_MMSI;
	delete buffer_coords;
}
