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
 * @description: Use this generator script to create random coordinates between
 * dim0 and dim1. The space-separated file is read by TileDB, HDF5, Vertica and
 * SciDB scripts for random reads. Also, for each cell, we calculate the Tile ID
 * or the chunk id to enable TileDB, HDF5 or Vertica can exploit spatial
 * locality of random reads within the tiles.
 */

#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <vector>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <map>
#include <sstream>
#include <cstdlib>

using namespace std;

int gettileid(int64_t r, int64_t c, int64_t dim0, int64_t dim1, int64_t chunkdim0, int64_t chunkdim1) {
  int64_t tile_coords[2];
  tile_coords[0] = r / chunkdim0; 
  tile_coords[1] = c / chunkdim1;
  int ntiles = dim1/chunkdim1;
  return tile_coords[0] * ntiles + tile_coords[1];
}

int main(int argc, char **argv) {

	if (argc < 8) {
		cout << "Usage: " << argv[0] <<
			" filename dim0 dim1 ncells srandkey chunkdim0 chunkdim1\n";
		exit(EXIT_FAILURE);
	}

	char filename[10240];
	strcpy(filename, argv[1]);
	const int64_t dim0 = atoll(argv[2]);
	const int64_t dim1 = atoll(argv[3]);
	const int ncells = atoi(argv[4]);
	const int srand_key = atoi(argv[5]);
	const int64_t chunkdim0 = atoll(argv[6]);
	const int64_t chunkdim1 = atoll(argv[7]);

	srand(srand_key);

	std::map<std::string, int> myMap;
	std::map<std::string, int>::iterator it;
	myMap.clear();

	int r, c, v;

	ofstream of(filename);
	for (int j = 0; j < ncells; ++j) {
		std::ostringstream mystream;
		do {
			r = rand() % dim0;
			c = rand() % dim1;
			v = rand() * (-1);
			std::ostringstream mystream;
			mystream << r << "," << c;
			it = myMap.find(mystream.str());
		} while (it!=myMap.end());
		mystream << r << "," << c;
		int tileid = gettileid(r,c,dim0,dim1,chunkdim0,chunkdim1);
		of << tileid << " " << r << " " << c << "\n";
		myMap[mystream.str()] = v;
	}

	of.close();
	return EXIT_SUCCESS;
}
