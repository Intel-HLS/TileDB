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
 * @description: Use this generator script to create random coordinates for a
 * 2D dim0xdim1 array and fill these random positions with random values to be
 * used by the random update scripts in TileDB, HDF5, Vertica and SciDB
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

int main(int argc, char **argv) {

	if (argc < 5) {
		cout << "Usage: " << argv[0] <<
			" filename dim0 dim1 ncells srandkey\n";
		exit(EXIT_FAILURE);
	}

	char filename[10240];
	strcpy(filename, argv[1]);
	const int64_t dim0 = atoll(argv[2]);
	const int64_t dim1 = atoll(argv[3]);
	const int ncells = atoi(argv[4]);
	const int srand_key = atoi(argv[5]);

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
			v = rand();
			std::ostringstream mystream;
			mystream << r << "," << c;
			it = myMap.find(mystream.str());
		} while (it!=myMap.end());
		mystream << r << "," << c;
		of << r << " " << c << " " << v << "\n";
		myMap[mystream.str()] = v;
	}

	of.close();
	return EXIT_SUCCESS;
}
