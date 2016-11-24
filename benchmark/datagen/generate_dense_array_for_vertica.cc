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
 * @description: Use this generator script to create a 2D MxN array filled with
 * values i*N+j in cell (i,j). The script creates one monolithic CSV file for
 * Vertica to create a dense array with or without tileid
 */


#include <iostream>
#include <fstream>
#include <time.h>
#include <sys/time.h>
#include <cstdlib>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>

using namespace std;

int main(int argc, char **argv) {

	if (argc < 6) {
		cout << "Usage: " << argv[0] << " dim1 dim2 chunkdim1 chunkdim2 withtileid\n";
		exit(EXIT_FAILURE);
	}

	const int dim1 = atoi(argv[1]);
	const int dim2 = atoi(argv[2]);
	const int chunkdim1 = atoi(argv[3]);
	const int chunkdim2 = atoi(argv[4]);
	const int withtileid  = atoi(argv[5]);

	long blockcount = (dim1/chunkdim1)*(dim2/chunkdim2);
	cout << blockcount << "\n";
	char filename[1024];
	sprintf(filename, "./dense_%dx%d_%dx%d", dim1,dim2,chunkdim1,chunkdim2);
	if (withtileid==1) {
		sprintf(filename, "%s_withtileid.csv", filename);
	} else {
		sprintf(filename, "%s.csv", filename);
	}
	ofstream file(filename);
	if (!file) {
		cout << "Error: Unable to write file " << filename << "\n";
		exit(EXIT_FAILURE);
	}
	
	int *buffer = new int [chunkdim1 * chunkdim2];
	int block = 0;

	for (int i = 0; i < dim1; i+=chunkdim1) {
		for (int j = 0; j < dim2; j+=chunkdim2) {
			for (int k = 0; k < chunkdim1; ++k) {
				for (int l = 0; l < chunkdim2; ++l) {
					buffer[k*chunkdim2+l] = (i+k)*dim2+(j+l);
					if (withtileid==1)
						file << i+k << " " << j+l << " " << block << " " << buffer[k*chunkdim2+l] << "\n";
					else
						file << i+k << " " << j+l << " " << buffer[k*chunkdim2+l] << "\n";
				}
			}
			block++;
			cout << "block: " << block << "\n";
		}
	}
	file.close();
	
  return 0;
}
