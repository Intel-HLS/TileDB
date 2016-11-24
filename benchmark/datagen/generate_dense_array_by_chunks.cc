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
 * values i*N+j in cell (i,j). The script creates separate binary files for
 * each chunk and stores them to the given directory ordered by tile id.
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
		cout << "Usage: " << argv[0] << " dim1 dim2 chunkdim1 chunkdim2 chunkdir\n";
		exit(EXIT_FAILURE);
	}

	const int dim1 = atoi(argv[1]);
	const int dim2 = atoi(argv[2]);
	const int chunkdim1 = atoi(argv[3]);
	const int chunkdim2 = atoi(argv[4]);

	long blockcount = (dim1/chunkdim1)*(dim2/chunkdim2);
	cout << blockcount << "\n";
	int *fd = new int [blockcount];
	char filename[1024];
	for (int i = 0; i < blockcount; ++i) {
		sprintf(filename, "%s/chunk%d.bin", argv[5], i);
		fd[i] = open(filename, O_WRONLY|O_CREAT);
		if (fd[i] != -1) {
			cout << filename << " opened\n";
		} else {
			cout << "Unable to open " << filename << "\n";
			exit(EXIT_FAILURE);
		}
	}
	
	int buffer_size = chunkdim1 * chunkdim2 * sizeof(int);
	int *buffer = new int [chunkdim1 * chunkdim2];
	int block = 0;

	for (int i = 0; i < dim1; i+=chunkdim1) {
		for (int j = 0; j < dim2; j+=chunkdim2) {
			cout << i << "," << j << "\n";
			for (int k = 0; k < chunkdim1; ++k) {
				for (int l = 0; l < chunkdim2; ++l) {
					buffer[k*chunkdim2+l] = (i+k)*dim2+(j+l);
				}
			}
			if (write(fd[block], (void*) buffer, buffer_size) == -1) {
				cerr << "Error occurred while writing block: " << block << "\n";
				exit(EXIT_FAILURE);
			}
			cout << "54\n";
			close(fd[block]);
			cout << argv[5] << "/chunk" << block << ".bin written and closed\n";
			block++;
			cout << "block: " << block << "\n";
		}
	}
	
  return 0;
}
