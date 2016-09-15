/**
 * @file   tiledb_array_create_sparse.cc
 *
 * @section LICENSE
 *
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
 * @section DESCRIPTION
 *
 * It shows how to create a sparse array.
 */

#include "c_api.h"
#include <tiledb_tests.h>
#include <iostream>
#include <cstdlib>
#include <string.h>
#include <map>
#include <sstream>

using namespace std;

int main(int argc, char **argv) {
	// Initialize context with the default configuration parameters
	TileDB_CTX* tiledb_ctx;
	tiledb_ctx_init(&tiledb_ctx, NULL);

	if (argc < 6) {
		cout << "Usage: " << argv[0] <<
			" arrayname dim0 dim1 ncells srandkey\n";
		exit(EXIT_FAILURE);
	}

	char *arrayname = new char [1024];
	strcpy(arrayname, argv[1]);
	const int64_t dim0 = atoll(argv[2]);
	const int64_t dim1 = atoll(argv[3]);
	const int ncells = atoi(argv[4]);
	const int srand_key = atoi(argv[5]);

	srand(srand_key);

	int dim0_ranges[RANK], dim1_ranges[RANK];
	dim0_ranges[0] = 0;
	dim0_ranges[1] = dim0 - 1;
	dim1_ranges[0] = 0;
	dim1_ranges[1] = dim1 - 1;

	// Prepare parameters for array schema
	const char* attributes[] = { "a1" };  // Three attributes
	const char* dimensions[] = { "d0", "d1" };        // Two dimensions
	int64_t domain[] = 
	{ 
		dim0_ranges[0], dim0_ranges[1],         // d0
		dim1_ranges[0], dim1_ranges[1]          // d1 
	};                
	const int cell_val_num[] = 
	{ 
		1                          // a1
	};
	const int compression[] = 
	{ 
		TILEDB_NO_COMPRESSION,    // a1 
		TILEDB_NO_COMPRESSION     // coordinates
	};
	const int types[] = 
	{ 
		TILEDB_INT32,               // a1 
		TILEDB_INT64                // coordinates
	};

	// Set array schema
	TileDB_ArraySchema array_schema;
	tiledb_array_set_schema( 
			&array_schema,              // Array schema struct 
			arrayname,                 // Array name 
			attributes,                 // Attributes 
			1,                          // Number of attributes 
			1000,                       // Capacity 
			TILEDB_ROW_MAJOR,           // Cell order 
			cell_val_num,               // Number of cell values per attribute  
			compression,                // Compression
			0,                          // 0 = Sparse array; 1 = Dense array
			dimensions,                 // Dimensions
			2,                          // Number of dimensions
			domain,                     // Domain
			2*RANK*sizeof(int64_t),     // Domain length in bytes
			NULL,                       // Tile extents
			0,                          // Tile extents length in bytes 
			0,                          // Tile order (will be ingored)
			types                       // Types
			);

	// Create array
	tiledb_array_create(tiledb_ctx, &array_schema); 

	// Free array schema
	tiledb_array_free_schema(&array_schema);

	/* Finalize context. */
	tiledb_ctx_finalize(tiledb_ctx);

	tiledb_ctx_init(&tiledb_ctx, NULL);	

// Initialize array
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                                // Context 
      &tiledb_array,                             // Array object
      arrayname,   // Array name
      TILEDB_ARRAY_WRITE_UNSORTED,               // Mode
      NULL,                                      // Entire domain
      NULL,                                      // All attributes
      0);                                        // Number of attributes

  // Prepare cell buffers
  int64_t *buffer_coords = new int64_t [RANK*ncells];
	int *buffer_a1 = new int [ncells];
	int index = 0;
	int64_t r, c;
	int v;

	map<string, int> myMap;
	map<string, int>::iterator it;
	myMap.clear();

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
		myMap[mystream.str()] = v;
		buffer_coords[index] = r;
		index++;
		buffer_coords[index] = c;
		index++;
		buffer_a1[j] = v;
		cout << "[" << r << "," << c << "]=" << v << "\n";
	}

	const void* buffers[] = { buffer_a1, buffer_coords };
	size_t buffer_sizes[] = { (size_t)ncells*sizeof(int), (size_t)RANK*ncells*sizeof(int64_t) };

	// Write to array
  tiledb_array_write(tiledb_array, buffers, buffer_sizes);

  // Finalize array
  tiledb_array_finalize(tiledb_array);
	/* Finalize context. */
	tiledb_ctx_finalize(tiledb_ctx);

	
	tiledb_ctx_init(&tiledb_ctx, NULL);	
	// Initialize array 
  tiledb_array_init(
      tiledb_ctx,                                       // Context
      &tiledb_array,                                    // Array object
      arrayname, //"my_workspace/sparse_arrays/my_array_B",          // Array name
      TILEDB_ARRAY_READ,                                // Mode
      NULL,                                             // Whole domain
      NULL,                                             // All attributes
      0);                                               // Number of attributes
	void* rbuffers[] = { buffer_a1, buffer_coords };
	tiledb_array_read(tiledb_array, rbuffers, buffer_sizes);
	for (int i=0,index=0;i<ncells;i++) {
		cout << "[" << buffer_coords[index];
		index++;
		cout << "," << buffer_coords[index] << "]=";
		index++;
		cout << buffer_a1[i] << "\n";
	}
	tiledb_ctx_finalize(tiledb_ctx);

	return 0;
}
