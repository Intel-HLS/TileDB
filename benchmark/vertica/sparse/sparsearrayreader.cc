/**
 * @file   c_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corp.
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
 * This file defines the C API of TileDB.
 */

#include "Vertica.h"
#include <time.h> 
#include <sstream>
#include <map>

using namespace Vertica;
using namespace std;

class SparseColumnReader : public TransformFunction {

	public:
		virtual void processPartition(ServerInterface &srvInterface, PartitionReader &inputReader, PartitionWriter &outputWriter) {
			try {
				// Sanity checks on input/output we've been given.
				// Expected input: (doc_id INTEGER, text VARCHAR)
				const SizedColumnTypes &inTypes = inputReader.getTypeMetaData();
				vector<size_t> argCols;
				inTypes.getArgumentColumns(argCols);

				//if (argCols.size() != 2 || !inTypes.getColumnType(argCols.at(0)).isInt() ||
				//		!inTypes.getColumnType(argCols.at(1)).isInt())
				//	vt_report_error(0, "Function expects 9 arguments (INTEGER, INTEGER, ...).");

				const SizedColumnTypes &outTypes = outputWriter.getTypeMetaData();
				vector<size_t> outArgCols;
				outTypes.getArgumentColumns(outArgCols);

				vint x = inputReader.getIntRef(argCols.at(0));
				vint y = inputReader.getIntRef(argCols.at(1));
				outputWriter.setInt(outArgCols.at(0), x);
				outputWriter.setInt(outArgCols.at(1), y);
				outputWriter.next();
			} catch (exception& e) {
				// Standard exception. Quit.
				vt_report_error(0, "Exception while processing partition: [%s]", e.what());
			}
		}
};

class SparseColumnReaderFactory : public TransformFunctionFactory
{
	// Tell Vertica that we take in a row with 2 integers, and return a row with two integers
	virtual void getPrototype(ServerInterface &srvInterface, 
			ColumnTypes &argTypes, 
			ColumnTypes &returnType)
	{
		// Expected input: 2 fields of vessel_traffic
		argTypes.addInt();
		argTypes.addInt();
	
		// Output is : (X: integer, Y: integer)
		returnType.addInt();
		returnType.addInt();
	}

	// Tell Vertica what the number of return integers will be, given the input two integers
	virtual void getReturnType(ServerInterface &srvInterface, 
			const SizedColumnTypes &inputTypes, SizedColumnTypes &outTypes) {   
		// Expected input:
		//   (INTEGER, INTEGER)
		vector<size_t> argCols;
		inputTypes.getArgumentColumns(argCols);
		//vector<size_t> pByCols;
		//inputTypes.getPartitionByColumns(pByCols);
		//vector<size_t> oByCols;
		//inputTypes.getOrderByColumns(oByCols);

		//if (argCols.size() != 2 ||
		//		!inputTypes.getColumnType(argCols.at(0)).isInt() ||
		//		!inputTypes.getColumnType(argCols.at(1)).isInt())
		//	vt_report_error(0, "Function expects an argument (INTEGER) with "
		//			"analytic clause OVER(PBY VARCHAR OBY INTEGER)");

		outTypes.addInt("X");
		outTypes.addInt("Y");
	}

	virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
	{ return vt_createFuncObject<SparseColumnReader>(srvInterface.allocator); }

};
    
RegisterFactory(SparseColumnReaderFactory);
