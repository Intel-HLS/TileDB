/**
 * @file   expression.cc
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
 * This file implements the TileDB_Expression class.
 */

#include <expression.h>

TileDB_Expression::TileDB_Expression() {}

TileDB_Expression::TileDB_Expression(
    std::string expression,
    std::vector<std::string> attribute_vec,
    std::vector<int> attributes_id,
    const ArraySchema *array_schema) :
      expression_(expression) {

  // Initialize the muparser parser object
  parser_.SetExpr(expression_);

  // Add the attribute names and variables
  for (auto i = 0, ret = 0; i < attributes_id.size(); ++i) {
    ret = add_attribute(
              attribute_vec[i],
              typeid(array_schema->type(array_schema->attribute_id(attribute_vec[i]))));
  }
}

int TileDB_Expression::add_attribute(
    std::string name,
    const std::type_info& attribute_type) {

  if (attribute_type == typeid(int)) {
    mup::Value x_int(0);
    attribute_map_[name] = x_int;
    parser_.DefineVar(name, &x_int);
  } else if (attribute_type == typeid(float)) {
    mup::Value x_float(0.0);
    attribute_map_[name] = x_float;
    parser_.DefineVar(name, &x_float);
  } else if (attribute_type == typeid(double)) {
    mup::Value x_double(0.0);
    attribute_map_[name] = x_double;
    parser_.DefineVar(name, &x_double);
  } else if (attribute_type == typeid(uint)) {
    mup::Value x_uint(0.0);
    attribute_map_[name] = x_uint;
    parser_.DefineVar(name, &x_uint);
  }

  // Return Success
  return TILEDB_EXPR_OK;
}

template<class UDFClass>
int TileDB_Expression::add_udf(UDFClass* function_class) {
  parser_.DefineFun(function_class);
}

int TileDB_Expression::evaluate(
    void *attribute) {
  
  mup::Value ret = parser_.Eval();
  return TILEDB_EXPR_OK;
}
