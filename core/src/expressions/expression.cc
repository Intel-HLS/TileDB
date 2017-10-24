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
 * This file implements the Expression class.
 */
#ifdef ENABLE_MUPARSERX_EXPRESSIONS

#include <expression.h>

Expression::Expression() {
  create_parser_object();
  array_schema_ = NULL;
}

Expression::Expression(
    std::string expression,
    std::vector<std::string> attribute_vec,
    std::vector<int> attributes_id,
    const ArraySchema *array_schema) :
      expression_(expression) {

  // Allocate the muparserx object
  create_parser_object();

  // Set the array schema
  set_array_schema(array_schema);

  // Initialize the muparser parser object
  parser_->SetExpr(expression_);

  // Add the attribute names and variables
  for (auto i = 0, ret = 0; i < attributes_id.size(); ++i) {
    ret = add_attribute(
              attribute_vec[i],
              typeid(array_schema->type(array_schema->attribute_id(attribute_vec[i]))));
  }
}

void Expression::create_parser_object() {
  // pckMATRIX: Matrix package adds functions and operators for matrix support.
  // pckALL_NON_COMPLEX: Combines the flags of all packages useable with noncomplex
  // numbers. Check link below for more reference:
  // http://beltoforion.de/article.php?a=muparserx&hl=en&p=using&s=idInit#idInit
  parser_ = new mup::ParserX(mup::pckALL_NON_COMPLEX | mup::pckMATRIX);
}

void Expression::set_array_schema(const ArraySchema* array_schema) {
  array_schema_ = array_schema;
}

void Expression::set_array_read_state(ArrayReadState* array_read_state) {
  array_read_state_ = array_read_state;
}

int Expression::add_attribute(
    std::string name,
    const std::type_info& attribute_type) {

  if (attribute_type == typeid(int)) {
    mup::Value x_int(0);
    attribute_map_[name] = x_int;
    parser_->DefineVar(name, &x_int);
  } else if (attribute_type == typeid(float)) {
    mup::Value x_float(0.0);
    attribute_map_[name] = x_float;
    parser_->DefineVar(name, &x_float);
  } else if (attribute_type == typeid(double)) {
    mup::Value x_double(0.0);
    attribute_map_[name] = x_double;
    parser_->DefineVar(name, &x_double);
  } else if (attribute_type == typeid(uint)) {
    mup::Value x_uint(0.0);
    attribute_map_[name] = x_uint;
    parser_->DefineVar(name, &x_uint);
  }

  // Return Success
  return TILEDB_EXPR_OK;
}

template<class UDFClass>
int Expression::add_udf(UDFClass* function_class) {
  parser_->DefineFun(function_class);
}

int Expression::evaluate(
    void** buffers,
    size_t* buffer_sizes) {

  assert(array_schema_ != NULL);

  int buffer_i = 0;
  for (int i = 0; i < attribute_id_num; ++i) {
    attribute_map_[attributes_[i]] = array_schema_->get_attribute_value(buffers[i]);
    mup::Value ret = parser_->Eval();
    if (!ret) {
      // drop from buffer
      buffers[i]=0;
    }
  }

  return TILEDB_EXPR_OK;
}

#endif //ENABLE_MUPARSERX_EXPRESSIONS
