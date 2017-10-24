/**
 * @file   expression.h
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
 * This file defines the class Expression to store and process filters 
 */

#ifndef __EXPRESSION_H__
#define __EXPRESSION_H__

#include "mpParser.h"
#include "array_schema.h"
#include "array_read_state.h"
#include <typeinfo>


/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_EXPR_OK          0
#define TILEDB_EXPR_ERR        -1
/**@}*/

/** Default error message. */
#define TILEDB_EXPR_ERRMSG std::string("[TileDB::Expression] Error: ")

//Forward declaration
class ArrayReadState;

class Expression {
  public:

    /* ********************************* */
    /*            MUTATORS               */
    /* ********************************* */
    Expression(
        std::string expression,
        std::vector<std::string> attribute_vec,
        std::vector<int> attributes_id,
        const ArraySchema* array_schema);

    Expression();
    ~Expression();

    void set_array_schema(const ArraySchema *array_schema);
    void set_array_read_state(ArrayReadState* array_read_state);

    int add_attribute(
        std::string name,
        const std::type_info& attribute_type);

    int add_vector(
        std::string name,
        const std::type_info& attribute_type,
        const int size,
        const int initial_value);

    int add_expression(std::string expression);

    template<class T>
    int add_udf(T *function_class);

    /**
     * The evaluate method is called after array read is done.
     * FIXME: This is extremely inefficient and only works
     * for the POC on filters. The idea is to change this
     * quickly with on-disk secondary indexing of attributes
     * which have been annotated to be indexed
     */
    int evaluate(void** buffers, size_t* buffer_sizes);

  private:
    void create_parser_object();

    mup::ParserX *parser_;
    std::string expression_;
    const ArraySchema* array_schema_;
    const ArrayReadState* array_read_state_;

    std::map<std::string, mup::Value> attribute_map_;
};
#endif // __EXPRESSION_H__
