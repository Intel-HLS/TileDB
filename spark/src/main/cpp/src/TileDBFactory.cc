/**
 * @file   TileDBFactory.cc
 * @author Kushal Datta <kushal.datta@intel.com>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2015
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
 * Implements the JNI functions to enable TileDB classes/methods
 * in Scala/Java TileDBContext
 *
 */

#include "array_schema.h"
#include "storage_manager.h"
#include "query_processor.h"
#include "loader.h"
#include "tiledb.h"
#include "tiledb_error.h"
#include "cell.h"
#include "csv_line.h"
#include <iostream>
#include <assert.h>
#include <vector>

//////////////////////////
// JNI generated header //
//////////////////////////
#include "TileDBFactory.h"

/////////////////////////////////
// Local function declarations //
/////////////////////////////////
void GetJStringContent(JNIEnv *AEnv, jstring AStr, std::string &ARes);
TileDB_CTX *initialize_context(JNIEnv *env, jstring workspace, int &errorcode);
TileDB_CTX *initialize_context(JNIEnv *env, std::string workspace, int &errorcode);
jint sendOutOfMemory(JNIEnv* env, char *message);
jobject longToObject(JNIEnv* env, long longValue);
char ** jstringArray_to_char2D(JNIEnv* env, jobjectArray java_2d_strings);
void jintArray_to_vec(JNIEnv *env, jintArray array, std::vector<int> &vecInt);

/////////////////////////////
// Global static variables //
/////////////////////////////
extern "C" {
  typedef struct TileDB_ConstCellIterator {
    const ArraySchema* array_schema_;
    void* it_;
    bool begin_;
  } TileDB_ConstCellIterator;

  typedef struct TileDB_CTX{
    std::string* workspace_;
    Loader* loader_;
    QueryProcessor* query_processor_;
    StorageManager* storage_manager_;
  } TileDB_CTX;
}
static TileDB_CTX* global_tiledb_ctx = NULL;
static TileDB_ConstCellIterator* global_tiledb_const_cell_iterator = NULL;

/**
 * Initialize a TileDB context and return the reference.
 * This reference is important as all successive TileDB
 * operations depend on it. It is stored in TileDBContext.scala
 */
JNIEXPORT jlong JNICALL Java_com_intel_tiledb_TileDBFactory_initialize_1context(
    JNIEnv* env,
    jobject obj,
    jstring workspace) {

  int errorcode = 0;
  TileDB_CTX* tiledb_ctx = initialize_context(env, workspace, errorcode);
  global_tiledb_ctx = tiledb_ctx;
//  jclass cls = env->FindClass("com/intel/tiledb/JNITileDBContext");
//  jmethodID longConstructor = env->GetMethodID(cls,"<init>","(L)V");
//  static TileDB_CTX* tiledb_ctx_ref = (TileDB_CTX*) env->NewGlobalRef(tiledb_ctx);
  if (tiledb_ctx == NULL) {
    std::string message = "TileDB context initialization failed";
    return (jlong) sendOutOfMemory(env, (char *)message.c_str());
  }
//  std::cout << "What global is: " << global_tiledb_ctx << std::endl;
//  std::cout << "Context in TileDBFactory.cc: " << (long) global_tiledb_ctx << std::endl;
  return (long) global_tiledb_ctx;
}

/* Close the global TileDB context */
JNIEXPORT jint JNICALL Java_com_intel_tiledb_TileDBFactory_finalize_1context(
    JNIEnv* env,
    jobject obj) {
  int errorcode = tiledb_ctx_finalize(global_tiledb_ctx);
  free(global_tiledb_ctx);
  return errorcode;
}

/**
 * Open an existing array in TileDB. If the array doesn't exist,
 * throw an error
 */
JNIEXPORT jint JNICALL Java_com_intel_tiledb_TileDBFactory_open_1array(
    JNIEnv* env,
    jobject obj,
    jstring workspace,
    jstring arrayname,
    jstring mode) {

//  int errorcode = 0;
//  TileDB_CTX *tctx = initialize_context(env, workspace, errorcode);
//  TileDB_CTX *tctx = (TileDB_CTX *) context;
  assert(global_tiledb_ctx != NULL);

  const char *nm = env->GetStringUTFChars(arrayname, 0);
  const char *m = env->GetStringUTFChars(mode, 0);
//  return tiledb_array_open(tctx, nm, m);
  return tiledb_array_open(global_tiledb_ctx, nm, m);
}

/**
 * Close the array specified using the array descriptor
 */
JNIEXPORT jint JNICALL Java_com_intel_tiledb_TileDBFactory_close_1array(
    JNIEnv* env,
    jobject obj,
    jstring workspace,
    jint ad) {
  int errorcode = 0;
  TileDB_CTX *tctx = initialize_context(env, workspace, errorcode);
  const char *ws = env->GetStringUTFChars(workspace, 0);
  return tiledb_array_close(tctx, ad);
}

/**
 * Return true if an array schema is defined in TileDB
 */
JNIEXPORT jboolean JNICALL Java_com_intel_tiledb_TileDBFactory_array_1defined(
    JNIEnv* env,
    jobject obj,
    jstring workspace,
    jstring arrayname) {

  int errorcode = 0;
  TileDB_CTX *tctx = initialize_context(env, workspace, errorcode);
  const char *nm = env->GetStringUTFChars(arrayname, 0);
  return tiledb_array_defined(tctx, nm);
}

/**
 * Define an array with a given identifier, dimensions,
 * attributes, types, cell/tile order and cell size
 */
JNIEXPORT jint JNICALL Java_com_intel_tiledb_TileDBFactory_define_1array(
  JNIEnv* env,
  jobject obj,
  jstring workspace,
  jstring name,
  jstring arrayschemastr) {
  int errorcode = 0;
  TileDB_CTX *tctx = initialize_context(env, workspace, errorcode);
  char * array_schema_str = NULL;
  return tiledb_define_array(tctx, array_schema_str);
}

/**
 * Get the array schema in a CSV serialized format
 */
JNIEXPORT jstring JNICALL Java_com_intel_tiledb_TileDBFactory_array_1schema(
    JNIEnv* env,
    jobject obj,
    jlong jniContext,
    jstring workspace,
    jstring arrayname) {

  assert(global_tiledb_ctx != NULL);
  TileDB_CTX *tiledb_ctx = (TileDB_CTX *) global_tiledb_ctx;
  const char *nm = env->GetStringUTFChars(arrayname, 0);
  size_t size = 1000;
  char *array_schema_str = new char [size];
  tiledb_array_schema(tiledb_ctx, nm, array_schema_str, &size);
  jstring retStr = env->NewStringUTF(array_schema_str);

  return retStr;
}

/** Export the entire array to Java memory space */
JNIEXPORT jint JNICALL Java_com_intel_tiledb_TileDBFactory_import_1all_1cells(
    JNIEnv* env,
    jobject obj,
    jstring array_name,
    jstring file_name,
    jobjectArray dim_names,
    jobjectArray attr_names) {

  assert(global_tiledb_ctx != NULL);
  TileDB_CTX *tiledb_ctx = (TileDB_CTX *) global_tiledb_ctx;

  char ** dim_name_array = (dim_names == NULL) ? NULL : jstringArray_to_char2D(env, dim_names);
  char ** attr_name_array = jstringArray_to_char2D(env, attr_names);

  std::cout << "import all cells called" << std::endl;

  return tiledb_export_csv(tiledb_ctx, env->GetStringUTFChars(array_name, NULL),
                env->GetStringUTFChars(file_name, NULL), (const char **) dim_name_array,
                env->GetArrayLength(dim_names),
                (const char **) attr_name_array, env->GetArrayLength(attr_names), false);
}

/** Initialize a constant cell iterator */
JNIEXPORT jint JNICALL Java_com_intel_tiledb_TileDBFactory_initialize_1const_1cell_1iterator(
    JNIEnv* env,
    jobject obj,
    jint array_id,
    jobjectArray attribute_names) {

  assert(global_tiledb_ctx != NULL);
  TileDB_CTX *tiledb_ctx = (TileDB_CTX *) global_tiledb_ctx;

  char ** attribute_name_array = jstringArray_to_char2D(env, attribute_names);
  TileDB_ConstCellIterator *cell_it = NULL;

  for (int i = 0; i < env->GetArrayLength(attribute_names); ++i) {
    std::cout << "Attribute name in cc: " << attribute_name_array[i] << "\n";
  }
  /* Initialize the cell iterator */
  int errorcode = tiledb_const_cell_iterator_init(tiledb_ctx, array_id,
    (const char **) attribute_name_array, env->GetArrayLength(attribute_names), cell_it);

  global_tiledb_const_cell_iterator = (TileDB_ConstCellIterator *) cell_it;
  return errorcode;
}

/** Return the array contents in a JNI string array */
JNIEXPORT jint JNICALL Java_com_intel_tiledb_TileDBFactory_import_1cells(
    JNIEnv* env,
    jobject obj,
    jint segment_size,
    jintArray dim_ids,
    jstring dim_type,
    jintArray attr_ids,
    jobjectArray cells) {

  assert(global_tiledb_ctx != NULL);
  assert(global_tiledb_const_cell_iterator != NULL);

  TileDB_CTX *tiledb_ctx = (TileDB_CTX *) global_tiledb_ctx;
  TileDB_ConstCellIterator *cell_it = (TileDB_ConstCellIterator *)
    global_tiledb_const_cell_iterator;

  char *dimType = new char [50];
  strcpy(dimType, env->GetStringUTFChars(dim_type, 0));

  const ArraySchema *array_schema = cell_it->array_schema_;
  std::vector<int>attr_ids_vec;
  attr_ids_vec.clear();
  jintArray_to_vec(env, attr_ids, attr_ids_vec);

  /** Just need to pass an empty dimension id vector. For load
   * operation all the dimensions are required for a sweep over
   * the book-keeping data structures. Therefore, even if a
   * subset of dimension ids are passed, they are eventually
   * neglected in native TileDB
   */
  std::vector<int>dim_ids_vec;
  dim_ids_vec.clear();
  jintArray_to_vec(env, dim_ids, dim_ids_vec);
  Cell cell(array_schema, attr_ids_vec, 0, true);
  const void * voidCell = NULL;
  long num_cells = 0;
  int errorcode = 0;

  for (int i = 0; i < dim_ids_vec.size(); ++i) {
    std::cout << "dim id in cc: " << dim_ids_vec[i] << "\n";
  }
  for (int i = 0; i < attr_ids_vec.size(); ++i) {
      std::cout << "attr id in cc: " << attr_ids_vec[i] << "\n";
  }
  do {
    voidCell = NULL;
    Cell cell(array_schema, attr_ids_vec, 0, true);
    errorcode = tiledb_const_cell_iterator_next(cell_it, voidCell);
    if (voidCell) {
      cell.set_cell(voidCell);
//      std::cout << "Cell in cc: ";
      if (strcmp(dimType, "double") == 0) {
//        std::cout << cell.csv_line<double>(dim_ids_vec, attr_ids_vec).c_str() << "\n";
        env->SetObjectArrayElement(cells, num_cells,
               env->NewStringUTF(cell.csv_line<double>(dim_ids_vec, attr_ids_vec).c_str()));
      } else if (strcmp(dimType, "int64") == 0) {
//        std::cout << cell.csv_line<int64_t>(dim_ids_vec, attr_ids_vec).c_str() << "\n";
        env->SetObjectArrayElement(cells, num_cells,
               env->NewStringUTF(cell.csv_line<int64_t>(dim_ids_vec, attr_ids_vec).c_str()));
        //env->SetObjectArrayElement(cells, num_cells,
        //env->NewStringUTF("1,1,1,1,1"));
      }
      ++num_cells;
    }
  } while (errorcode == TILEDB_OK && num_cells < segment_size && voidCell);

  std::cout << "Number of cell returned from JNI: " << num_cells << "\n";
  return num_cells;
}

/* Delete the cell iterator object of a given array */
JNIEXPORT jint JNICALL Java_com_intel_tiledb_TileDBFactory_finalize_1const_1cell_1iterator(
    JNIEnv* env,
    jobject obj) {

  assert(global_tiledb_const_cell_iterator != NULL);
  int errorcode = tiledb_const_cell_iterator_finalize(global_tiledb_const_cell_iterator);
  free (global_tiledb_const_cell_iterator);
  return errorcode;
}

JNIEXPORT jint JNICALL Java_com_intel_tiledb_TileDBFactory_store_1cells(
    JNIEnv* env,
    jobject obj) {
//  int rc = tiledb_load_csv();
  return 0;
}



/////////////////////////////////////////////
// Before and After methods for JNI library//
/////////////////////////////////////////////

JNIEXPORT int JNI_OnLoad(JNIEnv* env, jobject obj) {
  // TODO: Do nothing at this moment
  return 0;
}
JNIEXPORT int JNI_OnUnload(JNIEnv* env, jobject obj) {
  if (global_tiledb_ctx != NULL) {
    free(global_tiledb_ctx);
  }
}



/////////////////////
// Private Methods //
/////////////////////

/* Initialize TileDB context through TileDB C API */
TileDB_CTX *initialize_context(JNIEnv *env, jstring workspace, int &errorcode) {
  TileDB_CTX *tiledb_ctx = NULL;
  const char *ws = env->GetStringUTFChars(workspace, 0);
  errorcode = tiledb_ctx_init(ws, tiledb_ctx);
  if (errorcode != TILEDB_OK) {
    std::cout << "Failed to initialize TileDB context\n";
    return NULL;
  }
  return tiledb_ctx;
}

/* Initialize TileDB context through TileDB C API */
TileDB_CTX *initialize_context(JNIEnv *env, std::string workspace, int &errorcode) {
  TileDB_CTX *tiledb_ctx = NULL;
  const char *ws = workspace.c_str();
  errorcode = tiledb_ctx_init(ws, tiledb_ctx);
  if (errorcode != TILEDB_OK) {
    std::cout << "Failed to initialize TileDB context\n";
    return NULL;
  }
  return tiledb_ctx;
}

/* Convert jstring to c++ string */
void GetJStringContent(JNIEnv *env, jstring AStr, std::string &ARes) {
  if (!AStr) {
    ARes.clear();
    return;
  }

  const char *s = env->GetStringUTFChars(AStr,NULL);
  ARes=s;
  env->ReleaseStringUTFChars(AStr,s);
}

/** Throw Java OutOfMemoryError */
jint sendOutOfMemory(JNIEnv* env, char *message) {
  jclass oomClass = env->FindClass("java/lang/OutOfMemoryError");
  return env->ThrowNew(oomClass, message);
}

jobject longToObject(JNIEnv* env, long longValue) {
  jclass cls = env->FindClass("java/lang/Long");
  jmethodID longConstructor = env->GetMethodID(cls,"<init>","(J)V");
  jobject longObject = env->NewObject(cls, longConstructor, longValue);
  return longObject;
}

char ** jstringArray_to_char2D(JNIEnv *env, jobjectArray java_string_array) {

  int stringCount = env->GetArrayLength(java_string_array);
  char ** cpp_array = new char *[stringCount];
  for (int i=0; i<stringCount; ++i) {
    jstring string = (jstring) env->GetObjectArrayElement(java_string_array, i);
    cpp_array[i] = new char[env->GetStringLength(string)];
    strcpy(cpp_array[i], env->GetStringUTFChars(string, 0));
  }
  return cpp_array;
}

void jintArray_to_vec(JNIEnv *env, jintArray array, std::vector<int> &vecInt) {
  jboolean j;
  int * p= env->GetIntArrayElements(array, &j);
  const jsize length = env->GetArrayLength(array);
  for (int i = 0; i < length; ++i) {
    vecInt.push_back(p[i]);
  }
}
