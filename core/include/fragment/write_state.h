/**
 * @file   write_state.h
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
 * This file defines class WriteState. 
 */

#ifndef __WRITE_STATE_H__
#define __WRITE_STATE_H__

#include "book_keeping.h"
#include "fragment.h"
#include <vector>
#include <iostream>




/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_WS_OK        0
#define TILEDB_WS_ERR      -1
/**@}*/

/** Default error message. */
#define TILEDB_WS_ERRMSG std::string("[TileDB::WriteState] Error: ")




/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
extern std::string tiledb_ws_errmsg;




class BookKeeping;
class Fragment;




/** Stores the state necessary when writing cells to a fragment. */
class WriteState {
 public:

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** 
   * Constructor. 
   *
   * @param fragment The fragment the write state belongs to.
   * @param book_keeping The book-keeping *fragment*.
   */
  WriteState(const Fragment* fragment, BookKeeping* book_keeping);

  /** Destructor. */
  ~WriteState();




  /* ********************************* */
  /*              MUTATORS             */
  /* ********************************* */

  /**
   * Finalizes the fragment.
   *
   * @return TILEDB_WS_OK for success and TILEDB_WS_ERR for error. 
   */
  int finalize();

  /**
   * Syncs all attribute files in the fragment.
   * 
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int sync();

  /**
   * Syncs the input attribute in the fragment.
   * 
   * @param attribute The attribute name.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int sync_attribute(const std::string& attribute);

  
  /**
   * Performs a write operation in the fragment. The cell values are provided
   * in a set of buffers (one per attribute specified upon the array 
   * initialization). Note that there must be a one-to-one correspondance
   * between the cell values across the attribute buffers.
   *
   * The array must have been initialized in one of the following write modes,
   * each of which having a different behaviour:
   *    - TILEDB_ARRAY_WRITE: \n
   *      In this mode, the cell values are provided in the buffers respecting
   *      the cell order on the disk. It is practically an **append** operation,
   *      where the provided cell values are simply written at the end of
   *      their corresponding attribute files.  
   *    - TILEDB_ARRAY_WRITE_UNSORTED: \n
   *      This mode is applicable to sparse arrays, or when writing sparse
   *      updates to a dense array. One of the buffers holds the coordinates.
   *      The cells in this mode are given in an arbitrary, unsorted order
   *      (i.e., without respecting how the cells must be stored on the disk
   *      according to the array schema definition). 
   * 
   * @param buffers An array of buffers, one for each attribute. These must be
   *     provided in the same order as the attributes specified in
   *     Array::init() or Array::reset_attributes(). The case of variable-sized
   *     attributes is special. Instead of providing a single buffer for such an
   *     attribute, **two** must be provided: the second holds the
   *     variable-sized cell values, whereas the first holds the start offsets
   *     of each cell in the second buffer.
   * @param buffer_sizes The sizes (in bytes) of the input buffers (there is
   *     a one-to-one correspondence).
   * @return TILEDB_WS_OK for success and TILEDB_WS_ERR for error.
   */
  int write(
      const void** buffers, 
      const size_t* buffer_sizes);

  /**
   * Set zlib compression level
   * @param level zlib compression level
   */
  void set_zlib_compression_level(const int level);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array the fragment belongs to. */
  const Array* array_;
  /** The array schema. */
  const ArraySchema* array_schema_;
      /** The number of array attributes. */
  int attribute_num_;
  
  /** The book-keeping structure of the fragment the write state belongs to. */
  BookKeeping* book_keeping_;
  /** The first and last coordinates of the tile currently being populated. */
  void* bounding_coords_;

  /** Internal buffers associated with the attribute files */
  std::vector<Buffer *> file_buffer_;
  std::vector<Buffer *> file_var_buffer_;

  /**  
   * The current offsets of the variable-sized attributes in their 
   * respective files, or alternatively, the current file size of each
   * variable-sized attribute.
   */
  std::vector<size_t> buffer_var_offsets_;
  /** The fragment the write state belongs to. */
  const Fragment* fragment_;
  /** The MBR of the tile currently being populated. */
  void* mbr_;
  /** The number of cells written in the current tile for each attribute. */
  std::vector<int64_t> tile_cell_num_;
  /** Internal buffers used in the case of compression. */
  std::vector<void*> tiles_;
  /** Offsets to the internal variable tile buffers. */
  std::vector<size_t> tiles_var_offsets_;
  /** Internal buffers used in the case of compression for variable tiles. */
  std::vector<void*> tiles_var_;
  /** 
   * Sizes of internal buffers used in the case of compression for variable 
   * tiles. 
   */
  std::vector<size_t> tiles_var_sizes_;
  /** Internal buffer used in the case of compression. */
  void* tile_compressed_;
  /** Allocated size for internal buffer used in the case of compression. */
  size_t tile_compressed_allocated_size_;
  /** Offsets to the internal tile buffers used in compression. */
  std::vector<size_t> tile_offsets_;
  /** zlib compression level. */
  int zlib_compression_level_;

  /** The Storage Filesystem */
  StorageFS *fs_;



  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Compresses the input tile buffer, and stores it inside tile_compressed_
   * member attribute. 
   * 
   * @param attribute_id The id of the attribute the tile belongs to.
   * @param tile The tile buffer to be compressed.
   * @param tile_size The size of the tile buffer in bytes.
   * @param tile_compressed_size The size of the resulting compressed tile.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int compress_tile(
      int attribute_id,
      unsigned char* tile,
      size_t tile_size,
      size_t& tile_compressed_size);

  /**
   * Compresses with GZIP the input tile buffer, and stores it inside 
   * tile_compressed_ member attribute. 
   * 
   * @param tile The tile buffer to be compressed.
   * @param tile_size The size of the tile buffer in bytes.
   * @param tile_compressed_size The size of the resulting compressed tile.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int compress_tile_gzip(
      unsigned char* tile,
      size_t tile_size,
      size_t& tile_compressed_size);

  /**
   * Compresses with Zstandard the input tile buffer, and stores it inside 
   * tile_compressed_ member attribute. 
   * 
   * @param tile The tile buffer to be compressed.
   * @param tile_size The size of the tile buffer in bytes.
   * @param tile_compressed_size The size of the resulting compressed tile.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int compress_tile_zstd(
      unsigned char* tile,
      size_t tile_size,
      size_t& tile_compressed_size);

  /**
   * Compresses with LZ4 the input tile buffer, and stores it inside 
   * tile_compressed_ member attribute. 
   * 
   * @param tile The tile buffer to be compressed.
   * @param tile_size The size of the tile buffer in bytes.
   * @param tile_compressed_size The size of the resulting compressed tile.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int compress_tile_lz4(
      unsigned char* tile,
      size_t tile_size,
      size_t& tile_compressed_size);

  /**
   * Compresses with Blosc the input tile buffer, and stores it inside 
   * tile_compressed_ member attribute. 
   * 
   * @param attribute_id The id of the attribute the tile belongs to.
   * @param tile The tile buffer to be compressed.
   * @param tile_size The size of the tile buffer in bytes.
   * @param tile_compressed_size The size of the resulting compressed tile.
   * @param compressor  The Blosc compressor.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int compress_tile_blosc(
      int attribute_id,
      unsigned char* tile,
      size_t tile_size,
      size_t& tile_compressed_size,
      const char* compressor);

  /**
   * Compresses with RLE the input tile buffer, and stores it inside 
   * tile_compressed_ member attribute. 
   * 
   * @param attribute_id The id of the attribute the tile belongs to.
   * @param tile The tile buffer to be compressed.
   * @param tile_size The size of the tile buffer in bytes.
   * @param tile_compressed_size The size of the resulting compressed tile.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int compress_tile_rle(
      int attribute_id,
      unsigned char* tile,
      size_t tile_size,
      size_t& tile_compressed_size);

  /**
   * Compresses the current tile for the input attribute, and writes (appends)
   * it to its corresponding file on the disk.
   *
   * @param attribute_id The id of the attribute whose tile is compressed and
   *     written.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int compress_and_write_tile(int attribute_id);

  /**
   * Compresses the current variable-sized tile for the input attribute, and
   * writes (appends) it to its corresponding file on the disk.
   *
   * @param attribute_id The id of the attribute whose tile is compressed and
   *     written.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int compress_and_write_tile_var(int attribute_id);

  /**
   * Expands the current MBR with the input coordinates.
   *
   * @tparam T The type of the MBR and the input coordinates.
   * @param coords The input coordinates.
   * @return void
   */
  template<class T>
  void expand_mbr(const T* coords);

  /**
   * Shifts the offsets of the variable-sized cells recorded in the input
   * buffer, so that they correspond to the actual offsets in the corresponding
   * attribute file.
   *
   * @param attribute_id The id of the attribute whose variable cell offsets are
   *     shifted.
   * @param buffer_var_size The total size of the variable-sized cells written
   *     during this write operation.
   * @param buffer Holds the offsets of the variable-sized cells for this write
   *     operation.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param shifted_buffer Will hold the new shifted offsets.
   * @return void
   */
  void shift_var_offsets(
      int attribute_id,
      size_t buffer_var_size,
      const void* buffer,
      size_t buffer_size,
      void* shifted_buffer);

  /**
   * Sorts the input cell coordinates according to the order specified in the
   * array schema. This is not done in place; the sorted positions are stored
   * in a separate vector.
   * 
   * @param buffer The buffer holding the cell coordinates.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param cell_pos The sorted cell positions.
   * @return void 
   */
  void sort_cell_pos(
      const void* buffer, 
      size_t buffer_size,
      std::vector<int64_t>& cell_pos) const;

  /**
   * Sorts the input cell coordinates according to the order specified in the
   * array schema. This is not done in place; the sorted positions are stored
   * in a separate vector.
   * 
   * @tparam T The type of coordinates stored in *buffer*.
   * @param buffer The buffer holding the cell coordinates.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param cell_pos The sorted cell positions.
   * @return void 
   */
  template<class T>
  void sort_cell_pos(
      const void* buffer, 
      size_t buffer_size,
      std::vector<int64_t>& cell_pos) const;

  /**
   * Updates the book-keeping structures as tiles are written. Specifically, it
   * updates the MBR and bounding coordinates of each tile.
   *
   * @param buffer The buffer storing the cell coordinates.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @return void
   */
  void update_book_keeping(const void* buffer, size_t buffer_size);

  /**
   * Updates the book-keeping structures as tiles are written. Specifically, it
   * updates the MBR and bounding coordinates of each tile.
   *
   * @tparam T The coordinates type.
   * @param buffer The buffer storing the cell coordinates.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @return void
   */
  template<class T>
  void update_book_keeping(const void* buffer, size_t buffer_size);

  std::string construct_filename(int attribute_id, bool is_var);

  /**
   * Set up memory buffers to cache bytes to be ultimately written out
   * to the files
   */
  void init_file_buffers();

  /**
   * Persist the bytes in the memory buffers to the associated files
   *
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */ 
  int write_file_buffers();

  /**
   * Writes a segment from the file associated with the attribute.
   * @param attribute_id The id of the attribute.
   * @param is_var Boolean to specify whether the attribute is var.
   * @param segment Pointer to segment to be written out.
   * @param length Length of the segment to be written out.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_segment(int attribute_id, bool is_var, const void *segment, size_t length);

  /**
   * Takes the appropriate actions for writing the very last tile of this write
   * operation, such as updating the book-keeping structures, and compressing
   * and writing the last tile on the disk. This is done for every attribute.
   *
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_last_tile();

  /**
   * Performs the write operation for the case of a dense fragment.
   *
   * @param buffers See write().
   * @param buffer_sizes See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_dense(
      const void** buffers, 
      const size_t* buffer_sizes);

  /**
   * Performs the write operation for the case of a dense fragment, focusing
   * on a single fixed-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_dense_attr(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  /**
   * Performs the write operation for the case of a dense fragment, focusing
   * on a single fixed-sized attribute and the case of no compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_dense_attr_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  /**
   * Performs the write operation for the case of a dense fragment, focusing
   * on a single fixed-sized attribute and the case of any compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_dense_attr_cmp(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  /**
   * Performs the write operation for the case of a dense fragment, focusing
   * on a single variable-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_var_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_dense_attr_var(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  /**
   * Performs the write operation for the case of a dense fragment, focusing
   * on a single variable-sized attribute and the case of no compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_var_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_dense_attr_var_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  /**
   * Performs the write operation for the case of a dense fragment, focusing
   * on a single variable-sized attribute and the case of any compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_var_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_dense_attr_var_cmp(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  /**
   * Performs the write operation for the case of a sparse fragment.
   *
   * @param buffers See write().
   * @param buffer_sizes See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse(
      const void** buffers, 
      const size_t* buffer_sizes);

  /**
   * Performs the write operation for the case of a sparse fragment, focusing
   * on a single fixed-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_attr(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  /**
   * Performs the write operation for the case of a sparse fragment, focusing
   * on a single fixed-sized attribute and the case of no compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_attr_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  /**
   * Performs the write operation for the case of a sparse fragment, focusing
   * on a single fixed-sized attribute and the case of any compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_attr_cmp(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  /**
   * Performs the write operation for the case of a sparse fragment, focusing
   * on a single variable-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_var_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_attr_var(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  /**
   * Performs the write operation for the case of a sparse fragment, focusing
   * on a single variable-sized attribute and the case of no compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_var_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_attr_var_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  /**
   * Performs the write operation for the case of a sparse fragment, focusing
   * on a single variable-sized attribute and the case of any compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_var_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_attr_var_cmp(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  /**
   * Performs the write operation for the case of a sparse fragment when the 
   * coordinates are unsorted.
   *
   * @param buffers See write().
   * @param buffer_sizes See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_unsorted(
      const void** buffers, 
      const size_t* buffer_sizes);

  /**
   * Performs the write operation for the case of a sparse fragment when the 
   * coordinates are unsorted, focusing on a single fixed-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @param cell_pos The sorted positions of the cells.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_unsorted_attr(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const std::vector<int64_t>& cell_pos);

  /**
   * Performs the write operation for the case of a sparse fragment when the 
   * coordinates are unsorted, focusing on a single fixed-sized attribute and
   * the case of no compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @param cell_pos The sorted positions of the cells.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_unsorted_attr_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const std::vector<int64_t>& cell_pos);

  /**
   * Performs the write operation for the case of a sparse fragment when the 
   * coordinates are unsorted, focusing on a single fixed-sized attribute and
   * the case of any compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @param cell_pos The sorted positions of the cells.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_unsorted_attr_cmp(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const std::vector<int64_t>& cell_pos);

  /**
   * Performs the write operation for the case of a sparse fragment when the 
   * coordinates are unsorted, focusing on a single variable-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_var_size See write().
   * @param cell_pos The sorted positions of the cells.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_unsorted_attr_var(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size,
      const std::vector<int64_t>& cell_pos);

  /**
   * Performs the write operation for the case of a sparse fragment when the 
   * coordinates are unsorted, focusing on a single variable-sized attribute and
   * the case of no compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_var_size See write().
   * @param cell_pos The sorted positions of the cells.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_unsorted_attr_var_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size,
      const std::vector<int64_t>& cell_pos);

  /**
   * Performs the write operation for the case of a sparse fragment when the 
   * coordinates are unsorted, focusing on a single variable-sized attribute and
   * the case of any compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See in write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_var_size See write().
   * @param cell_pos The sorted positions of the cells.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_unsorted_attr_var_cmp(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size,
      const std::vector<int64_t>& cell_pos);
};

#endif
