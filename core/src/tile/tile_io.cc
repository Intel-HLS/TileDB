/**
 * @file   tile_io.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file implements class TileIO.
 */

#include "tile_io.h"
#include "blosc_compressor.h"
#include "bzip_compressor.h"
#include "dd_compressor.h"
#include "gzip_compressor.h"
#include "logger.h"
#include "lz4_compressor.h"
#include "rle_compressor.h"
#include "zstd_compressor.h"

#include <iostream>

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

TileIO::TileIO(StorageManager* storage_manager, const URI& uri)
    : uri_(uri)
    , storage_manager_(storage_manager) {
  buffer_ = new Buffer();
}

TileIO::~TileIO() {
  delete buffer_;
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status TileIO::file_size(uint64_t* size) const {
  return storage_manager_->file_size(uri_, size);
}

Status TileIO::read(
    Tile* tile,
    uint64_t file_offset,
    uint64_t compressed_size,
    uint64_t tile_size) {
  // No compression
  if (tile->compressor() == Compressor::NO_COMPRESSION)
    return storage_manager_->read_from_file(
        uri_, file_offset, tile->buffer(), tile_size);

  // Compression
  RETURN_NOT_OK(storage_manager_->read_from_file(
      uri_, file_offset, buffer_, compressed_size));

  // Decompress tile
  RETURN_NOT_OK(tile->realloc(tile_size));
  RETURN_NOT_OK(decompress_tile(tile));

  // Zip coordinates if this is a coordinates tile
  if (tile->stores_coords())
    tile->zip_coordinates();

  // TODO: here we will put all other filters, and potentially employ chunking
  // TODO: choose the proper buffer based on all filters, not just compression

  return Status::Ok();
}

Status TileIO::read_generic(Tile** tile, uint64_t file_offset) {
  uint64_t tile_size;
  uint64_t compressed_size;
  uint64_t header_size;
  RETURN_NOT_OK(read_generic_tile_header(
      tile, file_offset, &tile_size, &compressed_size, &header_size));
  RETURN_NOT_OK_ELSE(
      read(*tile, file_offset + header_size, compressed_size, tile_size),
      delete *tile);

  return Status::Ok();
}

Status TileIO::read_generic_tile_header(
    Tile** tile,
    uint64_t file_offset,
    uint64_t* tile_size,
    uint64_t* compressed_size,
    uint64_t* header_size) {
  // Initializations
  *header_size = 3 * sizeof(uint64_t) + 2 * sizeof(char) + sizeof(int);
  char datatype;
  uint64_t cell_size;
  char compressor;
  int compression_level;

  // Read header from file
  auto header_buff = new Buffer();
  RETURN_NOT_OK_ELSE(
      storage_manager_->read_from_file(
          uri_, file_offset, header_buff, *header_size),
      delete header_buff);

  // Read header individual values
  RETURN_NOT_OK_ELSE(
      header_buff->read(compressed_size, sizeof(uint64_t)), delete header_buff);
  RETURN_NOT_OK_ELSE(
      header_buff->read(tile_size, sizeof(uint64_t)), delete header_buff);
  RETURN_NOT_OK_ELSE(
      header_buff->read(&datatype, sizeof(char)), delete header_buff);
  RETURN_NOT_OK_ELSE(
      header_buff->read(&cell_size, sizeof(uint64_t)), delete header_buff);
  RETURN_NOT_OK_ELSE(
      header_buff->read(&compressor, sizeof(char)), delete header_buff);
  RETURN_NOT_OK_ELSE(
      header_buff->read(&compression_level, sizeof(int)), delete header_buff);

  delete header_buff;

  *tile = new Tile((Datatype)datatype, (Compressor)compressor, cell_size, 0);

  return Status::Ok();
}

Status TileIO::write(Tile* tile, uint64_t* bytes_written) {
  // TODO: here we will put all other filters, and potentially employ chunking
  // TODO: choose the proper buffer based on all filters, not just compression

  // Split coordinates if this is a coordinates tile
  if (tile->stores_coords())
    tile->split_coordinates();

  // Compress tile
  RETURN_NOT_OK(compress_tile(tile));

  // Prepare to write
  Compressor compressor = tile->compressor();
  auto buffer =
      (compressor == Compressor::NO_COMPRESSION) ? tile->buffer() : buffer_;
  *bytes_written = buffer->size();

  return storage_manager_->write_to_file(uri_, buffer);
}

Status TileIO::write_generic(Tile* tile) {
  // TODO: here we will put all other filters, and potentially employ chunking
  // TODO: choose the proper buffer based on all filters, not just compression

  // Split coordinates if this is a coordinates tile
  if (tile->stores_coords())
    tile->split_coordinates();

  // Compress tile
  RETURN_NOT_OK(compress_tile(tile));

  // Prepare to write
  Compressor compressor = tile->compressor();

  auto buffer =
      (compressor == Compressor::NO_COMPRESSION) ? tile->buffer() : buffer_;

  RETURN_NOT_OK(write_generic_tile_header(tile, buffer->size()));

  return storage_manager_->write_to_file(uri_, buffer);
}

Status TileIO::write_generic_tile_header(Tile* tile, uint64_t compressed_size) {
  // Initializations
  uint64_t tile_size = tile->size();
  auto datatype = (char)tile->type();
  uint64_t cell_size = tile->cell_size();
  auto compressor = (char)tile->compressor();
  int compression_level = tile->compression_level();

  // Write to buffer
  auto buff = new Buffer();
  RETURN_NOT_OK_ELSE(
      buff->write(&compressed_size, sizeof(uint64_t)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&tile_size, sizeof(uint64_t)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&datatype, sizeof(char)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&cell_size, sizeof(uint64_t)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&compressor, sizeof(char)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&compression_level, sizeof(int)), delete buff);

  // Write to file
  Status st = storage_manager_->write_to_file(uri_, buff);

  delete buff;

  return st;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status TileIO::compress_tile(Tile* tile) {
  // Reset the tile and buffer offset
  tile->reset_offset();
  buffer_->reset_offset();

  // Invoke the proper decompressor
  Compressor compression = tile->compressor();
  int level = tile->compression_level();
  switch (compression) {
    case Compressor::NO_COMPRESSION:
      return Status::Ok();
    case Compressor::GZIP:
      return compress_tile_gzip(tile, level);
    case Compressor::ZSTD:
      return compress_tile_zstd(tile, level);
    case Compressor::LZ4:
      return compress_tile_lz4(tile, level);
    case Compressor::BLOSC:
      return compress_tile_blosc(tile, level, "blosclz");
#undef BLOSC_LZ4
    case Compressor::BLOSC_LZ4:
      return compress_tile_blosc(tile, level, "lz4");
#undef BLOSC_LZ4HC
    case Compressor::BLOSC_LZ4HC:
      return compress_tile_blosc(tile, level, "lz4hc");
#undef BLOSC_SNAPPY
    case Compressor::BLOSC_SNAPPY:
      return compress_tile_blosc(tile, level, "snappy");
#undef BLOSC_ZLIB
    case Compressor::BLOSC_ZLIB:
      return compress_tile_blosc(tile, level, "zlib");
#undef BLOSC_ZSTD
    case Compressor::BLOSC_ZSTD:
      return compress_tile_blosc(tile, level, "zstd");
    case Compressor::RLE:
      return compress_tile_rle(tile);
    case Compressor::BZIP2:
      return compress_tile_bzip2(tile, level);
    case Compressor::DOUBLE_DELTA:
      return compress_tile_double_delta(tile);
  }
}

Status TileIO::compress_tile_gzip(Tile* tile, int level) {
  uint64_t tile_size = tile->size();
  uint64_t gzip_overhead = GZip::overhead(tile_size);
  RETURN_NOT_OK(buffer_->realloc(tile_size + gzip_overhead));
  RETURN_NOT_OK(GZip::compress(level, tile->buffer(), buffer_));

  return Status::Ok();
}

Status TileIO::compress_tile_zstd(Tile* tile, int level) {
  uint64_t tile_size = tile->size();
  uint64_t compress_bound = ZStd::compress_bound(tile_size);
  RETURN_NOT_OK(buffer_->realloc(compress_bound));
  RETURN_NOT_OK(ZStd::compress(level, tile->buffer(), buffer_));

  return Status::Ok();
}

Status TileIO::compress_tile_lz4(Tile* tile, int level) {
  // Allocate space to store the compressed tile
  uint64_t tile_size = tile->size();
  uint64_t compress_bound = LZ4::compress_bound(tile_size);
  RETURN_NOT_OK(buffer_->realloc(compress_bound));
  RETURN_NOT_OK(LZ4::compress(level, tile->buffer(), buffer_));

  // Success
  return Status::Ok();
}

Status TileIO::compress_tile_blosc(
    Tile* tile, int level, const char* compressor) {
  // Allocate space to store the compressed tile
  uint64_t tile_size = tile->size();
  uint64_t compress_bound = Blosc::compress_bound(tile_size);
  RETURN_NOT_OK(buffer_->realloc(compress_bound));
  RETURN_NOT_OK(Blosc::compress(
      compressor, datatype_size(tile->type()), level, tile->buffer(), buffer_));

  return Status::Ok();
}

Status TileIO::compress_tile_rle(Tile* tile) {
  uint64_t tile_size = tile->size();
  uint64_t value_size = tile->cell_size();
  uint64_t compress_bound = RLE::compress_bound(tile_size, value_size);
  RETURN_NOT_OK(buffer_->realloc(compress_bound));
  RETURN_NOT_OK(RLE::compress(value_size, tile->buffer(), buffer_));

  // Success
  return Status::Ok();
}

Status TileIO::compress_tile_bzip2(Tile* tile, int level) {
  // Allocate space to store the compressed tile
  uint64_t tile_size = tile->size();
  uint64_t compress_bound = BZip::compress_bound(tile_size);
  RETURN_NOT_OK(buffer_->realloc(compress_bound));
  RETURN_NOT_OK(BZip::compress(level, tile->buffer(), buffer_));

  return Status::Ok();
}

Status TileIO::compress_tile_double_delta(Tile* tile) {
  // Allocate space to store the compressed tile
  uint64_t tile_size = tile->size();
  auto dim_num = tile->dim_num();
  auto batch_num = (dim_num == 0) ? 1 : dim_num;
  uint64_t batch_size = tile_size / batch_num;
  uint64_t compress_bound = tile_size + batch_num * DoubleDelta::OVERHEAD;
  RETURN_NOT_OK(buffer_->realloc(compress_bound));

  Status st;
  for (unsigned int i = 0; i < batch_num; ++i) {
    auto input_buffer =
        new ConstBuffer((char*)tile->data() + i * batch_size, batch_size);
    switch (tile->type()) {
      case Datatype::CHAR:
        st = DoubleDelta::compress<char>(input_buffer, buffer_);
        break;
      case Datatype::INT8:
        st = DoubleDelta::compress<int8_t>(input_buffer, buffer_);
        break;
      case Datatype::UINT8:
        st = DoubleDelta::compress<uint8_t>(input_buffer, buffer_);
        break;
      case Datatype::INT16:
        st = DoubleDelta::compress<int16_t>(input_buffer, buffer_);
        break;
      case Datatype::UINT16:
        st = DoubleDelta::compress<uint16_t>(input_buffer, buffer_);
        break;
      case Datatype::INT32:
        st = DoubleDelta::compress<int>(input_buffer, buffer_);
        break;
      case Datatype::UINT32:
        st = DoubleDelta::compress<uint32_t>(input_buffer, buffer_);
        break;
      case Datatype::INT64:
        st = DoubleDelta::compress<int64_t>(input_buffer, buffer_);
        break;
      case Datatype::UINT64:
        st = DoubleDelta::compress<uint64_t>(input_buffer, buffer_);
        break;
      default:
        st = LOG_STATUS(Status::TileIOError(
            "Cannot compress tile with DoubleDelta; Not supported datatype"));
    }

    delete input_buffer;
    RETURN_NOT_OK(st);
  }

  return st;
}

Status TileIO::decompress_tile(Tile* tile) {
  // Reset tile offset
  tile->reset_offset();

  // Invoke the proper decompressor
  Compressor compression = tile->compressor();
  Status st;
  switch (compression) {
    case Compressor::NO_COMPRESSION:
      st = Status::Ok();
      break;
    case Compressor::GZIP:
      st = GZip::decompress(buffer_, tile->buffer());
      break;
    case Compressor::ZSTD:
      st = ZStd::decompress(buffer_, tile->buffer());
      break;
    case Compressor::LZ4:
      st = LZ4::decompress(buffer_, tile->buffer());
      break;
    case Compressor::BLOSC:
#undef BLOSC_LZ4
    case Compressor::BLOSC_LZ4:
#undef BLOSC_LZ4HC
    case Compressor::BLOSC_LZ4HC:
#undef BLOSC_SNAPPY
    case Compressor::BLOSC_SNAPPY:
#undef BLOSC_ZLIB
    case Compressor::BLOSC_ZLIB:
#undef BLOSC_ZSTD
    case Compressor::BLOSC_ZSTD:
      st = Blosc::decompress(buffer_, tile->buffer());
      break;
    case Compressor::RLE:
      st = RLE::decompress(tile->cell_size(), buffer_, tile->buffer());
      break;
    case Compressor::BZIP2:
      st = BZip::decompress(buffer_, tile->buffer());
      break;
    case Compressor::DOUBLE_DELTA:
      st = decompress_tile_double_delta(tile);
      break;
  }

  tile->reset_offset();

  return st;
}

Status TileIO::decompress_tile_double_delta(Tile* tile) {
  auto dim_num = tile->dim_num();
  auto batch_num = (dim_num == 0) ? 1 : dim_num;

  Status st;
  auto input_buffer = new ConstBuffer(buffer_->data(), buffer_->size());
  for (unsigned int i = 0; i < batch_num; ++i) {
    switch (tile->type()) {
      case Datatype::CHAR:
        st = DoubleDelta::decompress<char>(input_buffer, tile->buffer());
        break;
      case Datatype::INT8:
        st = DoubleDelta::decompress<int8_t>(input_buffer, tile->buffer());
        break;
      case Datatype::UINT8:
        st = DoubleDelta::decompress<uint8_t>(input_buffer, tile->buffer());
        break;
      case Datatype::INT16:
        st = DoubleDelta::decompress<int16_t>(input_buffer, tile->buffer());
        break;
      case Datatype::UINT16:
        st = DoubleDelta::decompress<uint16_t>(input_buffer, tile->buffer());
        break;
      case Datatype::INT32:
        st = DoubleDelta::decompress<int>(input_buffer, tile->buffer());
        break;
      case Datatype::UINT32:
        st = DoubleDelta::decompress<uint32_t>(input_buffer, tile->buffer());
        break;
      case Datatype::INT64:
        st = DoubleDelta::decompress<int64_t>(input_buffer, tile->buffer());
        break;
      case Datatype::UINT64:
        st = DoubleDelta::decompress<uint64_t>(input_buffer, tile->buffer());
        break;
      default:
        st = LOG_STATUS(Status::TileIOError(
            "Cannot decompress tile with DoubleDelta; Not supported datatype"));
    }
    RETURN_NOT_OK_ELSE(st, delete input_buffer);
  }

  delete input_buffer;

  return st;
}

}  // namespace tiledb
