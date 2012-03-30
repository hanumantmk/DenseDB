/*
Copyright (c) 2012, Jason Carey  https://github.com/hanumantmk/DenseDB
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
  The names of its contributors may not be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#define _GNU_SOURCE

#include <stdlib.h>
#include <endian.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <sys/mman.h>
#include <stdint.h>
#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include <error.h>
#include <errno.h>
#include "dense_db.h"

#ifndef DEBUG
#define DEBUG 0
#endif

#define DEBUG_LOG(fmt, ...) \
do { \
  if (DEBUG) printf(fmt , ##__VA_ARGS__); \
} while(0)

#define MIN(x, y) ((x) < (y) ? (x) : (y))
#define MAX(x, y) ((x) > (y) ? (x) : (y))

#define round_up_to_n(x, n) ((((x) % (n)) == 0) ? (x) : ((x) + (n) - ((x) % (n))))

#define ERROR_AT_LINE(fmt, ...) error_at_line(1, errno, __FILE__, __LINE__, fmt , ##__VA_ARGS__)

static uint64_t bit_get(uint64_t * storage, int size, int offset)
{
  uint64_t r = (*storage >> offset) & ((1ull << size) - 1ull);

  DEBUG_LOG("%p: %llu = bit_get(%d, %d)\n", storage, r, size, offset);

  return r;
}

static void bit_set(uint64_t * storage, int size, int offset, uint64_t set)
{
  *storage = (*storage & ~(((1ull << size) - 1ull) << offset)) | (set << offset);

  DEBUG_LOG("%p: bit_set(%d, %d, %llu)\n", storage, size, offset, set);
}

static void bit_fiddle(void * buf, int size, int offset, void * param, int is_read)
{
  uint64_t * data = buf;

  uint64_t * ptr = param;

  // Move the data pointer up until we're in the right 64 bit word
  if (offset >= 64) {
    data += offset / 64;
    offset = offset % 64;
  }

  // Loop until we've read or written all we set out to
  for (; size > 0; size -= 64) {
    // only work with as many bits as fit in the word
    int inner_size = MIN(size, 64 - offset);

    // memcpy has to round up to the nearest byte
    int inner_size4mem = round_up_to_n(MIN(size, 64), 8) / 8;

    // This get's interleaved for reads and writes
    uint64_t val;

    if (is_read) {
      val = bit_get(data, inner_size, offset);
    } else {
      val = 0;
      memcpy(&val, ptr, inner_size4mem);

      bit_set(data, inner_size, offset, val);
    }

    // We've read or written all we can in one pass
    data++;

    if (size > 64 && offset) {
      // If there's something left over
      
      if (is_read) {
	// read the remaining bytes from the next memory location and merge
	// them with what you got in the last read
	uint64_t ir = bit_get(data, offset, 0);

	ir <<= (64 - offset);

	val |= ir;
      } else {
	// write the remaining bits into the next memory location
	val >>= (64 - offset);

	bit_set(data, offset, 0, val);
      }
    } else {
      // The next bit of work to do is right at the byte boundary
      offset = 0;
    }

    if (is_read) {
      // We've been accumulating in val, now write it out
      memcpy(ptr, &val, inner_size4mem);
    }

    ptr++;
  }
}

dense_db_t * dense_db_new(char * storage_path, int max_fds)
{
  dense_db_t * db = calloc(sizeof(*db), 1);

  db->storage_path = strdup(storage_path);
  db->max_fds = max_fds;

  return db;
}

static off_t get_file_size(int fd)
{
  struct stat sb;
  fstat(fd, &sb);
  return sb.st_size;
}

static void * mmap_table(int fd, size_t size)
{
  void * data = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

  if (data == MAP_FAILED) ERROR_AT_LINE("failed to mmap table");

  return data;
}

static void dense_db_table_destroy(dense_db_table_t * table)
{
  int i;
  for (i = 0; i < table->n_fields; i++) {
    free(table->fields[i].name);
  }

  free(table->name);
  free(table->fields);

  if (munmap(table->data, table->size) < 0) ERROR_AT_LINE("Error in munmap");

  if (close(table->fd) < 0) ERROR_AT_LINE("Error in close");

  free(table);
}

void dense_table_sync(dense_db_table_t * table)
{
  if (msync(table->data, table->size, MS_SYNC | MS_INVALIDATE) < 0) ERROR_AT_LINE("Error in sync");
}

dense_db_accessor_t dense_db_table_get_accessor(dense_db_table_t * table, char * field)
{
  dense_db_accessor_t acc = { 0 };

  int i;
  for (i = 0; i < table->n_fields; i++) {
    if (strcmp(table->fields[i].name, field) == 0) {
      acc.size = table->fields[i].size;
      break;
    }

    acc.offset += table->fields[i].size;
  }

  return acc;
}

void dense_db_table_get(dense_db_table_t * table, uint64_t row, dense_db_accessor_t acc, void * out)
{
  void * data = (table->data + table->header_size + (row * table->row_size / 8));

  bit_fiddle(data, acc.size, acc.offset, out, 1);
}

uint64_t dense_db_table_get_int(dense_db_table_t * table, uint64_t row, dense_db_accessor_t acc)
{
  uint64_t num = 0;
  dense_db_table_get(table, row, acc, &num);

  return le64toh(num);
}

void dense_db_table_set(dense_db_table_t * table, uint64_t row, dense_db_accessor_t acc, void * in)
{
  void * data = (table->data + table->header_size + (row * table->row_size / 8));

  bit_fiddle(data, acc.size, acc.offset, in, 0);
}

void dense_db_table_set_int(dense_db_table_t * table, uint64_t row, dense_db_accessor_t acc, uint64_t in)
{
  uint64_t num = htole64(in);

  dense_db_table_set(table, row, acc, &num);
}

dense_db_table_t * dense_db_table_open(dense_db_t * db, char * name)
{
  dense_db_table_t * table = NULL;

  HASH_FIND(hh, db->lookup, name, strlen(name), table);

  if (table) {
    HASH_DEL(db->lookup, table);
  } else {
    if (HASH_COUNT(db->lookup) >= db->max_fds) {
      dense_db_table_t * to_delete, * temp;

      HASH_ITER(hh, db->lookup, to_delete, temp) {
	if (! to_delete->refcount) {
	  HASH_DEL(db->lookup, to_delete);

	  dense_db_table_destroy(to_delete);

	  if (HASH_COUNT(db->lookup) >= db->max_fds) break;
	}
      }
    }

    table = calloc(sizeof(*table), 1);

    table->name = strdup(name);

    char * path;
    assert(asprintf(&path, "%s/%s", db->storage_path, name) > 0);

    int fd;
    if ((fd = open(path, O_RDWR)) < 0) ERROR_AT_LINE("Error in open");

    table->fd = fd;

    free(path);

    table->size = get_file_size(fd);
    table->data = mmap_table(fd, table->size);

    char * ptr = table->data;

    uint32_t buf;
    memcpy(&buf, ptr, 4);
    ptr += 4;

    table->header_size = be32toh(buf);

    memcpy(&buf, ptr, 4);
    ptr += 4;

    table->n_fields = be32toh(buf);
    table->fields = calloc(sizeof(dense_db_field_t), table->n_fields);

    memcpy(&buf, ptr, 4);
    ptr += 4;

    table->rows = be32toh(buf);

    int i;
    for (i = 0; i < table->n_fields; i++) {
      table->fields[i].name = strdup(ptr);

      size_t len = strlen(ptr) + 1;

      ptr += len;

      memcpy(&buf, ptr, 4);

      ptr += 4;

      table->fields[i].size = be32toh(buf);
      table->row_size += table->fields[i].size;
    }

    table->row_size = round_up_to_n(table->row_size, 8);
    table->db = db;
  }

  HASH_ADD_KEYPTR(hh, db->lookup, table->name, strlen(table->name), table);

  table->refcount++;

  return table;
}

dense_db_table_t * dense_db_table_create(dense_db_t * db, char * name, dense_db_field_t * fields, size_t n_fields, size_t rows)
{
  size_t header_size = 12; // to accomodate for the leader header length, n_fields and rows
  size_t row_size = 0;

  int i;
  for (i = 0; i < n_fields; i++) {
    header_size += strlen(fields[i].name) + 1;
    header_size += 4;  // 32 bit field lengths;
    row_size += fields[i].size;
  }

  row_size = round_up_to_n(row_size, 8);

  char * fname;
  assert(asprintf(&fname, "%s/%s", db->storage_path, name) > 0);

  int fd;
  if ((fd = open(fname, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR)) < 0) ERROR_AT_LINE("Error in creat");

  free(fname);

  size_t total_size = round_up_to_n(header_size + rows * row_size / 8, 8);

  if (ftruncate(fd, total_size) < 0) ERROR_AT_LINE("Error in reserving %zd bytes for the table with fd %d", total_size, fd);

  void * data = mmap_table(fd, total_size);

  uint8_t * ptr = data;

  // Write the header
  
  uint32_t buf;
  
  buf = htobe32(header_size);
  memcpy(ptr, &buf, 4);
  ptr += 4;

  buf = htobe32(n_fields);
  memcpy(ptr, &buf, 4);
  ptr += 4;

  buf = htobe32(rows);
  memcpy(ptr, &buf, 4);
  ptr += 4;

  for (i = 0; i < n_fields; i++) {
    size_t len = strlen(fields[i].name) + 1;
    memcpy(ptr, fields[i].name, len);

    ptr += len;

    buf = htobe32(fields[i].size);
    memcpy(ptr, &buf, 4);

    ptr += 4;
  }

  if (msync(data, total_size, MS_SYNC | MS_INVALIDATE) < 0) ERROR_AT_LINE("Error in sync");
  if (munmap(data, total_size) < 0) ERROR_AT_LINE("Error in munmap");
  if (close(fd) < 0) ERROR_AT_LINE("Error in close");

  return dense_db_table_open(db, name);
}

void dense_db_table_close(dense_db_table_t * table)
{
  table->refcount--;
}

void dense_db_destroy(dense_db_t * db)
{
  dense_db_table_t * ele, * temp;

  HASH_ITER(hh, db->lookup, ele, temp) {
    HASH_DEL(db->lookup, ele);

    assert(! ele->refcount);

    dense_db_table_destroy(ele);
  }

  free(db->storage_path);
  free(db);
}
