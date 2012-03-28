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

uint64_t bit_get(uint64_t * storage, int size, int offset)
{
  uint64_t r = (*storage >> offset) & ((1ull << size) - 1ull);

  DEBUG_LOG("%p: %llu = bit_get(%d, %d)\n", storage, r, size, offset);

  return r;
}

void bit_set(uint64_t * storage, int size, int offset, uint64_t set)
{
  *storage = (*storage & ~(((1ull << size) - 1ull) << offset)) | (set << offset);

  DEBUG_LOG("%p: bit_set(%d, %d, %llu)\n", storage, size, offset, set);
}

dense_db_t * dense_db_new(char * storage_path)
{
  dense_db_t * db = calloc(sizeof(*db), 1);

  db->storage_path = strdup(storage_path);

  return db;
}

static off_t get_size(int fd)
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

void dense_table_sync(dense_db_table_t * table)
{
  if (msync(table->data, table->size, MS_SYNC | MS_INVALIDATE) < 0) ERROR_AT_LINE("Error in sync");
}

dense_db_accessor_t dense_db_table_get_accessor(dense_db_table_t * table, char * field)
{
  dense_db_accessor_t acc = { 0 };

  int i;
  for (i = 0; i < table->metadata->n_fields; i++) {
    if (strcmp(table->metadata->fields[i].name, field) == 0) {
      acc.size = table->metadata->fields[i].size;
      break;
    }

    acc.offset += table->metadata->fields[i].size;
  }

  return acc;
}

void dense_db_table_get(dense_db_table_t * table, uint64_t row, dense_db_accessor_t acc, void * out)
{
  uint64_t * data = (uint64_t *)(table->data + table->metadata->header_size + (row * table->metadata->row_size / 8));

  char * ptr = out;

  int size = acc.size;
  int offset = acc.offset;

  while (1) {
    if (size < 0) break;

    if (offset < 64) {
      int inner_size = MIN(size, 64 - offset);
      int inner_size4mem = MIN(size, 64);
      inner_size4mem = round_up_to_n(inner_size4mem, 8) / 8;
      if (! inner_size4mem) inner_size4mem = 1;

      int inner_offset = MAX(offset, 0);

      uint64_t r = bit_get(data, inner_size, inner_offset);

      data++;

      if (size > 64 && offset) {
	uint64_t ir = bit_get(data, offset, 0);

	ir <<= (64 - offset);

	r |= ir;
      } else {
	offset = 0;
      }

      memcpy(ptr, &r, inner_size4mem);

      ptr += 8;

      size -= 64;
    } else {
      data++;
      offset -= 64;
    }
  }
}

uint64_t dense_db_table_get_int(dense_db_table_t * table, uint64_t row, dense_db_accessor_t acc)
{
  uint64_t num = 0;
  dense_db_table_get(table, row, acc, &num);

  return le64toh(num);
}

void dense_db_table_set(dense_db_table_t * table, uint64_t row, dense_db_accessor_t acc, void * in)
{
  uint64_t * data = (uint64_t *)(table->data + table->metadata->header_size + (row * table->metadata->row_size / 8));

  int size = acc.size;
  int offset = acc.offset;

  char * ptr = in;

  while (1) {
    if (size < 0) break;

    if (offset < 64) {
      int inner_size = MIN(size, 64 - offset);
      int inner_size4mem = MIN(size, 64);
      inner_size4mem = round_up_to_n(inner_size4mem, 8) / 8;
      if (! inner_size4mem) inner_size4mem = 1;

      int inner_offset = MAX(offset, 0);

      uint64_t val = 0;
      memcpy(&val, ptr, inner_size4mem);

      bit_set(data, inner_size, inner_offset, val);

      data++;

      if (size > 64 && offset) {
	val >>= (64 - offset);

	bit_set(data, offset, 0, val);
      } else {
	offset = 0;
      }

      ptr += 8;

      size -= 64;
    } else {
      data++;
      offset -= 64;
    }
  }
}

void dense_db_table_set_int(dense_db_table_t * table, uint64_t row, dense_db_accessor_t acc, uint64_t in)
{
  uint64_t num = htole64(in);

  dense_db_table_set(table, row, acc, &num);
}

dense_db_table_t * dense_db_create_table(dense_db_t * db, char * name, dense_db_field_t * fields, size_t n_fields, size_t rows)
{
  dense_db_table_t * table = calloc(sizeof(*table), 1);
  dense_db_table_md_t * md = calloc(sizeof(*md), 1);

  md->name = strdup(name);

  md->fields = malloc(sizeof(dense_db_field_t) * n_fields);

  md->n_fields = n_fields;
  size_t header_size = 12; // to accomodate for the leader header length, n_fields and rows

  int i;
  for (i = 0; i < n_fields; i++) {
    md->fields[i].name = strdup(fields[i].name);
    md->fields[i].size = fields[i].size;

    header_size += strlen(fields[i].name) + 1;
    header_size += 4;  // 32 bit field lengths;
    md->row_size += fields[i].size;
  }

  md->row_size = round_up_to_n(md->row_size, 8);

  md->header_size = header_size;

  table->metadata = md;

  char * fname;
  assert(asprintf(&fname, "%s/%s", db->storage_path, name) > 0);

  int fd;
  if ((fd = open(fname, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR)) < 0) ERROR_AT_LINE("Error in creat");

  free(fname);

  size_t total_size = header_size + rows * md->row_size / 8;
  total_size = round_up_to_n(total_size, 8);

  if (ftruncate(fd, total_size) < 0) ERROR_AT_LINE("Error in reserving %zd bytes for the table with fd %d", total_size, fd);

  table->size = total_size;
  table->data = mmap_table(fd, total_size);
  table->fd = fd;

  char * ptr = table->data;

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

  table->rows = rows;

  dense_table_sync(table);

  return table;
}

dense_db_table_t * dense_db_open_table(dense_db_t * db, char * name)
{
  dense_db_table_t * table = calloc(sizeof(*table), 1);
  dense_db_table_md_t * md = calloc(sizeof(*md), 1);

  table->metadata = md;
  md->name = strdup(name);

  char * path;
  assert(asprintf(&path, "%s/%s", db->storage_path, name) > 0);

  int fd;
  if ((fd = open(path, O_RDWR)) < 0) ERROR_AT_LINE("Error in open");

  table->fd = fd;

  free(path);

  table->size = get_size(fd);
  table->data = mmap_table(fd, table->size);

  char * ptr = table->data;

  uint32_t buf;
  memcpy(&buf, ptr, 4);
  ptr += 4;

  md->header_size = be32toh(buf);

  memcpy(&buf, ptr, 4);
  ptr += 4;

  md->n_fields = be32toh(buf);
  md->fields = calloc(sizeof(dense_db_field_t), md->n_fields);

  memcpy(&buf, ptr, 4);
  ptr += 4;

  table->rows = be32toh(buf);

  int i;
  for (i = 0; i < md->n_fields; i++) {
    md->fields[i].name = strdup(ptr);

    size_t len = strlen(ptr) + 1;

    ptr += len;

    memcpy(&buf, ptr, 4);

    ptr += 4;

    md->fields[i].size = be32toh(buf);
    md->row_size += md->fields[i].size;
  }
  md->row_size = round_up_to_n(md->row_size, 8);

  return table;
}

void dense_db_table_md_destroy(dense_db_table_md_t * md)
{
  int i;
  for (i = 0; i < md->n_fields; i++) {
    free(md->fields[i].name);
  }

  free(md->name);
  free(md->fields);
  free(md);
}

void dense_db_table_destroy(dense_db_table_t * table)
{
  dense_db_table_md_destroy(table->metadata);

  if (munmap(table->data, table->size) < 0) ERROR_AT_LINE("Error in munmap");

  if (close(table->fd) < 0) ERROR_AT_LINE("Error in close");

  free(table);
}

void dense_db_destroy(dense_db_t * db)
{
  free(db->storage_path);
  free(db);
}
